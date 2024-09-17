/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.dynamodb.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEvent;
import org.apache.flink.connector.dynamodb.source.enumerator.tracker.SplitGraphInconsistencyTracker;
import org.apache.flink.connector.dynamodb.source.enumerator.tracker.SplitTracker;
import org.apache.flink.connector.dynamodb.source.exception.DynamoDbStreamsSourceException;
import org.apache.flink.connector.dynamodb.source.proxy.StreamProxy;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.util.ListShardsResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DESCRIBE_STREAM_INCONSISTENCY_RESOLUTION_RETRY_COUNT;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.INCREMENTAL_SHARD_DISCOVERY_INTERVAL;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.SHARD_DISCOVERY_INTERVAL;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION;

/**
 * This class is used to discover and assign DynamoDb Streams splits to subtasks on the Flink
 * cluster. This runs on the JobManager.
 */
@Internal
public class DynamoDbStreamsSourceEnumerator
        implements SplitEnumerator<
                DynamoDbStreamsShardSplit, DynamoDbStreamsSourceEnumeratorState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamoDbStreamsSourceEnumerator.class);

    private final SplitEnumeratorContext<DynamoDbStreamsShardSplit> context;
    private final String streamArn;
    private final Configuration sourceConfig;
    private final StreamProxy streamProxy;
    private final DynamoDbStreamsShardAssigner shardAssigner;
    private final ShardAssignerContext shardAssignerContext;
    private final SplitTracker splitTracker;

    private final Map<Integer, Set<DynamoDbStreamsShardSplit>> splitAssignment = new HashMap<>();

    public DynamoDbStreamsSourceEnumerator(
            SplitEnumeratorContext<DynamoDbStreamsShardSplit> context,
            String streamArn,
            Configuration sourceConfig,
            StreamProxy streamProxy,
            DynamoDbStreamsShardAssigner shardAssigner,
            DynamoDbStreamsSourceEnumeratorState state) {
        this.context = context;
        this.streamArn = streamArn;
        this.sourceConfig = sourceConfig;
        this.streamProxy = streamProxy;
        this.shardAssigner = shardAssigner;
        this.shardAssignerContext = new ShardAssignerContext(splitAssignment, context);
        InitialPosition initialPosition = sourceConfig.get(STREAM_INITIAL_POSITION);
        if (state == null) {
            this.splitTracker = new SplitTracker(streamArn, initialPosition);
        } else {
            this.splitTracker =
                    new SplitTracker(state.getKnownSplits(), streamArn, initialPosition);
        }
    }

    @Override
    public void start() {
        context.callAsync(this::discoverSplits, this::processDiscoveredSplits);
        final long shardDiscoveryInterval = sourceConfig.get(SHARD_DISCOVERY_INTERVAL).toMillis();
        final long incrementalShardDiscoveryInterval =
                sourceConfig.get(INCREMENTAL_SHARD_DISCOVERY_INTERVAL).toMillis();
        context.callAsync(
                this::incrementallyDiscoverSplits,
                this::processDiscoveredSplits,
                0,
                incrementalShardDiscoveryInterval);
        context.callAsync(
                this::discoverSplits,
                this::processDiscoveredSplits,
                shardDiscoveryInterval,
                shardDiscoveryInterval);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Do nothing, since we assign splits eagerly
    }

    @Override
    public void addSplitsBack(List<DynamoDbStreamsShardSplit> list, int i) {
        throw new UnsupportedOperationException("Partial recovery is not supported");
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SplitsFinishedEvent) {
            handleFinishedSplits(subtaskId, (SplitsFinishedEvent) sourceEvent);
        }
    }

    /** When we mark a split as finished, we will only assign its child splits to the subtasks. */
    private void handleFinishedSplits(int subtaskId, SplitsFinishedEvent splitsFinishedEvent) {
        splitTracker.markAsFinished(splitsFinishedEvent.getFinishedSplitIds());
        splitAssignment
                .get(subtaskId)
                .removeIf(
                        split ->
                                splitsFinishedEvent
                                        .getFinishedSplitIds()
                                        .contains(split.splitId()));
        assignChildSplits(splitsFinishedEvent.getFinishedSplitIds());
    }

    private void processDiscoveredSplits(ListShardsResult discoveredSplits, Throwable throwable) {
        if (throwable != null) {
            throw new DynamoDbStreamsSourceException("Failed to list shards.", throwable);
        }

        if (discoveredSplits.getInconsistencyDetected()) {
            return;
        }

        splitTracker.addSplits(discoveredSplits.getShards());
        splitTracker.cleanUpOldFinishedSplits(
                discoveredSplits.getShards().stream()
                        .map(Shard::shardId)
                        .collect(Collectors.toSet()));
        if (context.registeredReaders().size() < context.currentParallelism()) {
            LOG.info(
                    "Insufficient registered readers, skipping assignment of discovered splits until all readers are registered. Required number of readers: {}, registered readers: {}",
                    context.currentParallelism(),
                    context.registeredReaders().size());
            return;
        }
        assignAllAvailableSplits();
    }

    private void assignAllAvailableSplits() {
        List<DynamoDbStreamsShardSplit> splitsAvailableForAssignment =
                splitTracker.splitsAvailableForAssignment();
        assignSplits(splitsAvailableForAssignment);
    }

    private void assignChildSplits(Set<String> finishedSplitIds) {
        List<DynamoDbStreamsShardSplit> splitsAvailableForAssignment =
                splitTracker.getUnassignedChildSplits(finishedSplitIds);
        assignSplits(splitsAvailableForAssignment);
    }

    private void assignSplits(List<DynamoDbStreamsShardSplit> splitsAvailableForAssignment) {
        Map<Integer, List<DynamoDbStreamsShardSplit>> newSplitAssignments = new HashMap<>();
        for (DynamoDbStreamsShardSplit split : splitsAvailableForAssignment) {
            assignSplitToSubtask(split, newSplitAssignments);
        }
        updateSplitAssignment(newSplitAssignments);
        context.assignSplits(new SplitsAssignment<>(newSplitAssignments));
    }

    @Override
    public void addReader(int subtaskId) {
        splitAssignment.putIfAbsent(subtaskId, new HashSet<>());
    }

    @Override
    public DynamoDbStreamsSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> splitStates =
                splitTracker.snapshotState(checkpointId);
        return new DynamoDbStreamsSourceEnumeratorState(splitStates);
    }

    @Override
    public void close() throws IOException {
        streamProxy.close();
    }

    private ListShardsResult incrementallyDiscoverSplits() {
        List<Shard> shardList = splitTracker.getKnownShards();
        SplitGraphInconsistencyTracker splitGraphInconsistencyTracker =
                new SplitGraphInconsistencyTracker();
        splitGraphInconsistencyTracker.addNodes(shardList);
        ListShardsResult listShardsResult =
                streamProxy.listShards(
                        streamArn, splitGraphInconsistencyTracker.getLatestLeafNode());
        return trackSplitsAndResolveInconsistencies(
                splitGraphInconsistencyTracker, listShardsResult);
    }

    /**
     * This method tracks the discovered splits in a graph and if the graph has inconsistencies, it
     * tries to resolve them using DescribeStream calls using the first inconsistent node found in
     * the split graph.
     *
     * @param listShardsResult shards discovered after calling DescribeStream at the start of the
     *     application or periodically via either incremental discovery or full discovery.
     */
    private ListShardsResult trackSplitsAndResolveInconsistencies(
            SplitGraphInconsistencyTracker splitGraphInconsistencyTracker,
            ListShardsResult listShardsResult) {
        splitGraphInconsistencyTracker.addNodes(listShardsResult.getShards());
        boolean streamDisabled = listShardsResult.getStreamStatus().equals(StreamStatus.DISABLED);
        int describeStreamInconsistencyResolutionCount =
                sourceConfig.get(DESCRIBE_STREAM_INCONSISTENCY_RESOLUTION_RETRY_COUNT);
        for (int i = 0;
                i < describeStreamInconsistencyResolutionCount
                        && !streamDisabled
                        && splitGraphInconsistencyTracker.inconsistencyDetected();
                i++) {
            String earliestClosedLeafNodeId =
                    splitGraphInconsistencyTracker.getEarliestClosedLeafNode();
            LOG.warn(
                    "We have detected inconsistency with DescribeStream output, resolving inconsistency with shardId: {}",
                    earliestClosedLeafNodeId);
            ListShardsResult shardsToResolveInconsistencies =
                    streamProxy.listShards(streamArn, earliestClosedLeafNodeId);
            splitGraphInconsistencyTracker.addNodes(shardsToResolveInconsistencies.getShards());
        }
        ListShardsResult discoveredSplits = new ListShardsResult();
        discoveredSplits.setStreamStatus(listShardsResult.getStreamStatus());
        discoveredSplits.setInconsistencyDetected(listShardsResult.getInconsistencyDetected());
        if (splitGraphInconsistencyTracker.inconsistencyDetected()) {
            LOG.error(
                    "There are inconsistencies in DescribeStream which we were not able to resolve. First leaf node on which inconsistency was detected:"
                            + splitGraphInconsistencyTracker.getEarliestClosedLeafNode());
            return discoveredSplits;
        }
        discoveredSplits.addShards(new ArrayList<>(splitGraphInconsistencyTracker.getNodes()));
        return discoveredSplits;
    }

    /**
     * This method is used to discover DynamoDb Streams splits the job can subscribe to. It can be
     * run in parallel, is important to not mutate any shared state.
     *
     * @return list of discovered splits
     */
    private ListShardsResult discoverSplits() {
        ListShardsResult listShardsResult = streamProxy.listShards(streamArn, null);
        SplitGraphInconsistencyTracker splitGraphInconsistencyTracker =
                new SplitGraphInconsistencyTracker();
        splitGraphInconsistencyTracker.addNodes(listShardsResult.getShards());
        return trackSplitsAndResolveInconsistencies(
                splitGraphInconsistencyTracker, listShardsResult);
    }

    private void assignSplitToSubtask(
            DynamoDbStreamsShardSplit split,
            Map<Integer, List<DynamoDbStreamsShardSplit>> newSplitAssignments) {
        if (splitTracker.isAssigned(split.splitId())) {

            LOG.warn(
                    "Skipping assignment of shard {} from stream {} because it is already assigned.",
                    split.getShardId(),
                    split.getStreamArn());
            return;
        }

        int selectedSubtask =
                shardAssigner.assign(
                        split,
                        shardAssignerContext.withPendingSplitAssignments(newSplitAssignments));
        LOG.info(
                "Assigning shard {} from stream {} to subtask {}.",
                split.getShardId(),
                split.getStreamArn(),
                selectedSubtask);

        if (newSplitAssignments.containsKey(selectedSubtask)) {
            newSplitAssignments.get(selectedSubtask).add(split);
        } else {
            List<DynamoDbStreamsShardSplit> subtaskList = new ArrayList<>();
            subtaskList.add(split);
            newSplitAssignments.put(selectedSubtask, subtaskList);
        }
        splitTracker.markAsAssigned(Collections.singletonList(split));
    }

    private void updateSplitAssignment(
            Map<Integer, List<DynamoDbStreamsShardSplit>> newSplitsAssignment) {
        newSplitsAssignment.forEach(
                (subtaskId, newSplits) -> {
                    if (splitAssignment.containsKey(subtaskId)) {
                        splitAssignment.get(subtaskId).addAll(newSplits);
                    } else {
                        splitAssignment.put(subtaskId, new HashSet<>(newSplits));
                    }
                });
    }

    @Internal
    private static class ShardAssignerContext implements DynamoDbStreamsShardAssigner.Context {

        private final Map<Integer, Set<DynamoDbStreamsShardSplit>> splitAssignment;
        private final SplitEnumeratorContext<DynamoDbStreamsShardSplit> splitEnumeratorContext;
        private Map<Integer, List<DynamoDbStreamsShardSplit>> pendingSplitAssignments =
                Collections.emptyMap();

        private ShardAssignerContext(
                Map<Integer, Set<DynamoDbStreamsShardSplit>> splitAssignment,
                SplitEnumeratorContext<DynamoDbStreamsShardSplit> splitEnumeratorContext) {
            this.splitAssignment = splitAssignment;
            this.splitEnumeratorContext = splitEnumeratorContext;
        }

        private ShardAssignerContext withPendingSplitAssignments(
                Map<Integer, List<DynamoDbStreamsShardSplit>> pendingSplitAssignments) {
            Map<Integer, List<DynamoDbStreamsShardSplit>> copyPendingSplitAssignments =
                    new HashMap<>();
            for (Entry<Integer, List<DynamoDbStreamsShardSplit>> entry :
                    pendingSplitAssignments.entrySet()) {
                copyPendingSplitAssignments.put(
                        entry.getKey(),
                        Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
            }
            this.pendingSplitAssignments = Collections.unmodifiableMap(copyPendingSplitAssignments);
            return this;
        }

        @Override
        public Map<Integer, Set<DynamoDbStreamsShardSplit>> getCurrentSplitAssignment() {
            Map<Integer, Set<DynamoDbStreamsShardSplit>> copyCurrentSplitAssignment =
                    new HashMap<>();
            for (Entry<Integer, Set<DynamoDbStreamsShardSplit>> entry :
                    splitAssignment.entrySet()) {
                copyCurrentSplitAssignment.put(
                        entry.getKey(),
                        Collections.unmodifiableSet(new HashSet<>(entry.getValue())));
            }
            return Collections.unmodifiableMap(copyCurrentSplitAssignment);
        }

        @Override
        public Map<Integer, List<DynamoDbStreamsShardSplit>> getPendingSplitAssignments() {
            return pendingSplitAssignments;
        }

        @Override
        public Map<Integer, ReaderInfo> getRegisteredReaders() {
            // the split enumerator context already returns an unmodifiable map.
            return splitEnumeratorContext.registeredReaders();
        }
    }
}
