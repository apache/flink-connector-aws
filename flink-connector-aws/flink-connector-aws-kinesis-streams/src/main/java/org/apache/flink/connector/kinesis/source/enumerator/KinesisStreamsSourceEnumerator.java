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

package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.InitialPosition;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Shard;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_INITIAL_POSITION;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition;

/**
 * This class is used to discover and assign Kinesis splits to subtasks on the Flink cluster. This
 * runs on the JobManager.
 */
@Internal
public class KinesisStreamsSourceEnumerator
        implements SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamsSourceEnumerator.class);

    private final SplitEnumeratorContext<KinesisShardSplit> context;
    private final String streamArn;
    private final Configuration sourceConfig;
    private final StreamProxy streamProxy;
    private final KinesisShardAssigner shardAssigner;
    private final ShardAssignerContext shardAssignerContext;

    private final Map<Integer, Set<KinesisShardSplit>> splitAssignment = new HashMap<>();
    private final Set<String> assignedSplitIds = new HashSet<>();
    private final Set<KinesisShardSplit> unassignedSplits;

    private String lastSeenShardId;

    public KinesisStreamsSourceEnumerator(
            SplitEnumeratorContext<KinesisShardSplit> context,
            String streamArn,
            Configuration sourceConfig,
            StreamProxy streamProxy,
            KinesisShardAssigner shardAssigner,
            KinesisStreamsSourceEnumeratorState state) {
        this.context = context;
        this.streamArn = streamArn;
        this.sourceConfig = sourceConfig;
        this.streamProxy = streamProxy;
        this.shardAssigner = shardAssigner;
        this.shardAssignerContext = new ShardAssignerContext(splitAssignment, context);
        if (state == null) {
            this.lastSeenShardId = null;
            this.unassignedSplits = new HashSet<>();
        } else {
            this.lastSeenShardId = state.getLastSeenShardId();
            this.unassignedSplits = state.getUnassignedSplits();
        }
    }

    @Override
    public void start() {
        if (lastSeenShardId == null) {
            context.callAsync(this::initialDiscoverSplits, this::assignSplits);
        }

        final long shardDiscoveryInterval = sourceConfig.get(SHARD_DISCOVERY_INTERVAL_MILLIS);
        context.callAsync(
                this::periodicallyDiscoverSplits,
                this::assignSplits,
                shardDiscoveryInterval,
                shardDiscoveryInterval);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Do nothing, since we assign splits eagerly
    }

    @Override
    public void addSplitsBack(List<KinesisShardSplit> splits, int subtaskId) {
        if (!splitAssignment.containsKey(subtaskId)) {
            LOG.warn(
                    "Unable to add splits back for subtask {} since it is not assigned any splits. Splits: {}",
                    subtaskId,
                    splits);
            return;
        }

        for (KinesisShardSplit split : splits) {
            splitAssignment.get(subtaskId).remove(split);
            assignedSplitIds.remove(split.splitId());
            unassignedSplits.add(split);
        }

        // Assign the unassignedSplits
        // We did not discover any new splits, so we put in an empty list
        assignSplits(Collections.emptyList(), null);
    }

    @Override
    public void addReader(int subtaskId) {
        splitAssignment.putIfAbsent(subtaskId, new HashSet<>());
    }

    @Override
    public KinesisStreamsSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new KinesisStreamsSourceEnumeratorState(unassignedSplits, lastSeenShardId);
    }

    @Override
    public void close() throws IOException {
        streamProxy.close();
    }

    private List<KinesisShardSplit> initialDiscoverSplits() {
        List<Shard> shards = streamProxy.listShards(streamArn, lastSeenShardId);
        return mapToSplits(shards, sourceConfig.get(STREAM_INITIAL_POSITION));
    }

    /**
     * This method is used to discover Kinesis splits the job can subscribe to. It can be run in
     * parallel, is important to not mutate any shared state.
     *
     * @return list of discovered splits
     */
    private List<KinesisShardSplit> periodicallyDiscoverSplits() {
        List<Shard> shards = streamProxy.listShards(streamArn, lastSeenShardId);
        // Any shard discovered after the initial startup should be read from the start, since they
        // come from resharding
        return mapToSplits(shards, InitialPosition.TRIM_HORIZON);
    }

    private List<KinesisShardSplit> mapToSplits(
            List<Shard> shards, InitialPosition initialPosition) {
        StartingPosition startingPosition;
        switch (initialPosition) {
            case LATEST:
                // If LATEST is requested, we still set the starting position to the time of
                // startup. This way, the job starts reading from a deterministic timestamp
                // (i.e. time of job submission), even if it enters a restart loop immediately
                // after submission.
                startingPosition = StartingPosition.fromTimestamp(Instant.now());
                break;
            case AT_TIMESTAMP:
                startingPosition =
                        StartingPosition.fromTimestamp(
                                parseStreamTimestampStartingPosition(sourceConfig).toInstant());
                break;
            case TRIM_HORIZON:
            default:
                startingPosition = StartingPosition.fromStart();
        }

        List<KinesisShardSplit> splits = new ArrayList<>();
        for (Shard shard : shards) {
            splits.add(new KinesisShardSplit(streamArn, shard.shardId(), startingPosition));
        }

        return splits;
    }

    /**
     * This method assigns a given set of Kinesis splits to the readers currently registered on the
     * cluster. This assignment is done via a side-effect on the {@link SplitEnumeratorContext}
     * object.
     *
     * @param discoveredSplits list of discovered splits
     * @param throwable thrown when discovering splits. Will be null if no throwable thrown.
     */
    private void assignSplits(List<KinesisShardSplit> discoveredSplits, Throwable throwable) {
        if (throwable != null) {
            throw new KinesisStreamsSourceException("Failed to list shards.", throwable);
        }

        if (context.registeredReaders().size() < context.currentParallelism()) {
            LOG.info(
                    "Insufficient registered readers, skipping assignment of discovered splits until all readers are registered. Required number of readers: {}, Registered readers: {}",
                    context.currentParallelism(),
                    context.registeredReaders().size());
            unassignedSplits.addAll(discoveredSplits);
            return;
        }

        Map<Integer, List<KinesisShardSplit>> newSplitAssignments = new HashMap<>();
        for (KinesisShardSplit split : unassignedSplits) {
            assignSplitToSubtask(split, newSplitAssignments);
        }
        unassignedSplits.clear();
        for (KinesisShardSplit split : discoveredSplits) {
            assignSplitToSubtask(split, newSplitAssignments);
        }

        updateLastSeenShardId(discoveredSplits);
        updateSplitAssignment(newSplitAssignments);
        context.assignSplits(new SplitsAssignment<>(newSplitAssignments));
    }

    private void assignSplitToSubtask(
            KinesisShardSplit split, Map<Integer, List<KinesisShardSplit>> newSplitAssignments) {
        if (assignedSplitIds.contains(split.splitId())) {
            LOG.info(
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
            List<KinesisShardSplit> subtaskList = new ArrayList<>();
            subtaskList.add(split);
            newSplitAssignments.put(selectedSubtask, subtaskList);
        }
        assignedSplitIds.add(split.splitId());
    }

    private void updateLastSeenShardId(List<KinesisShardSplit> discoveredSplits) {
        if (!discoveredSplits.isEmpty()) {
            KinesisShardSplit lastSplit = discoveredSplits.get(discoveredSplits.size() - 1);
            lastSeenShardId = lastSplit.getShardId();
        }
    }

    private void updateSplitAssignment(Map<Integer, List<KinesisShardSplit>> newSplitsAssignment) {
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
    private static class ShardAssignerContext implements KinesisShardAssigner.Context {

        private final Map<Integer, Set<KinesisShardSplit>> splitAssignment;
        private final SplitEnumeratorContext<KinesisShardSplit> splitEnumeratorContext;
        private Map<Integer, List<KinesisShardSplit>> pendingSplitAssignments =
                Collections.emptyMap();

        private ShardAssignerContext(
                Map<Integer, Set<KinesisShardSplit>> splitAssignment,
                SplitEnumeratorContext<KinesisShardSplit> splitEnumeratorContext) {
            this.splitAssignment = splitAssignment;
            this.splitEnumeratorContext = splitEnumeratorContext;
        }

        private ShardAssignerContext withPendingSplitAssignments(
                Map<Integer, List<KinesisShardSplit>> pendingSplitAssignments) {
            Map<Integer, List<KinesisShardSplit>> copyPendingSplitAssignments = new HashMap<>();
            for (Entry<Integer, List<KinesisShardSplit>> entry :
                    pendingSplitAssignments.entrySet()) {
                copyPendingSplitAssignments.put(
                        entry.getKey(),
                        Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
            }
            this.pendingSplitAssignments = Collections.unmodifiableMap(copyPendingSplitAssignments);
            return this;
        }

        @Override
        public Map<Integer, Set<KinesisShardSplit>> getCurrentSplitAssignment() {
            Map<Integer, Set<KinesisShardSplit>> copyCurrentSplitAssignment = new HashMap<>();
            for (Entry<Integer, Set<KinesisShardSplit>> entry : splitAssignment.entrySet()) {
                copyCurrentSplitAssignment.put(
                        entry.getKey(),
                        Collections.unmodifiableSet(new HashSet<>(entry.getValue())));
            }
            return Collections.unmodifiableMap(copyCurrentSplitAssignment);
        }

        @Override
        public Map<Integer, List<KinesisShardSplit>> getPendingSplitAssignments() {
            return pendingSplitAssignments;
        }

        @Override
        public Map<Integer, ReaderInfo> getRegisteredReaders() {
            // the split enumerator context already returns an unmodifiable map.
            return splitEnumeratorContext.registeredReaders();
        }
    }
}
