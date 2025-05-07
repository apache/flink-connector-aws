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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.InitialPosition;
import org.apache.flink.connector.kinesis.source.enumerator.tracker.SplitTracker;
import org.apache.flink.connector.kinesis.source.event.SplitsFinishedEvent;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.ListShardsStartingPosition;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.reader.fanout.StreamConsumerRegistrar;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_DISCOVERY_INTERVAL;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.STREAM_INITIAL_POSITION;
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
    private final SplitTracker splitTracker;

    private final Map<Integer, Set<KinesisShardSplit>> splitAssignment = new HashMap<>();

    private String lastSeenShardId;
    private final StreamConsumerRegistrar streamConsumerRegistrar;

    public KinesisStreamsSourceEnumerator(
            SplitEnumeratorContext<KinesisShardSplit> context,
            String streamArn,
            Configuration sourceConfig,
            StreamProxy streamProxy,
            KinesisShardAssigner shardAssigner,
            KinesisStreamsSourceEnumeratorState state,
            boolean preserveShardOrder,
            StreamConsumerRegistrar streamConsumerRegistrar) {
        this.context = context;
        this.streamArn = streamArn;
        this.sourceConfig = sourceConfig;
        this.streamProxy = streamProxy;
        this.shardAssigner = shardAssigner;
        this.shardAssignerContext = new ShardAssignerContext(splitAssignment, context);
        this.streamConsumerRegistrar = streamConsumerRegistrar;
        if (state == null) {
            this.lastSeenShardId = null;
            this.splitTracker = new SplitTracker(preserveShardOrder);
        } else {
            this.lastSeenShardId = state.getLastSeenShardId();
            this.splitTracker = new SplitTracker(preserveShardOrder, state.getKnownSplits());
        }
    }

    @Override
    public void start() {
        streamConsumerRegistrar.registerStreamConsumer();

        if (lastSeenShardId == null) {
            context.callAsync(this::initialDiscoverSplits, this::processDiscoveredSplits);
        }

        final long shardDiscoveryInterval = sourceConfig.get(SHARD_DISCOVERY_INTERVAL).toMillis();
        context.callAsync(
                this::periodicallyDiscoverSplits,
                this::processDiscoveredSplits,
                shardDiscoveryInterval,
                shardDiscoveryInterval);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Do nothing, since we assign splits eagerly
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SplitsFinishedEvent) {
            handleFinishedSplits(subtaskId, (SplitsFinishedEvent) sourceEvent);
        }
    }

    private void handleFinishedSplits(int subtask, SplitsFinishedEvent splitsFinishedEvent) {
        splitTracker.markAsFinished(splitsFinishedEvent.getFinishedSplitIds());
        splitAssignment
                .get(subtask)
                .removeIf(
                        split ->
                                splitsFinishedEvent
                                        .getFinishedSplitIds()
                                        .contains(split.splitId()));

        assignSplits();
    }

    @Override
    public void addSplitsBack(List<KinesisShardSplit> splits, int subtaskId) {
        throw new UnsupportedOperationException("Partial recovery is not supported");
    }

    @Override
    public void addReader(int subtaskId) {
        splitAssignment.putIfAbsent(subtaskId, new HashSet<>());
    }

    @Override
    public KinesisStreamsSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        List<KinesisShardSplitWithAssignmentStatus> splitStates =
                splitTracker.snapshotState(checkpointId);
        return new KinesisStreamsSourceEnumeratorState(splitStates, lastSeenShardId);
    }

    @Override
    public void close() throws IOException {
        streamConsumerRegistrar.deregisterStreamConsumer();
        streamProxy.close();
    }

    @VisibleForTesting
    List<KinesisShardSplit> initialDiscoverSplits() {
        StreamDescriptionSummary streamDescriptionSummary =
                streamProxy.getStreamDescriptionSummary(streamArn);
        InitialPosition initialPosition = sourceConfig.get(STREAM_INITIAL_POSITION);
        Instant currentTimestamp = Instant.now();
        Optional<Instant> initialTimestamp = parseStreamTimestampStartingPosition(sourceConfig);

        ListShardsStartingPosition shardDiscoveryStartingPosition =
                getInitialPositionForShardDiscovery(initialPosition, currentTimestamp);
        StartingPosition initialStartingPosition =
                getInitialStartingPosition(initialPosition, currentTimestamp);

        List<Shard> shards;
        try {
            shards = streamProxy.listShards(streamArn, shardDiscoveryStartingPosition);
        } catch (InvalidArgumentException e) {
            // If initial timestamp is older than retention period of the stream,
            // fall back to TRIM_HORIZON for shard discovery
            Instant trimHorizonTimestamp =
                    Instant.now()
                            .minus(
                                    streamDescriptionSummary.retentionPeriodHours(),
                                    ChronoUnit.HOURS);
            boolean isInitialTimestampBeforeTrimHorizon =
                    initialTimestamp.map(trimHorizonTimestamp::isAfter).orElse(false);
            if (initialPosition.equals(InitialPosition.AT_TIMESTAMP)
                    && isInitialTimestampBeforeTrimHorizon) {
                LOG.warn(
                        "Configured initial position timestamp is before stream TRIM_HORIZON. Falling back to TRIM_HORIZON");
                shards = streamProxy.listShards(streamArn, ListShardsStartingPosition.fromStart());
            } else {
                throw new KinesisStreamsSourceException("Unable to list shards", e);
            }
        }

        return mapToSplits(shards, initialStartingPosition);
    }

    @VisibleForTesting
    ListShardsStartingPosition getInitialPositionForShardDiscovery(
            InitialPosition initialPosition, Instant currentTime) {
        switch (initialPosition) {
            case LATEST:
                LOG.info(
                        "Starting consumption from stream {} from LATEST. This translates into AT_TIMESTAMP from {}",
                        streamArn,
                        currentTime.toString());
                return ListShardsStartingPosition.fromTimestamp(currentTime);
            case AT_TIMESTAMP:
                Instant timestamp =
                        parseStreamTimestampStartingPosition(sourceConfig)
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Stream initial timestamp must be specified when initial position set to AT_TIMESTAMP"));
                LOG.info(
                        "Starting consumption from stream {} from AT_TIMESTAMP, starting from {}",
                        streamArn,
                        timestamp.toString());
                return ListShardsStartingPosition.fromTimestamp(timestamp);
            case TRIM_HORIZON:
                LOG.info("Starting consumption from stream {} from TRIM_HORIZON.", streamArn);
                return ListShardsStartingPosition.fromStart();
        }

        throw new IllegalArgumentException(
                "Unsupported initial position configuration " + initialPosition);
    }

    @VisibleForTesting
    StartingPosition getInitialStartingPosition(
            InitialPosition initialPosition, Instant currentTime) {
        switch (initialPosition) {
            case LATEST:
                // If LATEST is requested, we still set the starting position to the time of
                // startup. This way, the job starts reading from a deterministic timestamp
                // (i.e. time of job submission), even if it enters a restart loop immediately
                // after submission.
                return StartingPosition.fromTimestamp(currentTime);
            case AT_TIMESTAMP:
                Instant timestamp =
                        parseStreamTimestampStartingPosition(sourceConfig)
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Stream initial timestamp must be specified when initial position set to AT_TIMESTAMP"));
                return StartingPosition.fromTimestamp(timestamp);
            case TRIM_HORIZON:
                return StartingPosition.fromStart();
        }

        throw new IllegalArgumentException(
                "Unsupported initial position configuration " + initialPosition);
    }

    /**
     * This method is used to discover Kinesis splits the job can subscribe to. It can be run in
     * parallel, is important to not mutate any shared state.
     *
     * @return list of discovered splits
     */
    private List<KinesisShardSplit> periodicallyDiscoverSplits() {
        List<Shard> shards =
                streamProxy.listShards(
                        streamArn, ListShardsStartingPosition.fromShardId(lastSeenShardId));
        // Any shard discovered after the initial startup should be read from the start, since they
        // come from resharding
        return mapToSplits(shards, StartingPosition.fromStart());
    }

    private List<KinesisShardSplit> mapToSplits(
            List<Shard> shards, StartingPosition startingPosition) {

        List<KinesisShardSplit> splits = new ArrayList<>();
        for (Shard shard : shards) {
            Set<String> parentShardIds = new HashSet<>();
            if (shard.parentShardId() != null) {
                parentShardIds.add(shard.parentShardId());
            }
            if (shard.adjacentParentShardId() != null) {
                parentShardIds.add(shard.adjacentParentShardId());
            }
            splits.add(
                    new KinesisShardSplit(
                            streamArn,
                            shard.shardId(),
                            startingPosition,
                            parentShardIds,
                            shard.hashKeyRange().startingHashKey(),
                            shard.hashKeyRange().endingHashKey()));
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
    private void processDiscoveredSplits(
            List<KinesisShardSplit> discoveredSplits, Throwable throwable) {
        if (throwable != null) {
            throw new KinesisStreamsSourceException("Failed to list shards.", throwable);
        }

        splitTracker.addSplits(discoveredSplits);
        updateLastSeenShardId(discoveredSplits);

        if (context.registeredReaders().size() < context.currentParallelism()) {
            LOG.info(
                    "Insufficient registered readers, skipping assignment of discovered splits until all readers are registered. Required number of readers: {}, Registered readers: {}",
                    context.currentParallelism(),
                    context.registeredReaders().size());
            return;
        }

        assignSplits();
    }

    private void assignSplits() {
        Map<Integer, List<KinesisShardSplit>> newSplitAssignments = new HashMap<>();
        for (KinesisShardSplit split : splitTracker.splitsAvailableForAssignment()) {
            assignSplitToSubtask(split, newSplitAssignments);
        }

        updateSplitAssignment(newSplitAssignments);
        context.assignSplits(new SplitsAssignment<>(newSplitAssignments));
    }

    private void assignSplitToSubtask(
            KinesisShardSplit split, Map<Integer, List<KinesisShardSplit>> newSplitAssignments) {
        if (splitTracker.isAssigned(split.splitId())) {
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
        splitTracker.markAsAssigned(Collections.singletonList(split));
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
