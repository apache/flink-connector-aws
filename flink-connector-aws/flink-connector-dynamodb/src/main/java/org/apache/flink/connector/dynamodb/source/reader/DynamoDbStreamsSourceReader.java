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

package org.apache.flink.connector.dynamodb.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEvent;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEventContext;
import org.apache.flink.connector.dynamodb.source.metrics.DynamoDbStreamsShardMetrics;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Coordinates the reading from assigned splits. Runs on the TaskManager.
 *
 * @param <T> the data type emitted by the DynamoDb stream source
 */
@Internal
public class DynamoDbStreamsSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Record, T, DynamoDbStreamsShardSplit, DynamoDbStreamsShardSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamsSourceReader.class);
    private final Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap;
    private final NavigableMap<Long, Set<DynamoDbStreamsShardSplit>> splitFinishedEvents;
    private final Map<String, List<Shard>> childShardIdMap;
    private long currentCheckpointId;

    public DynamoDbStreamsSourceReader(
            SingleThreadFetcherManager<Record, DynamoDbStreamsShardSplit> splitFetcherManager,
            RecordEmitter<Record, T, DynamoDbStreamsShardSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            Map<String, List<Shard>> childShardIdMap,
            Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap) {
        super(splitFetcherManager, recordEmitter, config, context);
        this.shardMetricGroupMap = shardMetricGroupMap;
        this.splitFinishedEvents = new TreeMap<>();
        this.childShardIdMap = childShardIdMap;
        this.currentCheckpointId = Long.MIN_VALUE;
    }

    /**
     * We store the finished splits in a map keyed by the checkpoint id.
     *
     * @param finishedSplitIds
     */
    @Override
    protected void onSplitFinished(Map<String, DynamoDbStreamsShardSplitState> finishedSplitIds) {
        if (finishedSplitIds.isEmpty()) {
            return;
        }

        splitFinishedEvents.computeIfAbsent(currentCheckpointId, k -> new HashSet<>());
        finishedSplitIds.values().stream()
                .map(
                        finishedSplit -> {
                            List<Shard> childSplits = new ArrayList<>();
                            String finishedSplitId = finishedSplit.getSplitId();
                            if (childShardIdMap.containsKey(finishedSplitId)) {
                                List<Shard> childSplitIdsOfFinishedSplit =
                                        childShardIdMap.get(finishedSplitId);
                                childSplits.addAll(childSplitIdsOfFinishedSplit);
                            }
                            return new DynamoDbStreamsShardSplit(
                                    finishedSplit.getStreamArn(),
                                    finishedSplit.getShardId(),
                                    finishedSplit.getNextStartingPosition(),
                                    finishedSplit.getDynamoDbStreamsShardSplit().getParentShardId(),
                                    childSplits,
                                    true);
                        })
                .forEach(split -> splitFinishedEvents.get(currentCheckpointId).add(split));

        Set<SplitsFinishedEventContext> splitsFinishedEventContextMap =
                finishedSplitIds.values().stream()
                        .map(DynamoDbStreamsShardSplitState::getSplitId)
                        .map(
                                finishedSplitId ->
                                        new SplitsFinishedEventContext(
                                                finishedSplitId,
                                                childShardIdMap.getOrDefault(
                                                        finishedSplitId, Collections.emptyList())))
                        .peek(context -> childShardIdMap.remove(context.getSplitId()))
                        .collect(Collectors.toSet());

        LOG.info("Sending splits finished event to coordinator: {}", splitsFinishedEventContextMap);
        context.sendSourceEventToCoordinator(
                new SplitsFinishedEvent(splitsFinishedEventContextMap));
        finishedSplitIds.keySet().forEach(this::unregisterShardMetricGroup);
    }

    @Override
    protected DynamoDbStreamsShardSplitState initializedState(DynamoDbStreamsShardSplit split) {
        return new DynamoDbStreamsShardSplitState(split);
    }

    @Override
    protected DynamoDbStreamsShardSplit toSplitType(
            String splitId, DynamoDbStreamsShardSplitState splitState) {
        return splitState.getDynamoDbStreamsShardSplit();
    }

    @Override
    public void addSplits(List<DynamoDbStreamsShardSplit> splits) {
        List<DynamoDbStreamsShardSplit> dynamoDbStreamsShardSplits = new ArrayList<>();
        for (DynamoDbStreamsShardSplit split : splits) {
            if (split.isFinished()) {
                // Replay the finished split event.
                // We don't need to reload the split finished events in buffer back
                // since if the next checkpoint completes, these would just be removed from the
                // buffer. If the next checkpoint doesn't complete,
                // we would go back to the previous checkpointed
                // state which will again replay these split finished events.
                SplitsFinishedEventContext splitsFinishedEventContext =
                        new SplitsFinishedEventContext(split.splitId(), split.getChildSplits());
                context.sendSourceEventToCoordinator(
                        new SplitsFinishedEvent(Collections.singleton(splitsFinishedEventContext)));
            } else {
                dynamoDbStreamsShardSplits.add(split);
            }
        }
        dynamoDbStreamsShardSplits.forEach(this::registerShardMetricGroup);
        super.addSplits(dynamoDbStreamsShardSplits);
    }

    /**
     * At snapshot, we also store the pending finished split ids in the current checkpoint so that
     * in case we have to restore the reader from state, we also send the finished split ids
     * otherwise we run a risk of data loss during restarts of the source because of the
     * SplitsFinishedEvent going missing.
     *
     * @param checkpointId
     * @return
     */
    @Override
    public List<DynamoDbStreamsShardSplit> snapshotState(long checkpointId) {
        this.currentCheckpointId = checkpointId;
        List<DynamoDbStreamsShardSplit> splits = new ArrayList<>(super.snapshotState(checkpointId));

        if (!splitFinishedEvents.isEmpty()) {
            // Add all finished splits to the snapshot
            splitFinishedEvents.values().forEach(splits::addAll);
        }
        return splits;
    }

    /**
     * During notifyCheckpointComplete, we should clean up the state of finished splits that are
     * less than or equal to the checkpoint id.
     *
     * @param checkpointId
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitFinishedEvents.headMap(checkpointId, true).clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.shardMetricGroupMap.keySet().forEach(this::unregisterShardMetricGroup);
    }

    private void registerShardMetricGroup(DynamoDbStreamsShardSplit split) {
        if (!this.shardMetricGroupMap.containsKey(split.splitId())) {
            this.shardMetricGroupMap.put(
                    split.splitId(), new DynamoDbStreamsShardMetrics(split, context.metricGroup()));
        } else {
            LOG.warn(
                    "Metric group for shard with id {} has already been registered.",
                    split.splitId());
        }
    }

    private void unregisterShardMetricGroup(String shardId) {
        DynamoDbStreamsShardMetrics removed = this.shardMetricGroupMap.remove(shardId);
        if (removed != null) {
            removed.unregister();
        } else {
            LOG.warn(
                    "Shard metric group unregister failed. Metric group for {} does not exist.",
                    shardId);
        }
    }
}
