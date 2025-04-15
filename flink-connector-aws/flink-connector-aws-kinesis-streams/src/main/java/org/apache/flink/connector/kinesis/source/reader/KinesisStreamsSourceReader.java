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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.kinesis.source.event.SplitsFinishedEvent;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Coordinates the reading from assigned splits. Runs on the TaskManager.
 *
 * @param <T> the data type emitted by the Kinesis stream source
 */
@Internal
public class KinesisStreamsSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Record, T, KinesisShardSplit, KinesisShardSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamsSourceReader.class);
    private final Map<String, KinesisShardMetrics> shardMetricGroupMap;
    private final NavigableMap<Long, Set<KinesisShardSplit>> splitFinishedEvents;
    private long currentCheckpointId;

    public KinesisStreamsSourceReader(
            SingleThreadFetcherManager<Record, KinesisShardSplit> splitFetcherManager,
            RecordEmitter<Record, T, KinesisShardSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            Map<String, KinesisShardMetrics> shardMetricGroupMap) {
        super(splitFetcherManager, recordEmitter, config, context);
        this.shardMetricGroupMap = shardMetricGroupMap;
        this.splitFinishedEvents = new TreeMap<>();
        this.currentCheckpointId = Long.MIN_VALUE;
    }

    @Override
    protected void onSplitFinished(Map<String, KinesisShardSplitState> finishedSplitIds) {
        if (finishedSplitIds.isEmpty()) {
            return;
        }
        splitFinishedEvents.computeIfAbsent(currentCheckpointId, k -> new HashSet<>());
        finishedSplitIds.values().stream()
                .map(
                        finishedSplit ->
                                new KinesisShardSplit(
                                        finishedSplit.getStreamArn(),
                                        finishedSplit.getShardId(),
                                        finishedSplit.getNextStartingPosition(),
                                        finishedSplit.getKinesisShardSplit().getParentShardIds(),
                                        finishedSplit.getKinesisShardSplit().getStartingHashKey(),
                                        finishedSplit.getKinesisShardSplit().getEndingHashKey(),
                                        true))
                .forEach(split -> splitFinishedEvents.get(currentCheckpointId).add(split));

        context.sendSourceEventToCoordinator(
                new SplitsFinishedEvent(new HashSet<>(finishedSplitIds.keySet())));
        finishedSplitIds.keySet().forEach(this::unregisterShardMetricGroup);
    }

    /**
     * At snapshot, we also store the pending finished split ids in the current checkpoint so that
     * in case we have to restore the reader from state, we also send the finished split ids
     * otherwise we run a risk of data loss during restarts of the source because of the
     * SplitsFinishedEvent going missing.
     *
     * @param checkpointId the checkpoint id
     * @return a list of finished splits
     */
    @Override
    public List<KinesisShardSplit> snapshotState(long checkpointId) {
        this.currentCheckpointId = checkpointId;
        List<KinesisShardSplit> splits = new ArrayList<>(super.snapshotState(checkpointId));

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
     * @param checkpointId the checkpoint id
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitFinishedEvents.headMap(checkpointId, true).clear();
    }

    @Override
    protected KinesisShardSplitState initializedState(KinesisShardSplit split) {
        return new KinesisShardSplitState(split);
    }

    @Override
    protected KinesisShardSplit toSplitType(String splitId, KinesisShardSplitState splitState) {
        return splitState.getKinesisShardSplit();
    }

    @Override
    public void addSplits(List<KinesisShardSplit> splits) {
        List<KinesisShardSplit> kinesisShardSplits = new ArrayList<>();
        for (KinesisShardSplit split : splits) {
            if (split.isFinished()) {
                context.sendSourceEventToCoordinator(
                        new SplitsFinishedEvent(Collections.singleton(split.splitId())));
            } else {
                kinesisShardSplits.add(split);
            }
        }
        kinesisShardSplits.forEach(this::registerShardMetricGroup);
        super.addSplits(kinesisShardSplits);
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.shardMetricGroupMap.keySet().forEach(this::unregisterShardMetricGroup);
    }

    private void registerShardMetricGroup(KinesisShardSplit split) {
        if (!this.shardMetricGroupMap.containsKey(split.getShardId())) {
            this.shardMetricGroupMap.put(
                    split.getShardId(), new KinesisShardMetrics(split, context.metricGroup()));
        } else {
            LOG.warn(
                    "Metric group for shard with id {} has already been registered.",
                    split.getShardId());
        }
    }

    private void unregisterShardMetricGroup(String shardId) {
        KinesisShardMetrics removed = this.shardMetricGroupMap.remove(shardId);
        if (removed != null) {
            removed.unregister();
        } else {
            LOG.warn(
                    "Shard metric group unregister failed. Metric group for {} does not exist.",
                    shardId);
        }
    }
}
