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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEvent;
import org.apache.flink.connector.dynamodb.source.metrics.DynamoDbStreamsShardMetrics;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

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

    public DynamoDbStreamsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Record>> elementsQueue,
            SingleThreadFetcherManager<Record, DynamoDbStreamsShardSplit> splitFetcherManager,
            RecordEmitter<Record, T, DynamoDbStreamsShardSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.shardMetricGroupMap = shardMetricGroupMap;
    }

    @Override
    protected void onSplitFinished(Map<String, DynamoDbStreamsShardSplitState> finishedSplitIds) {
        context.sendSourceEventToCoordinator(
                new SplitsFinishedEvent(new HashSet<>(finishedSplitIds.keySet())));
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
        splits.forEach(this::registerShardMetricGroup);
        super.addSplits(splits);
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
