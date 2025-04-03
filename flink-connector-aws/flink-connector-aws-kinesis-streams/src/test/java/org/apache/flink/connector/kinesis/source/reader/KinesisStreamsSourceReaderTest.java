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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.kinesis.source.event.SplitsFinishedEvent;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.model.TestData;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.reader.polling.PollingKinesisShardSplitReader;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.util.KinesisContextProvider;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_GET_RECORDS_IDLE_SOURCE_INTERVAL;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_GET_RECORDS_INTERVAL;
import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsSourceReaderTest {
    private TestingReaderContext testingReaderContext;
    private KinesisStreamsSourceReader<TestData> sourceReader;
    private MetricListener metricListener;
    private Map<String, KinesisShardMetrics> shardMetricGroupMap;
    private Configuration sourceConfig;

    @BeforeEach
    public void init() {
        metricListener = new MetricListener();
        shardMetricGroupMap = new ConcurrentHashMap<>();
        sourceConfig = new Configuration();
        sourceConfig.set(SHARD_GET_RECORDS_INTERVAL, Duration.ofMillis(0));
        sourceConfig.set(SHARD_GET_RECORDS_IDLE_SOURCE_INTERVAL, Duration.ofMillis(250));
        StreamProxy testStreamProxy = getTestStreamProxy();
        Supplier<PollingKinesisShardSplitReader> splitReaderSupplier =
                () ->
                        new PollingKinesisShardSplitReader(
                                testStreamProxy, shardMetricGroupMap, sourceConfig);

        testingReaderContext =
                KinesisContextProvider.KinesisTestingContext.getKinesisTestingContext(
                        metricListener);
        sourceReader =
                new KinesisStreamsSourceReader<>(
                        new SingleThreadFetcherManager<>(
                                splitReaderSupplier::get, new Configuration()),
                        new KinesisStreamsRecordEmitter<>(null),
                        new Configuration(),
                        testingReaderContext,
                        shardMetricGroupMap);
    }

    @Test
    void testInitializedState() throws Exception {
        KinesisShardSplit split = getTestSplit();
        assertThat(sourceReader.initializedState(split))
                .usingRecursiveComparison()
                .isEqualTo(new KinesisShardSplitState(split));
    }

    @Test
    void testToSplitType() throws Exception {
        KinesisShardSplitState splitState = getTestSplitState();
        String splitId = splitState.getSplitId();
        assertThat(sourceReader.toSplitType(splitId, splitState))
                .usingRecursiveComparison()
                .isEqualTo(splitState.getKinesisShardSplit());
    }

    @Test
    void testOnSplitFinishedEventSent() {
        KinesisShardSplit split = getTestSplit();

        testingReaderContext.clearSentEvents();

        sourceReader.onSplitFinished(
                Collections.singletonMap(split.splitId(), new KinesisShardSplitState(split)));

        List<SourceEvent> events = testingReaderContext.getSentEvents();

        Set<String> expectedSplitIds = Collections.singleton(split.splitId());
        assertThat(events)
                .singleElement()
                .isInstanceOf(SplitsFinishedEvent.class)
                .usingRecursiveComparison()
                .isEqualTo(new SplitsFinishedEvent(expectedSplitIds));
    }

    @Test
    void testOnSplitFinishedShardMetricGroupUnregistered() throws Exception {
        KinesisShardSplit split = getTestSplit();

        List<KinesisShardSplit> splits = Collections.singletonList(split);

        sourceReader.addSplits(splits);
        sourceReader.isAvailable().get();

        assertThat(shardMetricGroupMap.get(split.getShardId())).isNotNull();

        sourceReader.onSplitFinished(
                Collections.singletonMap(split.getShardId(), new KinesisShardSplitState(split)));

        assertThat(shardMetricGroupMap.get(split.getShardId())).isNull();
    }

    @Test
    void testAddSplitsRegistersAndUpdatesShardMetricGroup() throws Exception {
        KinesisShardSplit split = getTestSplit();

        List<KinesisShardSplit> splits = Collections.singletonList(split);
        sourceReader.addSplits(splits);

        // Wait for fetcher tasks to finish to assert after the metric is registered and updated.
        sourceReader.isAvailable().get();

        assertThat(shardMetricGroupMap.get(split.getShardId())).isNotNull();

        TestUtil.assertMillisBehindLatest(
                split, TestUtil.MILLIS_BEHIND_LATEST_TEST_VALUE, metricListener);
    }
}
