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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getFinishedTestSplit;
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

    @Test
    void testSnapshotStateWithFinishedSplits() throws Exception {
        // Create and add a split
        KinesisShardSplit split = getTestSplit();
        List<KinesisShardSplit> splits = Collections.singletonList(split);
        sourceReader.addSplits(splits);

        // Set checkpoint ID by taking initial snapshot
        List<KinesisShardSplit> initialSnapshot = sourceReader.snapshotState(1L);
        assertThat(initialSnapshot).hasSize(1).containsExactly(split);

        // Simulate split finishing
        Map<String, KinesisShardSplitState> finishedSplits = new HashMap<>();
        finishedSplits.put(split.splitId(), new KinesisShardSplitState(split));
        sourceReader.onSplitFinished(finishedSplits);

        // Take another snapshot
        List<KinesisShardSplit> snapshotSplits = sourceReader.snapshotState(2L);
        List<KinesisShardSplit> snapshotFinishedSplits =
                snapshotSplits.stream()
                        .filter(KinesisShardSplit::isFinished)
                        .collect(Collectors.toList());
        // Verify we have 2 splits - the original split and the finished split
        assertThat(snapshotSplits).hasSize(2);
        assertThat(snapshotFinishedSplits)
                .hasSize(1)
                .allSatisfy(
                        s -> {
                            assertThat(s.splitId()).isEqualTo(split.splitId());
                        });
    }

    @Test
    void testAddSplitsWithStateRestoration() throws Exception {
        KinesisShardSplit finishedSplit1 = getFinishedTestSplit("finished-split-1");
        KinesisShardSplit finishedSplit2 = getFinishedTestSplit("finished-split-2");

        // Create active split
        KinesisShardSplit activeSplit = getTestSplit();

        List<KinesisShardSplit> allSplits =
                Arrays.asList(finishedSplit1, finishedSplit2, activeSplit);

        // Clear any previous events
        testingReaderContext.clearSentEvents();

        // Add splits
        sourceReader.addSplits(allSplits);

        // Verify finished events were sent
        List<SourceEvent> events = testingReaderContext.getSentEvents();
        assertThat(events)
                .hasSize(2)
                .allMatch(e -> e instanceof SplitsFinishedEvent)
                .satisfiesExactlyInAnyOrder(
                        e ->
                                assertThat(((SplitsFinishedEvent) e).getFinishedSplitIds())
                                        .containsExactly("finished-split-1"),
                        e ->
                                assertThat(((SplitsFinishedEvent) e).getFinishedSplitIds())
                                        .containsExactly("finished-split-2"));

        // Verify metrics registered only for active split
        assertThat(shardMetricGroupMap).hasSize(1).containsKey(activeSplit.splitId());
    }

    @Test
    void testNotifyCheckpointCompleteRemovesFinishedSplits() throws Exception {
        KinesisShardSplit split = getTestSplit();
        List<KinesisShardSplit> splits = Collections.singletonList(split);

        sourceReader.addSplits(splits);

        // Simulate splits finishing at different checkpoints
        Map<String, KinesisShardSplitState> finishedSplits1 = new HashMap<>();
        KinesisShardSplit finishedSplit1 = getFinishedTestSplit("split-1");
        finishedSplits1.put("split-1", new KinesisShardSplitState(finishedSplit1));
        sourceReader.snapshotState(1L); // Set checkpoint ID
        sourceReader.onSplitFinished(finishedSplits1);

        Map<String, KinesisShardSplitState> finishedSplits2 = new HashMap<>();
        KinesisShardSplit finishedSplit2 = getFinishedTestSplit("split-2");
        finishedSplits2.put("split-2", new KinesisShardSplitState(finishedSplit2));
        sourceReader.snapshotState(2L); // Set checkpoint ID
        sourceReader.onSplitFinished(finishedSplits2);

        // Take snapshot to verify initial state
        List<KinesisShardSplit> snapshotSplits = sourceReader.snapshotState(3L);

        assertThat(snapshotSplits).hasSize(3);
        assertThat(
                        snapshotSplits.stream()
                                .filter(KinesisShardSplit::isFinished)
                                .map(KinesisShardSplit::splitId)
                                .collect(Collectors.toList()))
                .hasSize(2)
                .containsExactlyInAnyOrder("split-1", "split-2");

        // Complete checkpoint 1
        sourceReader.notifyCheckpointComplete(1L);

        // Take another snapshot to verify state after completion
        snapshotSplits = sourceReader.snapshotState(4L);

        // Verify checkpoint 1 splits were removed
        assertThat(snapshotSplits).hasSize(2);
        assertThat(
                        snapshotSplits.stream()
                                .filter(KinesisShardSplit::isFinished)
                                .map(KinesisShardSplit::splitId)
                                .collect(Collectors.toList()))
                .hasSize(1)
                .first()
                .isEqualTo("split-2");
    }
}
