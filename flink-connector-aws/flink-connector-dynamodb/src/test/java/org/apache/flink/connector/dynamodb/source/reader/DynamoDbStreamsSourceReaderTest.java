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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEvent;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEventContext;
import org.apache.flink.connector.dynamodb.source.metrics.DynamoDbStreamsShardMetrics;
import org.apache.flink.connector.dynamodb.source.proxy.StreamProxy;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsContextProvider;
import org.apache.flink.connector.dynamodb.source.util.TestUtil;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.operators.testutils.TestData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.STREAM_ARN;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class DynamoDbStreamsSourceReaderTest {
    private TestingReaderContext testingReaderContext;
    private DynamoDbStreamsSourceReader<TestData> sourceReader;
    private MetricListener metricListener;
    private Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap;
    private static final Duration NON_EMPTY_POLLING_DELAY_MILLIS = Duration.ofMillis(250);
    private static final Duration EMPTY_POLLING_DELAY_MILLIS = Duration.ofMillis(1000);

    @BeforeEach
    public void init() {
        metricListener = new MetricListener();
        shardMetricGroupMap = new ConcurrentHashMap<>();
        StreamProxy testStreamProxy = getTestStreamProxy();
        Supplier<PollingDynamoDbStreamsShardSplitReader> splitReaderSupplier =
                () ->
                        new PollingDynamoDbStreamsShardSplitReader(
                                testStreamProxy,
                                NON_EMPTY_POLLING_DELAY_MILLIS,
                                EMPTY_POLLING_DELAY_MILLIS,
                                new ConcurrentHashMap<>(),
                                shardMetricGroupMap);

        testingReaderContext =
                DynamoDbStreamsContextProvider.DynamoDbStreamsTestingContext
                        .getDynamoDbStreamsTestingContext(metricListener);
        sourceReader =
                new DynamoDbStreamsSourceReader<>(
                        new SingleThreadFetcherManager<>(splitReaderSupplier::get),
                        new DynamoDbStreamsRecordEmitter<>(null),
                        new Configuration(),
                        testingReaderContext,
                        new ConcurrentHashMap<>(),
                        shardMetricGroupMap);
    }

    @Test
    void testInitializedState() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        assertThat(sourceReader.initializedState(split))
                .usingRecursiveComparison()
                .isEqualTo(new DynamoDbStreamsShardSplitState(split));
    }

    @Test
    void testToSplitType() throws Exception {
        DynamoDbStreamsShardSplitState splitState = getTestSplitState();
        String splitId = splitState.getSplitId();
        assertThat(sourceReader.toSplitType(splitId, splitState))
                .usingRecursiveComparison()
                .isEqualTo(splitState.getDynamoDbStreamsShardSplit());
    }

    @Test
    void testOnSplitFinishedIsNoOp() throws Exception {
        assertThatNoException()
                .isThrownBy(() -> sourceReader.onSplitFinished(Collections.emptyMap()));
    }

    @Test
    void testOnSplitFinishedEventSent() {
        DynamoDbStreamsShardSplit split = getTestSplit();

        testingReaderContext.clearSentEvents();

        sourceReader.onSplitFinished(
                Collections.singletonMap(
                        split.splitId(), new DynamoDbStreamsShardSplitState(split)));

        List<SourceEvent> events = testingReaderContext.getSentEvents();

        Set<SplitsFinishedEventContext> expectedFinishedSplits =
                Collections.singleton(
                        new SplitsFinishedEventContext(split.splitId(), new ArrayList<>()));
        assertThat(events)
                .singleElement()
                .isInstanceOf(SplitsFinishedEvent.class)
                .usingRecursiveComparison()
                .isEqualTo(new SplitsFinishedEvent(expectedFinishedSplits));
    }

    @Test
    void testOnSplitFinishedShardMetricGroupUnregistered() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();

        List<DynamoDbStreamsShardSplit> splits = Collections.singletonList(split);

        sourceReader.addSplits(splits);
        sourceReader.isAvailable().get();

        assertThat(shardMetricGroupMap.get(split.splitId())).isNotNull();

        sourceReader.onSplitFinished(
                Collections.singletonMap(
                        split.splitId(), new DynamoDbStreamsShardSplitState(split)));

        assertThat(shardMetricGroupMap.get(split.splitId())).isNull();
    }

    @Test
    void testAddSplitsRegistersAndUpdatesShardMetricGroup() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();

        List<DynamoDbStreamsShardSplit> splits = Collections.singletonList(split);
        sourceReader.addSplits(splits);

        // Wait for fetcher tasks to finish to assert after the metric is registered and updated.
        sourceReader.isAvailable().get();

        assertThat(shardMetricGroupMap.get(split.splitId())).isNotNull();

        TestUtil.assertMillisBehindLatest(
                split, TestUtil.MILLIS_BEHIND_LATEST_TEST_VALUE, metricListener);
    }

    @Test
    void testSnapshotStateWithFinishedSplits() throws Exception {
        // Create and add a split
        DynamoDbStreamsShardSplit split = getTestSplit();
        List<DynamoDbStreamsShardSplit> splits = Collections.singletonList(split);
        sourceReader.addSplits(splits);

        // Set checkpoint ID by taking initial snapshot
        List<DynamoDbStreamsShardSplit> initialSnapshot = sourceReader.snapshotState(1L);
        assertThat(initialSnapshot).hasSize(1).containsExactly(split);

        // Simulate split finishing
        Map<String, DynamoDbStreamsShardSplitState> finishedSplits = new HashMap<>();
        finishedSplits.put(split.splitId(), new DynamoDbStreamsShardSplitState(split));
        sourceReader.onSplitFinished(finishedSplits);

        // Take another snapshot
        List<DynamoDbStreamsShardSplit> snapshotSplits = sourceReader.snapshotState(2L);
        List<DynamoDbStreamsShardSplit> snapshotFinishedSplits =
                snapshotSplits.stream()
                        .filter(DynamoDbStreamsShardSplit::isFinished)
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
        DynamoDbStreamsShardSplit finishedSplit1 =
                new DynamoDbStreamsShardSplit(
                        STREAM_ARN, "finished-split-1", StartingPosition.fromStart(), null, true);
        DynamoDbStreamsShardSplit finishedSplit2 =
                new DynamoDbStreamsShardSplit(
                        STREAM_ARN, "finished-split-2", StartingPosition.fromStart(), null, true);

        // Create active split
        DynamoDbStreamsShardSplit activeSplit = getTestSplit();

        List<DynamoDbStreamsShardSplit> allSplits =
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
                .allSatisfy(
                        e -> {
                            SplitsFinishedEvent event = (SplitsFinishedEvent) e;
                            assertThat(event.getFinishedSplits()).hasSize(1);
                            assertThat(
                                            event.getFinishedSplits().stream()
                                                    .map(SplitsFinishedEventContext::getSplitId))
                                    .containsAnyOf("finished-split-1", "finished-split-2");
                        });

        // Verify metrics registered only for active split
        assertThat(shardMetricGroupMap).hasSize(1).containsKey(activeSplit.splitId());
    }

    @Test
    void testNotifyCheckpointCompleteRemovesFinishedSplits() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        List<DynamoDbStreamsShardSplit> splits = Collections.singletonList(split);

        sourceReader.addSplits(splits);

        // Simulate splits finishing at different checkpoints
        Map<String, DynamoDbStreamsShardSplitState> finishedSplits1 = new HashMap<>();
        DynamoDbStreamsShardSplit finishedSplit1 =
                new DynamoDbStreamsShardSplit(
                        STREAM_ARN, "split-1", StartingPosition.fromStart(), null);
        finishedSplits1.put("split-1", new DynamoDbStreamsShardSplitState(finishedSplit1));
        sourceReader.snapshotState(1L); // Set checkpoint ID
        sourceReader.onSplitFinished(finishedSplits1);

        Map<String, DynamoDbStreamsShardSplitState> finishedSplits2 = new HashMap<>();
        DynamoDbStreamsShardSplit finishedSplit2 =
                new DynamoDbStreamsShardSplit(
                        STREAM_ARN, "split-2", StartingPosition.fromStart(), null);
        finishedSplits2.put("split-2", new DynamoDbStreamsShardSplitState(finishedSplit2));
        sourceReader.snapshotState(2L); // Set checkpoint ID
        sourceReader.onSplitFinished(finishedSplits2);

        // Take snapshot to verify initial state
        List<DynamoDbStreamsShardSplit> snapshotSplits = sourceReader.snapshotState(3L);

        assertThat(snapshotSplits).hasSize(3);
        assertThat(
                        snapshotSplits.stream()
                                .filter(DynamoDbStreamsShardSplit::isFinished)
                                .map(DynamoDbStreamsShardSplit::splitId)
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
                                .filter(DynamoDbStreamsShardSplit::isFinished)
                                .map(DynamoDbStreamsShardSplit::splitId)
                                .collect(Collectors.toList()))
                .hasSize(1)
                .first()
                .isEqualTo("split-2");
    }
}
