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

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.util.FakeKinesisFanOutBehaviorsFactory;
import org.apache.flink.connector.kinesis.source.util.FakeKinesisFanOutBehaviorsFactory.TrackCloseStreamProxy;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class FanOutKinesisShardSplitReaderTest {
    private static final String TEST_SHARD_ID = TestUtil.generateShardId(1);

    SplitReader<Record, KinesisShardSplit> splitReader;

    private AsyncStreamProxy testAsyncStreamProxy;
    private Map<String, KinesisShardMetrics> shardMetricGroupMap;
    private MetricListener metricListener;



    @BeforeEach
    public void init() {
        metricListener = new MetricListener();

        shardMetricGroupMap = new ConcurrentHashMap<>();
        shardMetricGroupMap.put(TEST_SHARD_ID, new KinesisShardMetrics(getTestSplit(TEST_SHARD_ID), metricListener.getMetricGroup()));
    }


    @Test
    public void testNoAssignedSplitsHandledGracefully() throws Exception {
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).isEmpty();

    }

    @Test
    public void testAssignedSplitHasNoRecordsHandledGracefully() throws Exception {
        // Given assigned split with no records
        testAsyncStreamProxy = FakeKinesisFanOutBehaviorsFactory.boundedShard().build();
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When fetching records
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Then retrieve no records
        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).isEmpty();
    }

    @Test
    public void testSplitWithExpiredShardHandledAsCompleted() throws Exception {
        // Given Kinesis will respond with expired shard
        testAsyncStreamProxy = FakeKinesisFanOutBehaviorsFactory.resourceNotFoundWhenObtainingSubscription();
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When fetching records
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Then shard is marked as completed
        // Then retrieve no records and mark split as complete
        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).containsExactly(TEST_SHARD_ID);
    }

    @Test
    public void testSingleAssignedSplitAllConsumed() throws Exception {
        // Given Kinesis configured with single shard
        testAsyncStreamProxy = FakeKinesisFanOutBehaviorsFactory.boundedShard()
                .withBatchCount(10)
                .withRecordsPerBatch(5)
                .build();
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When consume all records
        // Then we consume all 50 records
        consumeAllRecordsFromKinesis(splitReader, 50);
    }

    @Test
    public void testHandleEmptyCompletedShard() {
        // Given empty shard that has been completed
        testAsyncStreamProxy = FakeKinesisFanOutBehaviorsFactory.boundedShard()
                .withBatchCount(0)
                .build();
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When consume all records
        // Then we complete shard
        // Set timeout to prevent infinite loop on failure
        assertTimeoutPreemptively(
                Duration.ofSeconds(10),
                () -> {
                    int numRetrievedRecords = 0;
                    RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
                    while (retrievedRecords.finishedSplits().isEmpty()) {
                        retrievedRecords = splitReader.fetch();
                        List<Record> records = readAllRecords(retrievedRecords);
                        numRetrievedRecords += records.size();
                    }
                    assertThat(numRetrievedRecords).isEqualTo(0);
                    assertThat(retrievedRecords).isNotNull();
                    // Check that the shard has been consumed completely
                    assertThat(retrievedRecords.finishedSplits()).containsExactly(TEST_SHARD_ID);
                },
                "did not complete reading from shard within 10 seconds."
        );
    }

    @Test
    public void testFinishedSplitsReturned() {
        // Given empty shard that has been completed
        testAsyncStreamProxy = FakeKinesisFanOutBehaviorsFactory.boundedShard()
                .withBatchCount(0)
                .build();
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When consume all records
        // Then we complete shard
        // Set timeout to prevent infinite loop on failure
        assertTimeoutPreemptively(
                Duration.ofSeconds(10),
                () -> {
                    int numRetrievedRecords = 0;
                    RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
                    while (retrievedRecords.finishedSplits().isEmpty()) {
                        retrievedRecords = splitReader.fetch();
                        List<Record> records = readAllRecords(retrievedRecords);
                        numRetrievedRecords += records.size();
                    }
                    assertThat(numRetrievedRecords).isEqualTo(0);
                    assertThat(retrievedRecords).isNotNull();
                    // Check that the shard has been consumed completely
                    assertThat(retrievedRecords.finishedSplits()).containsExactly(TEST_SHARD_ID);
                },
                "did not complete reading from shard within 10 seconds."
        );
    }

    @Test
    public void testWakeUpIsNoOp() {
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);

        // When wakeup is called
        // Then no exception is thrown and no-op
        assertThatNoException().isThrownBy(splitReader::wakeUp);
    }

    @Test
    public void testPauseOrResumeAllSplits() throws Exception {
        // Given Kinesis configured with single shard
        // Configured with 10 x 5 records.
        testAsyncStreamProxy = FakeKinesisFanOutBehaviorsFactory.boundedShard()
                .withBatchCount(10)
                .withRecordsPerBatch(5)
                .build();
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);
        KinesisShardSplit testSplit = getTestSplit(TEST_SHARD_ID);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(testSplit)));

        // When consume all records
        // Then we consume 25 records
        consumeSomeRecordsFromKinesis(splitReader, 25);

        // When we pause a different split
        KinesisShardSplit differentTestSplit = getTestSplit(generateShardId(999));
        splitReader.pauseOrResumeSplits(Collections.singletonList(differentTestSplit), Collections.emptyList());
        // Then we can continue consuming records
        consumeSomeRecordsFromKinesis(splitReader, 5);

        // When we pause the target split
        splitReader.pauseOrResumeSplits(Collections.singletonList(testSplit), Collections.emptyList());
        // Then we cannot continue consuming records.
        for (int i = 0; i < 10; i++) {
            RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
            assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
            assertThat(retrievedRecords.finishedSplits()).isEmpty();
        }

        // When we resume the target split
        splitReader.pauseOrResumeSplits(Collections.emptyList(), Collections.singletonList(testSplit));
        // Then we continue consuming remaining records
        consumeAllRecordsFromKinesis(splitReader, 20);
    }

    @Test
    public void testCloseClosesStreamProxy() throws Exception {
        // Given stream proxy
        TrackCloseStreamProxy trackCloseStreamProxy = FakeKinesisFanOutBehaviorsFactory.testCloseStreamProxy();
        splitReader = new FanOutKinesisShardSplitReader(trackCloseStreamProxy, CONSUMER_ARN, shardMetricGroupMap);

        // When split reader is not closed
        // Then stream proxy is still open
        assertThat(trackCloseStreamProxy.isClosed()).isFalse();

        // When close split reader
        splitReader.close();

        // Then stream proxy is also closed
        assertThat(trackCloseStreamProxy.isClosed()).isTrue();
    }

    @Test
    public void testFetchUpdatesTheMillisBehindLatestMetric() {
        // When split is not consumed from
        // Then MillisBehindLatest is -1L
        KinesisShardSplit testSplit = getTestSplit(TEST_SHARD_ID);
        TestUtil.assertMillisBehindLatest(testSplit, -1L, metricListener);

        // When split is consumed
        testAsyncStreamProxy = FakeKinesisFanOutBehaviorsFactory.boundedShard()
                .withBatchCount(1)
                .withRecordsPerBatch(5)
                .withMillisBehindLatest(1234L)
                .build();
        splitReader = new FanOutKinesisShardSplitReader(testAsyncStreamProxy, CONSUMER_ARN, shardMetricGroupMap);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));
        consumeAllRecordsFromKinesis(splitReader, 5);

        // Then MillisBehindLatest is updated
        TestUtil.assertMillisBehindLatest(testSplit, 1234L, metricListener);
    }


    private void consumeAllRecordsFromKinesis(SplitReader<Record, KinesisShardSplit> splitReader, int numRecords) {
        consumeRecordsFromKinesis(splitReader, numRecords, true);
    }

    private void consumeSomeRecordsFromKinesis(SplitReader<Record, KinesisShardSplit> splitReader, int numRecords) {
        consumeRecordsFromKinesis(splitReader, numRecords, false);
    }

    private void consumeRecordsFromKinesis(SplitReader<Record, KinesisShardSplit> splitReader, int numRecords, boolean checkForShardCompletion) {
        // Set timeout to prevent infinite loop on failure
        assertTimeoutPreemptively(
                Duration.ofSeconds(10),
                () -> {
                    int numRetrievedRecords = 0;
                    RecordsWithSplitIds<Record> retrievedRecords = null;
                    while (numRetrievedRecords < numRecords) {
                        retrievedRecords = splitReader.fetch();
                        List<Record> records = readAllRecords(retrievedRecords);
                        numRetrievedRecords += records.size();
                    }
                    assertThat(numRetrievedRecords).isEqualTo(numRecords);
                    assertThat(retrievedRecords).isNotNull();
                    // Check that the shard has been consumed completely
                    if (checkForShardCompletion) {
                        assertThat(retrievedRecords.finishedSplits()).containsExactly(TEST_SHARD_ID);
                    } else {
                        assertThat(retrievedRecords.finishedSplits()).isEmpty();
                    }
                },
                "did not expected " + numRecords + " records within 10 seconds."
        );
    }



    private List<Record> readAllRecords(RecordsWithSplitIds<Record> recordsWithSplitIds) {
        List<Record> outputRecords = new ArrayList<>();
        Record record;
        do {
            record = recordsWithSplitIds.nextRecordFromSplit();
            if (record != null) {
                outputRecords.add(record);
            }
        } while (record != null);

        return outputRecords;
    }
}
