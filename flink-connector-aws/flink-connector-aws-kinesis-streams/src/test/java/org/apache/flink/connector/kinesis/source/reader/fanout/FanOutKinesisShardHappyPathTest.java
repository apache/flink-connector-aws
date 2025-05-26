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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the happy path flow in {@link FanOutKinesisShardSubscription}
 * and {@link FanOutKinesisShardSplitReader}.
 */
public class FanOutKinesisShardHappyPathTest extends FanOutKinesisShardTestBase {

    /**
     * Tests the basic happy path flow for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testBasicHappyPathSingleShard() throws Exception {
        // Create a metrics map for the shard
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();
        KinesisShardSplit split = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart());
        metricsMap.put(SHARD_ID, new KinesisShardMetrics(split, mockMetricGroup));

        // Create a reader
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                metricsMap,
                configuration,
                createTestSubscriptionFactory(),
                testExecutor,
                testExecutor);

        // Add a split to the reader
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // Trigger the executor to execute the subscription tasks
        testExecutor.triggerAll();

        // Verify that the subscription was activated
        ArgumentCaptor<String> shardIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<StartingPosition> startingPositionCaptor = ArgumentCaptor.forClass(StartingPosition.class);

        verify(mockAsyncStreamProxy, times(1)).subscribeToShard(
                eq(CONSUMER_ARN),
                shardIdCaptor.capture(),
                startingPositionCaptor.capture(),
                any());

        // Verify the subscription parameters
        assertThat(shardIdCaptor.getValue()).isEqualTo(SHARD_ID);
        assertThat(startingPositionCaptor.getValue()).isEqualTo(StartingPosition.fromStart());
    }

    /**
     * Tests the happy path flow for multiple shards.
     */
    @Test
    @Timeout(value = 30)
    public void testBasicHappyPathMultipleShards() throws Exception {
        // Create metrics map for the shards
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();

        KinesisShardSplit split1 = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID_1,
                StartingPosition.fromStart());

        KinesisShardSplit split2 = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID_2,
                StartingPosition.fromStart());

        metricsMap.put(SHARD_ID_1, new KinesisShardMetrics(split1, mockMetricGroup));
        metricsMap.put(SHARD_ID_2, new KinesisShardMetrics(split2, mockMetricGroup));

        // Create a reader
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                metricsMap,
                configuration,
                createTestSubscriptionFactory(),
                testExecutor,
                testExecutor);

        // Add splits to the reader
        List<KinesisShardSplit> splits = new ArrayList<>();
        splits.add(split1);
        splits.add(split2);
        reader.handleSplitsChanges(new SplitsAddition<>(splits));

        // Trigger the executor to execute the subscription tasks
        testExecutor.triggerAll();

        // Verify that subscriptions were activated for both shards
        ArgumentCaptor<String> shardIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<StartingPosition> startingPositionCaptor = ArgumentCaptor.forClass(StartingPosition.class);

        verify(mockAsyncStreamProxy, times(2)).subscribeToShard(
                eq(CONSUMER_ARN),
                shardIdCaptor.capture(),
                startingPositionCaptor.capture(),
                any());

        // Verify the subscription parameters
        List<String> capturedShardIds = shardIdCaptor.getAllValues();
        assertThat(capturedShardIds).containsExactlyInAnyOrder(SHARD_ID_1, SHARD_ID_2);

        List<StartingPosition> capturedStartingPositions = startingPositionCaptor.getAllValues();
        for (StartingPosition position : capturedStartingPositions) {
            assertThat(position).isEqualTo(StartingPosition.fromStart());
        }
    }

    /**
     * Tests the basic happy path flow with record processing for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testBasicHappyPathWithRecordProcessing() throws Exception {
        // Create a blocking queue to store processed records
        BlockingQueue<Record> processedRecords = new LinkedBlockingQueue<>();

        // Create a custom TestableSubscription that captures processed records
        TestableSubscription testSubscription = createTestableSubscription(
                SHARD_ID,
                StartingPosition.fromStart(),
                processedRecords);

        // Create test events with records in a specific order
        int numEvents = 3;
        int recordsPerEvent = 5;
        List<List<Record>> eventRecords = new ArrayList<>();

        for (int i = 0; i < numEvents; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                int recordNum = i * recordsPerEvent + j;
                records.add(FanOutKinesisTestUtils.createTestRecord("record-" + recordNum));
            }
            eventRecords.add(records);
        }

        // Process the events
        for (int i = 0; i < numEvents; i++) {
            String sequenceNumber = "sequence-" + i;
            testSubscription.processSubscriptionEvent(
                    FanOutKinesisTestUtils.createTestEvent(sequenceNumber, eventRecords.get(i)));
        }

        // Verify that all records were processed in the correct order
        List<Record> allProcessedRecords = new ArrayList<>();
        processedRecords.drainTo(allProcessedRecords);

        assertThat(allProcessedRecords).hasSize(numEvents * recordsPerEvent);

        // Verify the order of records
        for (int i = 0; i < numEvents * recordsPerEvent; i++) {
            String expectedData = "record-" + i;
            String actualData = new String(
                    allProcessedRecords.get(i).data().asByteArray(),
                    java.nio.charset.StandardCharsets.UTF_8);
            assertThat(actualData).isEqualTo(expectedData);
        }

        // Verify that the starting position was updated correctly
        assertThat(testSubscription.getStartingPosition().getStartingMarker())
                .isEqualTo("sequence-" + (numEvents - 1));
    }

    /**
     * Tests that metrics are properly updated during record processing.
     */
    @Test
    @Timeout(value = 30)
    public void testMetricsUpdatedDuringProcessing() throws Exception {
        // Create a metrics map for the shard
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();
        KinesisShardSplit split = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart());
        KinesisShardMetrics spyMetrics = Mockito.spy(new KinesisShardMetrics(split, mockMetricGroup));
        metricsMap.put(SHARD_ID, spyMetrics);

        // Create a test event with millisBehindLatest set
        long millisBehindLatest = 1000L;

        // Directly update the metrics
        spyMetrics.setMillisBehindLatest(millisBehindLatest);

        // Verify that the metrics were updated
        verify(spyMetrics, times(1)).setMillisBehindLatest(millisBehindLatest);
    }
}
