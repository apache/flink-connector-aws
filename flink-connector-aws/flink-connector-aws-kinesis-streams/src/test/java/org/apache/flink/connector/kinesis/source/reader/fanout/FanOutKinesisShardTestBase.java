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
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.metrics.MetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for Kinesis shard tests.
 */
public abstract class FanOutKinesisShardTestBase {

    protected static final Duration TEST_SUBSCRIPTION_TIMEOUT = Duration.ofMillis(1000);
    protected static final String SHARD_ID_1 = "shardId-000000000001";
    protected static final String SHARD_ID_2 = "shardId-000000000002";
    protected static final String CONSUMER_ARN = "abcdedf";

    protected AsyncStreamProxy mockAsyncStreamProxy;
    protected ManuallyTriggeredScheduledExecutorService testExecutor;
    protected MetricGroup mockMetricGroup;

    @BeforeEach
    public void setUp() {
        mockAsyncStreamProxy = Mockito.mock(AsyncStreamProxy.class);
        when(mockAsyncStreamProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        testExecutor = new ManuallyTriggeredScheduledExecutorService();

        mockMetricGroup = mock(MetricGroup.class);
        when(mockMetricGroup.addGroup(any(String.class))).thenReturn(mockMetricGroup);
        when(mockMetricGroup.addGroup(any(String.class), any(String.class))).thenReturn(mockMetricGroup);
    }

    /**
     * A testable version of FanOutKinesisShardSubscription that captures processed records.
     */
    protected static class TestableSubscription extends FanOutKinesisShardSubscription {
        private final BlockingQueue<Record> recordQueue;
        private volatile StartingPosition currentStartingPosition;
        private volatile boolean shouldUpdateStartingPosition = true;

        public TestableSubscription(
                AsyncStreamProxy kinesis,
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                Duration subscriptionTimeout,
                ExecutorService subscriptionEventProcessingExecutor,
                BlockingQueue<Record> recordQueue) {
            super(kinesis, consumerArn, shardId, startingPosition, subscriptionTimeout, subscriptionEventProcessingExecutor);
            this.recordQueue = recordQueue;
            this.currentStartingPosition = startingPosition;
        }

        @Override
        public StartingPosition getStartingPosition() {
            return currentStartingPosition;
        }

        @Override
        public void processSubscriptionEvent(SubscribeToShardEvent event) {
            boolean recordsProcessed = false;

            try {
                // Add all records to the queue
                if (recordQueue != null && event.records() != null) {
                    for (Record record : event.records()) {
                        recordQueue.put(record);
                    }
                    recordsProcessed = true;
                }

                // Update the starting position only if records were processed
                if (recordsProcessed && shouldUpdateStartingPosition) {
                    String continuationSequenceNumber = event.continuationSequenceNumber();
                    if (continuationSequenceNumber != null) {
                        currentStartingPosition = StartingPosition.continueFromSequenceNumber(continuationSequenceNumber);
                    }
                }

                // Note: We're not calling super.processSubscriptionEvent(event) here
                // because that would try to use the shardSubscriber which is null in our test
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while processing event", e);
            }
        }

        public void setShouldUpdateStartingPosition(boolean shouldUpdateStartingPosition) {
            this.shouldUpdateStartingPosition = shouldUpdateStartingPosition;
        }
    }

    /**
     * Creates a TestableSubscription for testing.
     *
     * @param shardId The shard ID
     * @param startingPosition The starting position
     * @param recordQueue The queue to store processed records
     * @return A TestableSubscription
     */
    protected TestableSubscription createTestableSubscription(
            String shardId,
            StartingPosition startingPosition,
            BlockingQueue<Record> recordQueue) {
        return new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                shardId,
                startingPosition,
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                recordQueue);
    }

    /**
     * Creates a test subscription factory.
     *
     * @return A test subscription factory
     */
    protected FanOutKinesisShardSplitReader.SubscriptionFactory createTestSubscriptionFactory() {
        return (proxy, consumerArn, shardId, startingPosition, timeout, executor) ->
                new FanOutKinesisShardSubscription(
                        proxy,
                        consumerArn,
                        shardId,
                        startingPosition,
                        timeout,
                        executor);
    }

    /**
     * Creates a FanOutKinesisShardSplitReader with a single shard.
     *
     * @param shardId The shard ID
     * @return A FanOutKinesisShardSplitReader
     */
    protected FanOutKinesisShardSplitReader createSplitReaderWithShard(String shardId) {
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                Mockito.mock(java.util.Map.class),
                configuration,
                createTestSubscriptionFactory(),
                testExecutor);

        // Create a split
        reader.handleSplitsChanges(
                new org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition<>(
                        java.util.Collections.singletonList(
                                FanOutKinesisTestUtils.createTestSplit(
                                        STREAM_ARN,
                                        shardId,
                                        StartingPosition.fromStart()))));

        return reader;
    }
}
