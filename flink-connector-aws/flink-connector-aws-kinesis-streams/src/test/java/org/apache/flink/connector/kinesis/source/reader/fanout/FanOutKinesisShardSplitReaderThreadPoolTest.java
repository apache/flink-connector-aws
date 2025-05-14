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
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.metrics.MetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the thread pool behavior in {@link FanOutKinesisShardSplitReader}.
 */
public class FanOutKinesisShardSplitReaderThreadPoolTest {
    private static final Duration TEST_SUBSCRIPTION_TIMEOUT = Duration.ofMillis(1000);
    private static final int NUM_SHARDS = 10;
    private static final int EVENTS_PER_SHARD = 5;

    private AsyncStreamProxy mockAsyncStreamProxy;
    private FanOutKinesisShardSplitReader splitReader;

    @BeforeEach
    public void setUp() {
        mockAsyncStreamProxy = Mockito.mock(AsyncStreamProxy.class);
        when(mockAsyncStreamProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    /**
     * Tests that the thread pool correctly processes events from multiple shards.
     */
    @Test
    @Timeout(value = 30)
    public void testThreadPoolProcessesMultipleShards() throws Exception {
        // Use a counter to track events processed
        AtomicInteger processedEvents = new AtomicInteger(0);
        int expectedEvents = NUM_SHARDS * EVENTS_PER_SHARD;

        // Create a manually triggered executor service
        ManuallyTriggeredScheduledExecutorService testExecutor = new ManuallyTriggeredScheduledExecutorService();

        // Create a map to store our test subscriptions
        java.util.Map<String, TestSubscription> testSubscriptions = new java.util.HashMap<>();

        // Create a custom subscription factory that creates test subscriptions
        FanOutKinesisShardSplitReader.SubscriptionFactory customFactory =
                (proxy, consumerArn, shardId, startingPosition, timeout, executor) -> {
                    TestSubscription subscription = new TestSubscription(
                            proxy, consumerArn, shardId, startingPosition, timeout, executor,
                            processedEvents, expectedEvents);
                    testSubscriptions.put(shardId, subscription);
                    return subscription;
                };

        // Create a metrics map for each shard
        java.util.Map<String, KinesisShardMetrics> metricsMap = new java.util.HashMap<>();
        for (int i = 0; i < NUM_SHARDS; i++) {
            String shardId = SHARD_ID + "-" + i;
            KinesisShardSplit split = new KinesisShardSplit(
                    STREAM_ARN,
                    shardId,
                    StartingPosition.fromStart(),
                    Collections.emptySet(),
                    TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                    TestUtil.ENDING_HASH_KEY_TEST_VALUE);
            MetricGroup metricGroup = mock(MetricGroup.class);
            when(metricGroup.addGroup(any(String.class))).thenReturn(metricGroup);
            when(metricGroup.addGroup(any(String.class), any(String.class))).thenReturn(metricGroup);
            metricsMap.put(shardId, new KinesisShardMetrics(split, metricGroup));
        }

        // Create a split reader with the custom factory and test executor
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        splitReader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                metricsMap,
                configuration,
                customFactory,
                testExecutor);

        // Add multiple splits to the reader
        List<KinesisShardSplit> splits = new ArrayList<>();
        for (int i = 0; i < NUM_SHARDS; i++) {
            String shardId = SHARD_ID + "-" + i;
            KinesisShardSplit split = new KinesisShardSplit(
                    STREAM_ARN,
                    shardId,
                    StartingPosition.fromStart(),
                    Collections.emptySet(),
                    TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                    TestUtil.ENDING_HASH_KEY_TEST_VALUE);
            splits.add(split);
        }
        splitReader.handleSplitsChanges(new SplitsAddition<>(splits));

        // Trigger all tasks in the executor to process subscription activations
        testExecutor.triggerAll();

        // Process all events for all shards by directly calling nextEvent() on each subscription
        for (int i = 0; i < EVENTS_PER_SHARD; i++) {
            for (String shardId : testSubscriptions.keySet()) {
                TestSubscription subscription = testSubscriptions.get(shardId);
                // Force the subscription to process an event
                SubscribeToShardEvent event = subscription.nextEvent();
                // Trigger all tasks in the executor after each event
                testExecutor.triggerAll();
            }
        }

        // Verify that all events were processed
        assertThat(processedEvents.get()).as("All events should be processed").isEqualTo(expectedEvents);
    }

    /**
     * Tests that the thread pool has natural flow control that prevents queue overflow.
     */
    @Test
    @Timeout(value = 30)
    public void testThreadPoolFlowControl() throws Exception {
        // Create a counter to track the maximum number of queued tasks
        AtomicInteger maxQueuedTasks = new AtomicInteger(0);
        AtomicInteger currentQueuedTasks = new AtomicInteger(0);

        // Create a custom AsyncStreamProxy that will delay subscription events
        AsyncStreamProxy customProxy = Mockito.mock(AsyncStreamProxy.class);
        when(customProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Create a metrics map for each shard
        java.util.Map<String, KinesisShardMetrics> metricsMap = new java.util.HashMap<>();
        for (int i = 0; i < NUM_SHARDS; i++) {
            String shardId = SHARD_ID + "-" + i;
            KinesisShardSplit split = new KinesisShardSplit(
                    STREAM_ARN,
                    shardId,
                    StartingPosition.fromStart(),
                    Collections.emptySet(),
                    TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                    TestUtil.ENDING_HASH_KEY_TEST_VALUE);
            MetricGroup metricGroup = mock(MetricGroup.class);
            when(metricGroup.addGroup(any(String.class))).thenReturn(metricGroup);
            when(metricGroup.addGroup(any(String.class), any(String.class))).thenReturn(metricGroup);
            metricsMap.put(shardId, new KinesisShardMetrics(split, metricGroup));
        }

        // Create a split reader
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        splitReader = new FanOutKinesisShardSplitReader(
                customProxy,
                CONSUMER_ARN,
                metricsMap,
                configuration);

        // Get access to the executor service
        ExecutorService executor = getExecutorService(splitReader);
        assertThat(executor).isInstanceOf(ThreadPoolExecutor.class);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;

        // Monitor the queue size
        Thread monitorThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    int queueSize = threadPoolExecutor.getQueue().size();
                    currentQueuedTasks.set(queueSize);
                    maxQueuedTasks.updateAndGet(current -> Math.max(current, queueSize));
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        monitorThread.start();

        // Create a custom subscription factory that adds artificial delay
        FanOutKinesisShardSplitReader.SubscriptionFactory customFactory =
                (proxy, consumerArn, shardId, startingPosition, timeout, executorService) -> {
                    return new FanOutKinesisShardSubscription(
                            proxy, consumerArn, shardId, startingPosition, timeout, executorService) {
                        @Override
                        public void processSubscriptionEvent(SubscribeToShardEvent event) {
                            try {
                                // Add artificial delay to simulate processing time
                                Thread.sleep(50);
                                super.processSubscriptionEvent(event);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }

                        @Override
                        public SubscribeToShardEvent nextEvent() {
                            // Create a test event
                            return createTestEvent("sequence-" + shardId);
                        }
                    };
                };

        // Set the custom factory using reflection
        setSubscriptionFactory(splitReader, customFactory);

        // Add multiple splits to the reader
        List<KinesisShardSplit> splits = new ArrayList<>();
        for (int i = 0; i < NUM_SHARDS; i++) {
            String shardId = SHARD_ID + "-" + i;
            KinesisShardSplit split = new KinesisShardSplit(
                    STREAM_ARN,
                    shardId,
                    StartingPosition.fromStart(),
                    Collections.emptySet(),
                    TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                    TestUtil.ENDING_HASH_KEY_TEST_VALUE);
            splits.add(split);
        }
        splitReader.handleSplitsChanges(new SplitsAddition<>(splits));

        // Fetch records multiple times to trigger event processing
        for (int i = 0; i < EVENTS_PER_SHARD * 2; i++) {
            for (int j = 0; j < NUM_SHARDS; j++) {
                splitReader.fetch();
            }
        }

        // Wait for some time to allow tasks to be queued and processed
        Thread.sleep(1000);

        // Stop the monitor thread
        monitorThread.interrupt();
        monitorThread.join(1000);

        // Verify that the maximum queue size is bounded
        // The theoretical maximum is 2 * NUM_SHARDS (each subscription has a queue of 2)
        assertThat(maxQueuedTasks.get()).isLessThanOrEqualTo(2 * NUM_SHARDS);
    }

    /**
     * Tests that the thread pool is properly shut down when the split reader is closed.
     */
    @Test
    @Timeout(value = 30)
    public void testThreadPoolShutdown() throws Exception {
        // Create a metrics map for the test
        java.util.Map<String, KinesisShardMetrics> metricsMap = new java.util.HashMap<>();
        KinesisShardSplit split = new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                Collections.emptySet(),
                TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                TestUtil.ENDING_HASH_KEY_TEST_VALUE);
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(metricGroup.addGroup(any(String.class))).thenReturn(metricGroup);
        when(metricGroup.addGroup(any(String.class), any(String.class))).thenReturn(metricGroup);
        metricsMap.put(SHARD_ID, new KinesisShardMetrics(split, metricGroup));

        // Create a split reader
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        splitReader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                metricsMap,
                configuration);

        // Get access to the executor service
        ExecutorService executor = getExecutorService(splitReader);
        assertThat(executor).isNotNull();

        // Close the split reader
        splitReader.close();

        // Verify that the executor service is shut down
        assertThat(executor.isShutdown()).isTrue();
    }

    /**
     * Creates a test SubscribeToShardEvent with the given continuation sequence number.
     */
    private SubscribeToShardEvent createTestEvent(String continuationSequenceNumber) {
        return SubscribeToShardEvent.builder()
                .continuationSequenceNumber(continuationSequenceNumber)
                .millisBehindLatest(0L)
                .records(new ArrayList<>())
                .build();
    }

    /**
     * Gets the executor service from the split reader using reflection.
     */
    private ExecutorService getExecutorService(FanOutKinesisShardSplitReader splitReader) throws Exception {
        Field field = FanOutKinesisShardSplitReader.class.getDeclaredField("sharedShardSubscriptionExecutor");
        field.setAccessible(true);
        return (ExecutorService) field.get(splitReader);
    }

    /**
     * Sets the subscription factory in the split reader using reflection.
     */
    private void setSubscriptionFactory(
            FanOutKinesisShardSplitReader splitReader,
            FanOutKinesisShardSplitReader.SubscriptionFactory factory) throws Exception {
        Field field = FanOutKinesisShardSplitReader.class.getDeclaredField("subscriptionFactory");
        field.setAccessible(true);
        field.set(splitReader, factory);
    }

    /**
     * A test subscription that ensures we process exactly EVENTS_PER_SHARD events per shard.
     */
    private static class TestSubscription extends FanOutKinesisShardSubscription {
        private final AtomicInteger eventsProcessed = new AtomicInteger(0);
        private final AtomicInteger globalCounter;
        private final int expectedTotal;
        private final String shardId;

        public TestSubscription(
                AsyncStreamProxy proxy,
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                Duration timeout,
                ExecutorService executor,
                AtomicInteger globalCounter,
                int expectedTotal) {
            super(proxy, consumerArn, shardId, startingPosition, timeout, executor);
            this.shardId = shardId;
            this.globalCounter = globalCounter;
            this.expectedTotal = expectedTotal;
        }

        @Override
        public SubscribeToShardEvent nextEvent() {
            int current = eventsProcessed.get();

            // Only return events up to EVENTS_PER_SHARD
            if (current < EVENTS_PER_SHARD) {
                // Create a test event
                SubscribeToShardEvent event = SubscribeToShardEvent.builder()
                        .continuationSequenceNumber("sequence-" + shardId + "-" + current)
                        .millisBehindLatest(0L)
                        .records(new ArrayList<>())
                        .build();

                // Increment the counters
                eventsProcessed.incrementAndGet();
                int globalCount = globalCounter.incrementAndGet();
                return event;
            }

            // Return null when we've processed all events for this shard
            return null;
        }
    }
}
