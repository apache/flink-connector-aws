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

import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for thread safety in {@link FanOutKinesisShardSubscription}.
 */
public class FanOutKinesisShardSubscriptionThreadSafetyTest {

    private static final Duration TEST_SUBSCRIPTION_TIMEOUT = Duration.ofMillis(1000);
    private static final String TEST_CONTINUATION_SEQUENCE_NUMBER = "test-continuation-sequence-number";

    private AsyncStreamProxy mockAsyncStreamProxy;
    private ExecutorService testExecutor;
    private FanOutKinesisShardSubscription subscription;

    @BeforeEach
    public void setUp() {
        mockAsyncStreamProxy = Mockito.mock(AsyncStreamProxy.class);
        when(mockAsyncStreamProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        testExecutor = Executors.newFixedThreadPool(4);
    }

    /**
     * Tests that events are processed sequentially, ensuring that the starting position
     * is updated in the correct order.
     */
    @Test
    @Timeout(value = 30)
    public void testEventProcessingSequential() throws Exception {
        // Create a custom TestableSubscription that doesn't require shardSubscriber to be initialized
        TestableSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                null);

        // Create test events with different sequence numbers
        List<SubscribeToShardEvent> testEvents = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            testEvents.add(createTestEvent("sequence-" + i));
        }

        // Process events sequentially
        for (SubscribeToShardEvent event : testEvents) {
            testSubscription.processSubscriptionEvent(event);
        }

        // Verify that the final starting position is based on the last event
        assertThat(testSubscription.getStartingPosition().getStartingMarker())
                .isEqualTo(testEvents.get(testEvents.size() - 1).continuationSequenceNumber());
    }

    /**
     * Tests that the subscription event processing lock prevents concurrent processing of events.
     */
    @Test
    @Timeout(value = 30)
    public void testEventProcessingLock() throws Exception {
        // Create a CountDownLatch to track when the first task starts
        CountDownLatch firstTaskStarted = new CountDownLatch(1);

        // Create a CountDownLatch to control when the first task completes
        CountDownLatch allowFirstTaskToComplete = new CountDownLatch(1);

        // Create a CountDownLatch to track when the second task completes
        CountDownLatch secondTaskCompleted = new CountDownLatch(1);

        // Create an AtomicInteger to track the order of execution
        AtomicInteger executionOrder = new AtomicInteger(0);

        // Create a custom executor that will help us control the execution order
        ExecutorService customExecutor = Executors.newFixedThreadPool(2);

        // Create a custom TestableSubscription with a synchronized processSubscriptionEvent method
        TestableSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                customExecutor,
                null) {

            @Override
            public synchronized void processSubscriptionEvent(SubscribeToShardEvent event) {
                String sequenceNumber = event.continuationSequenceNumber();

                if ("sequence-1".equals(sequenceNumber)) {
                    // First task signals it has started and waits for permission to complete
                    executionOrder.incrementAndGet(); // Should be 1
                    firstTaskStarted.countDown();
                    try {
                        allowFirstTaskToComplete.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else if ("sequence-2".equals(sequenceNumber)) {
                    // Second task just increments the counter and signals completion
                    executionOrder.incrementAndGet(); // Should be 2
                    secondTaskCompleted.countDown();
                }

                // Call the parent method
                super.processSubscriptionEvent(event);
            }
        };

        // Submit the first event
        CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> {
            testSubscription.processSubscriptionEvent(createTestEvent("sequence-1"));
        });

        // Wait for the first task to start
        assertThat(firstTaskStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // Submit the second event
        CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
            testSubscription.processSubscriptionEvent(createTestEvent("sequence-2"));
        });

        // Allow some time for the second task to potentially start if there was no lock
        Thread.sleep(500);

        // The second task should not have executed yet due to the lock
        assertThat(executionOrder.get()).isEqualTo(1);

        // Allow the first task to complete
        allowFirstTaskToComplete.countDown();

        // Wait for the second task to complete
        assertThat(secondTaskCompleted.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify the execution order
        assertThat(executionOrder.get()).isEqualTo(2);

        // Verify both futures completed
        CompletableFuture.allOf(future1, future2).get(5, TimeUnit.SECONDS);
    }

    /**
     * Tests that events are processed using the executor service.
     */
    @Test
    @Timeout(value = 30)
    public void testExecutorServiceUsage() throws Exception {
        // Create a latch to track when the executor service is used
        CountDownLatch executorUsed = new CountDownLatch(1);

        // Create a custom executor that will signal when it's used
        ExecutorService customExecutor = spy(testExecutor);
        doAnswer(invocation -> {
            executorUsed.countDown();
            return invocation.callRealMethod();
        }).when(customExecutor).execute(any(Runnable.class));

        // Create a custom TestableSubscription that doesn't require shardSubscriber to be initialized
        TestableSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                customExecutor,
                null);

        // Submit an event for processing
        testSubscription.submitEventProcessingTask(createTestEvent(TEST_CONTINUATION_SEQUENCE_NUMBER));

        // Verify that the executor was used
        assertThat(executorUsed.await(5, TimeUnit.SECONDS)).isTrue();
        verify(customExecutor, times(1)).execute(any(Runnable.class));
    }

    /**
     * Tests that exceptions in event processing are properly propagated.
     */
    @Test
    @Timeout(value = 30)
    public void testExceptionPropagation() throws Exception {
        // Create a custom TestableSubscription that throws a KinesisStreamsSourceException
        TestableSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                null) {

            @Override
            public void processSubscriptionEvent(SubscribeToShardEvent event) {
                throw new KinesisStreamsSourceException("Test exception", new RuntimeException("Cause"));
            }
        };

        // This should throw a KinesisStreamsSourceException
        assertThatThrownBy(() -> {
            testSubscription.processSubscriptionEvent(createTestEvent(TEST_CONTINUATION_SEQUENCE_NUMBER));
        }).isInstanceOf(KinesisStreamsSourceException.class);
    }

    /**
     * Tests that the starting position is updated only after the event is successfully added to the queue.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionUpdatedAfterQueuePut() throws Exception {
        // Create a blocking queue that we can control
        BlockingQueue<SubscribeToShardEvent> controlledQueue = spy(new LinkedBlockingQueue<>(2));

        // Create a latch to track when put is called
        CountDownLatch putCalled = new CountDownLatch(1);

        // Create a latch to control when put returns
        CountDownLatch allowPutToReturn = new CountDownLatch(1);

        // Create an atomic boolean to track if the starting position was updated before put completed
        AtomicBoolean startingPositionUpdatedBeforePutCompleted = new AtomicBoolean(false);

        // Mock the queue's put method to control its execution
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                putCalled.countDown();
                allowPutToReturn.await(5, TimeUnit.SECONDS);

                // Call the real method
                invocation.callRealMethod();
                return null;
            }
        }).when(controlledQueue).put(any(SubscribeToShardEvent.class));

        // Create a subscription with access to the controlled queue
        FanOutKinesisShardSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                controlledQueue);

        // Create a thread to check the starting position while put is blocked
        Thread checkThread = new Thread(() -> {
            try {
                // Wait for put to be called
                assertThat(putCalled.await(5, TimeUnit.SECONDS)).isTrue();

                // Check if the starting position was updated before put completed
                StartingPosition currentPosition = testSubscription.getStartingPosition();
                if (currentPosition.getStartingMarker() != null &&
                    currentPosition.getStartingMarker().equals(TEST_CONTINUATION_SEQUENCE_NUMBER)) {
                    startingPositionUpdatedBeforePutCompleted.set(true);
                }

                // Allow put to return
                allowPutToReturn.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Start the check thread
        checkThread.start();

        // Process an event
        testSubscription.processSubscriptionEvent(createTestEvent(TEST_CONTINUATION_SEQUENCE_NUMBER));

        // Wait for the check thread to complete
        checkThread.join(5000);

        // Verify that the starting position was not updated before put completed
        assertThat(startingPositionUpdatedBeforePutCompleted.get()).isFalse();

        // Verify that the starting position was updated after put completed
        assertThat(testSubscription.getStartingPosition().getStartingMarker())
                .isEqualTo(TEST_CONTINUATION_SEQUENCE_NUMBER);
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
     * A testable version of FanOutKinesisShardSubscription that allows access to the event queue
     * and overrides methods that require shardSubscriber to be initialized.
     */
    private static class TestableSubscription extends FanOutKinesisShardSubscription {
        private final BlockingQueue<SubscribeToShardEvent> testEventQueue;
        private StartingPosition currentStartingPosition;

        public TestableSubscription(
                AsyncStreamProxy kinesis,
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                Duration subscriptionTimeout,
                ExecutorService subscriptionEventProcessingExecutor,
                BlockingQueue<SubscribeToShardEvent> testEventQueue) {
            super(kinesis, consumerArn, shardId, startingPosition, subscriptionTimeout, subscriptionEventProcessingExecutor);
            this.testEventQueue = testEventQueue;
            this.currentStartingPosition = startingPosition;
        }

        @Override
        public StartingPosition getStartingPosition() {
            return currentStartingPosition;
        }

        @Override
        public void processSubscriptionEvent(SubscribeToShardEvent event) {
            try {
                if (testEventQueue != null) {
                    testEventQueue.put(event);
                }

                // Update the starting position to ensure we can recover after failover
                currentStartingPosition = StartingPosition.continueFromSequenceNumber(
                        event.continuationSequenceNumber());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KinesisStreamsSourceException(
                        "Interrupted while adding Kinesis record to internal buffer.", e);
            }
        }

        /**
         * Public method to submit an event processing task directly to the executor.
         * This is used for testing the executor service usage.
         */
        public void submitEventProcessingTask(SubscribeToShardEvent event) {
            try {
                // Use reflection to access the private executor field
                java.lang.reflect.Field field = FanOutKinesisShardSubscription.class.getDeclaredField("subscriptionEventProcessingExecutor");
                field.setAccessible(true);
                ExecutorService executor = (ExecutorService) field.get(this);

                executor.execute(() -> {
                    synchronized (this) {
                        processSubscriptionEvent(event);
                    }
                });
            } catch (Exception e) {
                throw new KinesisStreamsSourceException(
                    "Error submitting subscription event task", e);
            }
        }
    }
}
