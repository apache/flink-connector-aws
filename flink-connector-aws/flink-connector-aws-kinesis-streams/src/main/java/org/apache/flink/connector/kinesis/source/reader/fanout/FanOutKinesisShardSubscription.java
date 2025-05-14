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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.ExceptionUtils;

import io.netty.handler.timeout.ReadTimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.InternalFailureException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FanOutSubscription class responsible for handling the subscription to a single shard of the
 * Kinesis stream. Given a shardId, it will manage the lifecycle of the subscription, and eagerly
 * keep the next batch of records available for consumption when next polled.
 */
@Internal
public class FanOutKinesisShardSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSubscription.class);
    private static final List<Class<? extends Throwable>> RECOVERABLE_EXCEPTIONS =
            Arrays.asList(
                    InternalFailureException.class,
                    ResourceNotFoundException.class,
                    KinesisException.class,
                    ResourceInUseException.class,
                    ReadTimeoutException.class,
                    TimeoutException.class,
                    IOException.class,
                    LimitExceededException.class);

    private final AsyncStreamProxy kinesis;
    private final String consumerArn;
    private final String shardId;

    private final Duration subscriptionTimeout;

    /** Executor service to run subscription event processing tasks. */
    private final ExecutorService subscriptionEventProcessingExecutor;

    /**
     * Lock to ensure sequential processing of subscription events for this shard.
     * This lock guarantees that for each shard:
     * 1. Only one event is processed at a time
     * 2. Events are processed in the order they are received
     * 3. The critical operations (queue.put, startingPosition update, requestRecords) are executed atomically
     *
     * <p>This is essential to prevent race conditions that could lead to data loss or incorrect
     * continuation sequence numbers being used after failover.
     */
    private final Object subscriptionEventProcessingLock = new Object();

    // Queue is meant for eager retrieval of records from the Kinesis stream. We will always have 2
    // record batches available on next read.
    private final BlockingQueue<SubscribeToShardEvent> eventQueue = new LinkedBlockingQueue<>(2);
    private final AtomicBoolean subscriptionActive = new AtomicBoolean(false);
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();

    // Store the current starting position for this subscription. Will be updated each time new
    // batch of records is consumed
    private StartingPosition startingPosition;

    /**
     * Gets the current starting position for this subscription.
     *
     * @return The current starting position
     */
    public StartingPosition getStartingPosition() {
        return startingPosition;
    }

    /**
     * Checks if the subscription is active.
     *
     * @return true if the subscription is active, false otherwise
     */
    public boolean isActive() {
        return subscriptionActive.get();
    }

    private FanOutShardSubscriber shardSubscriber;

    /**
     * Creates a new FanOutKinesisShardSubscription with the specified parameters.
     *
     * @param kinesis The AsyncStreamProxy to use for Kinesis operations
     * @param consumerArn The ARN of the consumer
     * @param shardId The ID of the shard to subscribe to
     * @param startingPosition The starting position for the subscription
     * @param subscriptionTimeout The timeout for the subscription
     * @param subscriptionEventProcessingExecutor The executor service to use for processing subscription events
     */
    public FanOutKinesisShardSubscription(
            AsyncStreamProxy kinesis,
            String consumerArn,
            String shardId,
            StartingPosition startingPosition,
            Duration subscriptionTimeout,
            ExecutorService subscriptionEventProcessingExecutor) {
        this.kinesis = kinesis;
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
        this.subscriptionTimeout = subscriptionTimeout;
        this.subscriptionEventProcessingExecutor = subscriptionEventProcessingExecutor;
    }

    /** Method to allow eager activation of the subscription. */
    public void activateSubscription() {
        LOG.info(
                "Activating subscription to shard {} with starting position {} for consumer {}.",
                shardId,
                startingPosition,
                consumerArn);
        if (subscriptionActive.get()) {
            LOG.warn("Skipping activation of subscription since it is already active.");
            return;
        }

        // We have to use our own CountDownLatch to wait for subscription to be acquired because
        // subscription event is tracked via the handler.
        CountDownLatch waitForSubscriptionLatch = new CountDownLatch(1);
        shardSubscriber = new FanOutShardSubscriber(waitForSubscriptionLatch);
        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder()
                        .subscriber(() -> shardSubscriber)
                        .onError(
                                throwable -> {
                                    // Errors that occur when obtaining a subscription are thrown
                                    // here.
                                    // After subscription is acquired, these errors can be ignored.
                                    if (waitForSubscriptionLatch.getCount() > 0) {
                                        terminateSubscription(throwable);
                                        waitForSubscriptionLatch.countDown();
                                    }
                                })
                        .build();

        // We don't need to keep track of the future here because we monitor subscription success
        // using our own CountDownLatch
        kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                .exceptionally(
                        throwable -> {
                            // If consumer exists and is still activating, we want to countdown.
                            if (ExceptionUtils.findThrowable(
                                            throwable, ResourceInUseException.class)
                                    .isPresent()) {
                                waitForSubscriptionLatch.countDown();
                                return null;
                            }
                            LOG.error(
                                    "Error subscribing to shard {} with starting position {} for consumer {}.",
                                    shardId,
                                    startingPosition,
                                    consumerArn,
                                    throwable);
                            terminateSubscription(throwable);
                            return null;
                        });

        // We have to handle timeout for subscriptions separately because Java 8 does not support a
        // fluent orTimeout() methods on CompletableFuture.
        CompletableFuture.runAsync(
                () -> {
                    try {
                        if (waitForSubscriptionLatch.await(
                                subscriptionTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                            LOG.info(
                                    "Successfully subscribed to shard {} with starting position {} for consumer {}.",
                                    shardId,
                                    startingPosition,
                                    consumerArn);
                            subscriptionActive.set(true);
                            // Request first batch of records.
                            shardSubscriber.requestRecords();
                        } else {
                            String errorMessage =
                                    "Timeout when subscribing to shard "
                                            + shardId
                                            + " with starting position "
                                            + startingPosition
                                            + " for consumer "
                                            + consumerArn
                                            + ".";
                            LOG.error(errorMessage);
                            terminateSubscription(new TimeoutException(errorMessage));
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for subscription to complete.", e);
                        terminateSubscription(e);
                        Thread.currentThread().interrupt();
                    }
                });
    }

    private void terminateSubscription(Throwable t) {
        if (!subscriptionException.compareAndSet(null, t)) {
            LOG.warn(
                    "Another subscription exception has been queued, ignoring subsequent exceptions",
                    t);
        }
        shardSubscriber.cancel();
    }

    /**
     * This is the main entrypoint for this subscription class. It will retrieve the next batch of
     * records from the Kinesis stream shard. It will throw any unrecoverable exceptions encountered
     * during the subscription process.
     *
     * @return next FanOut subscription event containing records. Returns null if subscription is
     *     not yet active and fetching should be retried at a later time.
     */
    public SubscribeToShardEvent nextEvent() {
        Throwable throwable = subscriptionException.getAndSet(null);
        if (throwable != null) {
            // If consumer is still activating, we want to wait.
            if (ExceptionUtils.findThrowable(throwable, ResourceInUseException.class).isPresent()) {
                return null;
            }
            // We don't want to wrap ResourceNotFoundExceptions because it is handled via a
            // try-catch loop
            if (throwable instanceof ResourceNotFoundException) {
                throw (ResourceNotFoundException) throwable;
            }
            Optional<? extends Throwable> recoverableException =
                    RECOVERABLE_EXCEPTIONS.stream()
                            .map(clazz -> ExceptionUtils.findThrowable(throwable, clazz))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .findFirst();
            if (recoverableException.isPresent()) {
                LOG.warn(
                        "Recoverable exception encountered while subscribing to shard. Ignoring.",
                        recoverableException.get());
                shardSubscriber.cancel();
                activateSubscription();
                return null;
            }
            LOG.error("Subscription encountered unrecoverable exception.", throwable);
            throw new KinesisStreamsSourceException(
                    "Subscription encountered unrecoverable exception.", throwable);
        }

        if (!subscriptionActive.get()) {
            LOG.debug(
                    "Subscription to shard {} for consumer {} is not yet active. Skipping.",
                    shardId,
                    consumerArn);
            return null;
        }

        return eventQueue.poll();
    }

    /**
     * Implementation of {@link Subscriber} to retrieve events from Kinesis stream using Reactive
     * Streams.
     */
    private class FanOutShardSubscriber implements Subscriber<SubscribeToShardEventStream> {
        private final CountDownLatch subscriptionLatch;

        private Subscription subscription;

        private FanOutShardSubscriber(CountDownLatch subscriptionLatch) {
            this.subscriptionLatch = subscriptionLatch;
        }

        public void requestRecords() {
            subscription.request(1);
        }

        public void cancel() {
            if (!subscriptionActive.get()) {
                LOG.warn("Trying to cancel inactive subscription. Ignoring.");
                return;
            }
            subscriptionActive.set(false);
            if (subscription != null) {
                subscription.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            LOG.info(
                    "Successfully subscribed to shard {} at {} using consumer {}.",
                    shardId,
                    startingPosition,
                    consumerArn);
            this.subscription = subscription;
            subscriptionLatch.countDown();
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(
                    new SubscribeToShardResponseHandler.Visitor() {
                        @Override
                        public void visit(SubscribeToShardEvent event) {
                            // For critical path operations like processing subscription events, we need to ensure:
                            // 1. Events are processed in order (sequential processing)
                            // 2. No events are dropped (reliable processing)
                            // 3. The Netty event loop thread is not blocked (async processing)
                            // 4. The starting position is correctly updated for checkpointing

                            // Submit the event processing to the executor service
                            submitEventProcessingTask(event);
                        }
                    });
        }

        @Override
        public void onError(Throwable throwable) {
            if (!subscriptionException.compareAndSet(null, throwable)) {
                LOG.warn(
                        "Another subscription exception has been queued, ignoring subsequent exceptions",
                        throwable);
            }
        }

        @Override
        public void onComplete() {
            LOG.info("Subscription complete - {} ({})", shardId, consumerArn);
            cancel();
            activateSubscription();
        }
    }

    /**
     * Submits an event processing task to the executor service.
     * This method encapsulates the task submission logic and error handling.
     *
     * @param event The subscription event to process
     */
    private void submitEventProcessingTask(SubscribeToShardEvent event) {
        try {
            subscriptionEventProcessingExecutor.execute(() -> {
                synchronized (subscriptionEventProcessingLock) {
                    try {
                        processSubscriptionEvent(event);
                    } catch (Exception e) {
                        // For critical path operations, propagate exceptions to cause a Flink job restart
                        LOG.error("Error processing subscription event", e);
                        // Propagate the exception to the subscription exception handler
                        terminateSubscription(new KinesisStreamsSourceException(
                            "Error processing subscription event", e));
                    }
                }
            });
        } catch (Exception e) {
            // This should never happen with an unbounded queue, but if it does,
            // we need to propagate the exception to cause a Flink job restart
            LOG.error("Error submitting subscription event task", e);
            throw new KinesisStreamsSourceException(
                "Error submitting subscription event task", e);
        }
    }

    /**
     * Processes a subscription event in a separate thread from the shared executor pool.
     * This method encapsulates the critical path operations:
     * 1. Putting the event in the blocking queue (which has a capacity of 2)
     * 2. Updating the starting position for recovery after failover
     * 3. Requesting more records
     *
     * <p>These operations are executed sequentially for each shard to ensure thread safety
     * and prevent race conditions. The bounded nature of the event queue (capacity 2) combined
     * with only requesting more records after processing an event provides natural flow control,
     * effectively limiting the number of tasks in the executor's queue.
     *
     * <p>This method is made public for testing purposes.
     *
     * @param event The subscription event to process
     */
    public void processSubscriptionEvent(SubscribeToShardEvent event) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Processing event for shard {}: {}, {}",
                        shardId,
                        event.getClass().getSimpleName(),
                        event);
            }

            // Put event in queue - this is a blocking operation
            eventQueue.put(event);

            // Update the starting position to ensure we can recover after failover
            // Note: We don't need additional synchronization here because this method is already
            // called within a synchronized block on subscriptionEventProcessingLock
            startingPosition = StartingPosition.continueFromSequenceNumber(
                    event.continuationSequenceNumber());

            // Request more records
            shardSubscriber.requestRecords();

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Successfully processed event for shard {}, updated position to {}",
                        shardId,
                        startingPosition);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Consistent with current implementation - throw KinesisStreamsSourceException
            throw new KinesisStreamsSourceException(
                    "Interrupted while adding Kinesis record to internal buffer.", e);
        }
        // No catch for other exceptions - let them propagate to be handled by the AWS SDK
    }
}
