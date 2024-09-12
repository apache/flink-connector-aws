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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FanOutSubscription class responsible for handling the subscription to a single shard of the Kinesis stream.
 * Given a shardId, it will manage the lifecycle of the subscription, and eagerly keep the next batch of records
 * available for consumption when next polled.
 */
@Internal
public class FanOutKinesisShardSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSubscription.class);

    private final AsyncStreamProxy kinesis;
    private final String consumerArn;
    private final String shardId;

    // TODO: configure these
    private final Duration subscriptionTimeout = Duration.ofSeconds(60);

    // TODO: configure these
    // Queue is meant for eager retrieval of records from the Kinesis stream. We will always have 2 record batches available on next read.
    private final BlockingQueue<SubscribeToShardEvent> eventQueue = new LinkedBlockingQueue<>(2);
    private final AtomicBoolean subscriptionActive = new AtomicBoolean(false);
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();

    // Store the current starting position for this subscription. Will be updated each time new batch of records is consumed
    private StartingPosition startingPosition;
    private FanOutShardSubscriber shardSubscriber;

    public FanOutKinesisShardSubscription(AsyncStreamProxy kinesis, String consumerArn, String shardId, StartingPosition startingPosition) {
        this.kinesis = kinesis;
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
    }


    /**
     * Method to allow eager activation of the subscription.
     */
    public void activateSubscription() {
        LOG.info("Activating subscription to shard {} with starting position {} for consumer {}.", shardId, startingPosition, consumerArn);
        if (subscriptionActive.get()) {
            LOG.warn("Skipping activation of subscription since it is already active.");
            return;
        }

        // We have to use our own CountDownLatch to wait for subscription to be acquired because subscription event is tracked via the handler.
        CountDownLatch waitForSubscriptionLatch = new CountDownLatch(1);
        shardSubscriber = new FanOutShardSubscriber(waitForSubscriptionLatch);
        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler.builder()
                .subscriber(() -> shardSubscriber)
                .onError(throwable -> {
                    // Errors that occur when obtaining a subscription are thrown here.
                    // After subscription is acquired, these errors can be ignored.
                    if (waitForSubscriptionLatch.getCount() > 0) {
                        terminateSubscription(throwable);
                        waitForSubscriptionLatch.countDown();
                    }})
                .build();

        // We don't need to keep track of the future here because we monitor subscription success using our own CountDownLatch
        kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                .exceptionally(throwable -> {
                    LOG.error("Error subscribing to shard {} with starting position {} for consumer {}.", shardId, startingPosition, consumerArn, throwable);
                    terminateSubscription(throwable);
                    return null;
                });

        // We have to handle timeout for subscriptions separately because Java 8 does not support a fluent orTimeout() methods on CompletableFuture.
        CompletableFuture.runAsync(() -> {
            try {
                if (waitForSubscriptionLatch.await(subscriptionTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    LOG.info("Successfully subscribed to shard {} with starting position {} for consumer {}.", shardId, startingPosition, consumerArn);
                    subscriptionActive.set(true);
                    // Request first batch of records.
                    shardSubscriber.requestRecords();
                } else {
                    String errorMessage = "Timeout when subscribing to shard "+shardId+" with starting position "+startingPosition+" for consumer "+consumerArn+".";
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
            LOG.warn("Another subscription exception has been queued, ignoring subsequent exceptions", t);
        }
        shardSubscriber.cancel();
    }

    /**
     * This is the main entrypoint for this subscription class.
     * It will retrieve the next batch of records from the Kinesis stream shard.
     * It will throw any unrecoverable exceptions encountered during the subscription process.
     *
     * @return next FanOut subscription event containing records. Returns null if subscription is not yet active and fetching should be retried at a later time.
     */
    public SubscribeToShardEvent nextEvent() {
        Throwable throwable = subscriptionException.get();
        if (throwable != null) {
            LOG.error("Subscription encountered unrecoverable exception.", throwable);
            throw new KinesisStreamsSourceException("Subscription encountered unrecoverable exception.", throwable);
        }

        if (!subscriptionActive.get()) {
            LOG.debug("Subscription to shard {} for consumer {} is not yet active. Skipping.", shardId, consumerArn);
            return null;
        }

        return eventQueue.poll();
    }

    /**
     * Implementation of {@link Subscriber} to retrieve events from Kinesis stream using Reactive Streams.
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
            subscription.cancel();
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            LOG.info("Successfully subscribed to shard {} at {} using consumer {}.", shardId, startingPosition, consumerArn);
            this.subscription = subscription;
            subscriptionLatch.countDown();
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(new SubscribeToShardResponseHandler.Visitor() {
                @Override
                public void visit(SubscribeToShardEvent event) {
                    try {
                        LOG.debug("Received event: {}, {}", event.getClass().getSimpleName(), event);
                        eventQueue.put(event);

                        // Update the starting position in case we have to recreate the subscription
                        startingPosition = StartingPosition.continueFromSequenceNumber(event.continuationSequenceNumber());

                        // Replace the record just consumed in the Queue
                        requestRecords();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new KinesisStreamsSourceException("Interrupted while adding Kinesis record to internal buffer.", e);
                    }
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            // TODO: Add recoverable error
            if (!subscriptionException.compareAndSet(null, throwable)) {
                LOG.warn("Another subscription exception has been queued, ignoring subsequent exceptions", throwable);
            }
        }

        @Override
        public void onComplete() {
            LOG.info("Subscription complete - {} ({})", shardId, consumerArn);
            subscriptionActive.set(false);
            activateSubscription();
        }
    }

}
