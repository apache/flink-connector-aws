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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private final ScheduledExecutorService timeoutScheduler;
    private final AsyncStreamProxy kinesis;
    private final String consumerArn;
    private final String shardId;
    private final Duration subscriptionTimeout;

    /**
     * Number of events to keep in flight per subscriber. Pipelining the fetch overlaps the server's
     * next-event work with the consumer's drain work. Must match the capacity of {@link
     * #eventQueue}.
     */
    private static final int PREFETCH = 2;

    private final BlockingQueue<SubscribeToShardEvent> eventQueue =
            new LinkedBlockingQueue<>(PREFETCH);
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();

    // All fields below are guarded by lockObject
    private final Object lockObject = new Object();
    private ScheduledFuture<?> timeoutFuture;
    private FanOutShardSubscriber shardSubscriber;
    private boolean closed = false;
    private StartingPosition startingPosition;

    public FanOutKinesisShardSubscription(
            AsyncStreamProxy kinesis,
            String consumerArn,
            String shardId,
            StartingPosition startingPosition,
            Duration subscriptionTimeout,
            ScheduledExecutorService timeoutScheduler) {
        this.kinesis = kinesis;
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
        this.subscriptionTimeout = subscriptionTimeout;
        this.timeoutScheduler = timeoutScheduler;
    }

    /** Method to allow eager activation of the subscription. */
    public void activateSubscription() {
        synchronized (lockObject) {
            if (closed) {
                LOG.info("Subscription for shard {} is closed; skipping activation.", shardId);
                return;
            }
            if (startingPosition == null) {
                LOG.info(
                        "Shard {} has been completely consumed (shard end). Skipping re-subscription.",
                        shardId);
                return;
            }
            if (shardSubscriber != null) {
                LOG.warn(
                        "Shard {} Skipping activation of subscription since one is already active or in progress.",
                        shardId);
                return;
            }

            LOG.info(
                    "Activating subscription to shard {} with starting position {} for consumer {}.",
                    shardId,
                    startingPosition,
                    consumerArn);

            FanOutShardSubscriber subscriber = new FanOutShardSubscriber();
            shardSubscriber = subscriber;

            SubscribeToShardResponseHandler responseHandler =
                    SubscribeToShardResponseHandler.builder()
                            .subscriber(() -> subscriber)
                            .onError(
                                    throwable -> {
                                        synchronized (lockObject) {
                                            if (!disposeIfActive(subscriber)) {
                                                return;
                                            }
                                        }
                                        LOG.error(
                                                "Error onError subscribing to shard {} with "
                                                        + "starting position {} for consumer {}.",
                                                shardId,
                                                startingPosition,
                                                consumerArn,
                                                throwable);
                                        setSubscriptionException(throwable);
                                    })
                            .build();

            cancelTimeoutFuture();
            timeoutFuture =
                    timeoutScheduler.schedule(
                            () -> {
                                String errorMessage =
                                        "Timeout when subscribing to shard "
                                                + shardId
                                                + " with starting position "
                                                + startingPosition
                                                + " for consumer "
                                                + consumerArn
                                                + ".";
                                synchronized (lockObject) {
                                    // The timeout future was cancelled between firing and
                                    // acquiring the lock (e.g. onSubscribe succeeded, or another
                                    // error path disposed the subscriber). Do nothing.
                                    if (timeoutFuture == null) {
                                        return;
                                    }
                                    if (!disposeIfActive(subscriber)) {
                                        return;
                                    }
                                }
                                LOG.error(errorMessage);
                                setSubscriptionException(new TimeoutException(errorMessage));
                            },
                            subscriptionTimeout.toMillis(),
                            TimeUnit.MILLISECONDS);

            kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                    .exceptionally(
                            throwable -> {
                                synchronized (lockObject) {
                                    if (!disposeIfActive(subscriber)) {
                                        return null;
                                    }
                                }
                                LOG.error(
                                        "Error exceptionally subscribing to shard {} with starting position {} for "
                                                + "consumer {}.",
                                        shardId,
                                        startingPosition,
                                        consumerArn,
                                        throwable);
                                setSubscriptionException(throwable);
                                return null;
                            });
        }
    }

    // Must be called while holding lockObject
    private void cancelTimeoutFuture() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
    }

    // Must be called while holding lockObject
    private boolean disposeIfActive(FanOutShardSubscriber subscriber) {
        if (shardSubscriber != subscriber) {
            return false;
        }
        cancelTimeoutFuture();
        shardSubscriber.cancelSubscription();
        shardSubscriber = null;
        return true;
    }

    private void setSubscriptionException(Throwable t) {
        if (!subscriptionException.compareAndSet(null, t)) {
            LOG.warn(
                    "Another subscription exception has been queued for shardId {}, ignoring subsequent exceptions",
                    shardId,
                    t);
        }
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
                        "Recoverable exception encountered for shard {} while subscribing to shard. Ignoring: {}",
                        shardId,
                        recoverableException.get());
                activateSubscription();
                return null;
            }
            LOG.error("Subscription encountered unrecoverable exception. {}", shardId, throwable);
            throw new KinesisStreamsSourceException(
                    "Subscription encountered unrecoverable exception.", throwable);
        }

        return pollAndRequestNext();
    }

    private SubscribeToShardEvent pollAndRequestNext() {
        synchronized (lockObject) {
            SubscribeToShardEvent event = eventQueue.poll();
            // If shardSubscriber is null, the subscriber has either completed or been disposed.
            // In either case, do not issue a follow-up request.
            if (event != null && shardSubscriber != null) {
                shardSubscriber.requestRecords();
            }
            return event;
        }
    }

    /**
     * Implementation of {@link Subscriber} to retrieve events from Kinesis stream using Reactive
     * Streams.
     */
    private class FanOutShardSubscriber implements Subscriber<SubscribeToShardEventStream> {

        private Subscription subscription;

        public void requestRecords() {
            // subscription can be null if onSubscribe has not yet fired on a freshly activated
            // subscriber. In that case the initial request(1) will be issued from onSubscribe
            // itself, so it is safe to skip here.
            if (subscription != null) {
                subscription.request(1);
            }
        }

        public void cancelSubscription() {
            if (subscription != null) {
                subscription.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            synchronized (lockObject) {
                if (shardSubscriber != this) {
                    // Timeout/error disposed this subscriber and a new one was created before SDK
                    // called onSubscribe
                    subscription.cancel();
                    return;
                }
                cancelTimeoutFuture();
                this.subscription = subscription;

                int priming = PREFETCH - eventQueue.size();
                if (priming > 0) {
                    subscription.request(priming);
                } else {
                    LOG.debug(
                            "Shard {} reactivated with {} buffered event(s). Skipping initial "
                                    + "priming; request(1) will come from the consumer-drain path.",
                            shardId,
                            eventQueue.size());
                }
                LOG.info(
                        "Successfully subscribed to shard {} at {} using consumer {}.",
                        shardId,
                        startingPosition,
                        consumerArn);
            }
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(
                    new SubscribeToShardResponseHandler.Visitor() {
                        @Override
                        public void visit(SubscribeToShardEvent event) {
                            synchronized (lockObject) {
                                if (shardSubscriber != FanOutShardSubscriber.this) {
                                    LOG.warn(
                                            "Ignoring late event for shard {} from a disposed "
                                                    + "subscriber; it will be re-delivered after "
                                                    + "reactivation.",
                                            shardId);
                                    return;
                                }

                                LOG.debug(
                                        "Received event: {}, {}",
                                        event.getClass().getSimpleName(),
                                        event);

                                // Non-blocking offer. Under the prefetch discipline maintained
                                // by onSubscribe (primes PREFETCH - queue.size() requests) and
                                // pollAndRequestNext (issues request(1) after each consumer
                                // drain), the invariant queue.size + outstanding == PREFETCH
                                // holds in steady state, so the queue is guaranteed to have
                                // room for each delivered event. If offer() ever returns false
                                // it indicates a protocol / state invariant violation (e.g. the
                                // server delivered an unrequested event) - fail loud rather
                                // than block the Netty event loop. The subscription will be
                                // reactivated from the previous startingPosition (which has
                                // not yet been advanced below) and the server will re-deliver
                                // this event.
                                if (!eventQueue.offer(event)) {
                                    LOG.error(
                                            "Event queue overflow for shard {}; server delivered "
                                                    + "an unrequested event. Failing subscription "
                                                    + "to recover.",
                                            shardId);

                                    if (disposeIfActive(FanOutShardSubscriber.this)) {
                                        setSubscriptionException(
                                                new IOException(
                                                        "Event queue overflow for shard "
                                                                + shardId
                                                                + "; server delivered an "
                                                                + "unrequested event."));
                                    }
                                    return;
                                }

                                if (event.continuationSequenceNumber() == null) {
                                    startingPosition = null;
                                } else {
                                    startingPosition =
                                            StartingPosition.continueFromSequenceNumber(
                                                    event.continuationSequenceNumber());
                                }
                            }
                        }
                    });
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (lockObject) {
                if (!disposeIfActive(this)) {
                    return;
                }
            }
            setSubscriptionException(throwable);
        }

        @Override
        public void onComplete() {
            synchronized (lockObject) {
                if (shardSubscriber != this) {
                    LOG.warn(
                            "Ignoring late onComplete for shard {} from a disposed subscriber.",
                            shardId);
                    return;
                }
                LOG.info("Subscription complete - {} ({})", shardId, consumerArn);
                shardSubscriber = null;
            }
            activateSubscription();
        }
    }

    public void close() {
        synchronized (lockObject) {
            closed = true;
            if (shardSubscriber != null) {
                disposeIfActive(shardSubscriber);
            }
        }
    }
}
