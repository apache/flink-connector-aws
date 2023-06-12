/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * This class is responsible for acquiring an Enhanced Fan Out subscription and consuming records
 * from a shard. A queue is used to buffer records between the Kinesis Proxy and Flink application.
 * This allows processing to be separated from consumption; errors thrown in the consumption layer
 * do not propagate up to application.
 *
 * <pre>{@code [
 * | ----------- Source Connector Thread ----------- |                      | --- KinesisAsyncClient Thread(s) -- |
 * | FanOutRecordPublisher | FanOutShardSubscription | == blocking queue == | KinesisProxyV2 | KinesisAsyncClient |
 * ]}</pre>
 *
 * <p>Three types of message are passed over the queue for inter-thread communication:
 *
 * <ul>
 *   <li>{@link SubscriptionNextEvent} - passes data from the network to the consumer
 *   <li>{@link SubscriptionCompleteEvent} - indicates a subscription has expired
 *   <li>{@link SubscriptionErrorEvent} - passes an exception from the network to the consumer
 * </ul>
 *
 * <p>The blocking queue has a maximum capacity of two. One slot is used for a record batch, the
 * remaining slot is reserved to completion events. At maximum capacity we will have two {@link
 * SubscribeToShardEvent} in memory (per instance of this class):
 *
 * <ul>
 *   <li>1 event being processed by the consumer
 *   <li>1 event enqueued in the blocking queue
 * </ul>
 */
@Internal
public class FanOutShardSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(FanOutShardSubscriber.class);



    /**
     * Read timeout will occur after 30 seconds, a sanity timeout to prevent lockup in unexpected
     * error states. If the consumer does not receive a new event within the QUEUE_TIMEOUT_SECONDS
     * it will backoff and resubscribe.
     */
    private static final Duration DEFAULT_QUEUE_TIMEOUT = Duration.ofSeconds(35);

    private final BlockingQueue<FanOutSubscriptionEvent> queue;
    private final Supplier<Boolean> runningSupplier;

    private final StreamProxy kinesis;

    private final String consumerArn;

    private final String shardId;
    private FanOutShardSubscription fanOutShardSubscription;

    /**
     * Create a new Fan Out Shard Subscriber.
     *
     * @param consumerArn the stream consumer ARN
     * @param shardId the shard ID to subscribe to
     * @param kinesis the Kinesis Proxy used to communicate via AWS SDK v2
     */
    @VisibleForTesting
    FanOutShardSubscriber(
            final String consumerArn,
            final String shardId,
            final StreamProxy kinesis,
            BlockingQueue<FanOutSubscriptionEvent> queue,
            Supplier<Boolean> runningSupplier) {
        this.kinesis = Preconditions.checkNotNull(kinesis);
        this.consumerArn = Preconditions.checkNotNull(consumerArn);
        this.shardId = Preconditions.checkNotNull(shardId);
        this.queue = Preconditions.checkNotNull(queue);
        this.runningSupplier = Preconditions.checkNotNull(runningSupplier);
    }

    void openSubscriptionToShard(final StartingPosition startingPosition) {
        CountDownLatch waitForSubscriptionLatch = new CountDownLatch(1);
        FanOutShardSubscription fanOutShardSubscription = new FanOutShardSubscription(waitForSubscriptionLatch, queue);

        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler.builder()
            .subscriber(() -> fanOutShardSubscription)
            .build();
        try {
            kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                    .exceptionally(e -> {
                        LOG.error("LOOK AT ME - Error in future!", e);
                        queue.add(new SubscribeToShardErrorEvent(e, shardId));
                        fanOutShardSubscription.cancelSubscription();
                        throw new FlinkRuntimeException("LOOK AT ME SOME RUNTIME EXCEPTION", e);
                    })
                .get(10000L, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            final String errorMessage =
                "Timed out acquiring subscription - " + shardId + " (" + consumerArn + ")";
            LOG.error(errorMessage);
            fanOutShardSubscription.cancelSubscription();
            queue.add(new SubscribeToShardErrorEvent(new TimeoutException(errorMessage), shardId));
        } catch (ExecutionException e) {
            throw new FlinkRuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        // Request the first record to kick off consumption
        // Following requests are made by the FanOutShardSubscription on the netty thread
        fanOutShardSubscription.requestRecord();
        this.fanOutShardSubscription = fanOutShardSubscription;
    }

    void requestRecords() {
        this.fanOutShardSubscription.requestRecord();
    }

    /**
     * The {@link FanOutShardSubscription} subscribes to the events coming from KDS and adds them to
     * the {@link BlockingQueue}. Backpressure is applied based on the maximum capacity of the
     * queue. The {@link Subscriber} methods of this class are invoked by a thread from the {@link
     * KinesisAsyncClient}.
     */
    private class FanOutShardSubscription implements Subscriber<SubscribeToShardEventStream> {

        private Subscription subscription;

        private volatile boolean cancelled = false;

        private final CountDownLatch waitForSubscriptionLatch;
        private final BlockingQueue<FanOutSubscriptionEvent> eventQueue;

        private FanOutShardSubscription(final CountDownLatch waitForSubscriptionLatch, final BlockingQueue<FanOutSubscriptionEvent> eventQueue) {
            this.waitForSubscriptionLatch = waitForSubscriptionLatch;
            this.eventQueue = eventQueue;
        }

        private boolean isCancelled() {
            return cancelled || !runningSupplier.get();
        }

        /** Flag to the producer that we are ready to receive more events. */
        void requestRecord() {
            if (!isCancelled()) {
                LOG.debug(
                        "Requesting more records from EFO subscription - {} ({})",
                        shardId,
                        consumerArn);
                subscription.request(1);
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            waitForSubscriptionLatch.countDown();
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(
                    new SubscribeToShardResponseHandler.Visitor() {
                        @Override
                        public void visit(SubscribeToShardEvent event) {
                            enqueueEvent(new SubscriptionNextEvent(event, shardId));
                        }
                    });
        }

        @Override
        public void onError(Throwable throwable) {
            LOG.debug(
                    "Error occurred on EFO subscription: {} - ({}).  {} ({})",
                    throwable.getClass().getName(),
                    throwable.getMessage(),
                    shardId,
                    consumerArn,
                    throwable);

            waitForSubscriptionLatch.countDown();
            SubscriptionErrorEvent subscriptionErrorEvent = new SubscriptionErrorEvent(throwable, shardId);

            // Cancel the subscription to signal the onNext to stop requesting data
            cancelSubscription();
            enqueueEvent(subscriptionErrorEvent);
        }

        @Override
        public void onComplete() {
            LOG.debug("EFO subscription complete - {} ({})", shardId, consumerArn);
            enqueueEvent(new SubscriptionCompleteEvent(shardId));
        }

        private void cancelSubscription() {
            if (isCancelled()) {
                return;
            }
            cancelled = true;

            if (subscription != null) {
                subscription.cancel();
            }
        }

        /**
         * Adds the event to the queue blocking until complete.
         *
         * @param event the event to enqueue
         */
        private void enqueueEvent(final FanOutSubscriptionEvent event) {
            if (isCancelled()) {
                return;
            }
            try {
                if (!eventQueue.offer(event, DEFAULT_QUEUE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    final String errorMessage =
                        "Timed out enqueuing event "
                            + event.getClass().getSimpleName()
                            + " - "
                            + shardId
                            + " ("
                            + consumerArn
                            + ")";
                    LOG.error(errorMessage);
                    onError(new RecoverableFanOutSubscriberException(new TimeoutException(errorMessage)));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /** An exception wrapper to indicate an error has been thrown from the networking stack. */
    abstract static class FanOutSubscriberException extends Exception {

        private static final long serialVersionUID = -3899472233945299730L;

        public FanOutSubscriberException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception wrapper to indicate a retryable error has been thrown from the networking stack.
     * Retryable errors are subject to the Subscribe to Shard retry policy. If the configured number
     * of retries are exceeded the application will terminate.
     */
    static class RetryableFanOutSubscriberException extends FanOutSubscriberException {

        private static final long serialVersionUID = -2967281117554404883L;

        public RetryableFanOutSubscriberException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception wrapper to indicate a recoverable error has been thrown from the networking
     * stack. Recoverable errors are not counted in the retry policy.
     */
    static class RecoverableFanOutSubscriberException extends FanOutSubscriberException {

        private static final long serialVersionUID = -3223347557038294482L;

        public RecoverableFanOutSubscriberException(Throwable cause) {
            super(cause);
        }
    }

    /** An exception wrapper to indicate the subscriber has been interrupted. */
    static class FanOutSubscriberInterruptedException extends FanOutSubscriberException {

        private static final long serialVersionUID = -2783477408630427189L;

        public FanOutSubscriberInterruptedException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An interface used to pass messages between {@link FanOutShardSubscription} and {@link
     * FanOutShardSubscriber} via the {@link BlockingQueue}.
     */
    interface FanOutSubscriptionEvent {

        String getShardId();

        default boolean isSubscribeToShardEvent() {
            return false;
        }

        default boolean isSubscriptionComplete() {
            return false;
        }

        default SubscribeToShardEvent getSubscribeToShardEvent() {
            throw new UnsupportedOperationException(
                    "This event does not support getSubscribeToShardEvent()");
        }

        default Throwable getThrowable() {
            throw new UnsupportedOperationException("This event does not support getThrowable()");
        }
    }

    /** Indicates that an EFO subscription has completed/expired. */
    private static class SubscriptionCompleteEvent implements FanOutSubscriptionEvent {

        private final String shardId;

        private SubscriptionCompleteEvent(String shardId) {
            this.shardId = shardId;
        }

        @Override
        public String getShardId() {
            return shardId;
        }

        @Override
        public boolean isSubscriptionComplete() {
            return true;
        }
    }

    /** Poison pill, indicates that an error occurred while consuming from KDS. */
    private static class SubscriptionErrorEvent implements FanOutSubscriptionEvent {
        private final Throwable throwable;
        private final String shardId;

        private SubscriptionErrorEvent(Throwable throwable, String shardId) {
            this.throwable = throwable;
            this.shardId = shardId;
        }

        @Override
        public String getShardId() {
            return shardId;
        }

        @Override
        public Throwable getThrowable() {
            return throwable;
        }
    }

    /** Indicates that an error occurred while subscribing to KDS shard. */
    private static class SubscribeToShardErrorEvent implements FanOutSubscriptionEvent {
        private final Throwable throwable;
        private final String shardId;

        private SubscribeToShardErrorEvent(Throwable throwable, String shardId) {
            this.throwable = throwable;
            this.shardId = shardId;
        }

        @Override
        public String getShardId() {
            return shardId;
        }

        @Override
        public Throwable getThrowable() {
            return throwable;
        }
    }

    /** A wrapper to pass the next {@link SubscribeToShardEvent} between threads. */
    private static class SubscriptionNextEvent implements FanOutSubscriptionEvent {
        private final SubscribeToShardEvent subscribeToShardEvent;
        private final String shardId;

        private SubscriptionNextEvent(SubscribeToShardEvent subscribeToShardEvent, String shardId) {
            this.subscribeToShardEvent = subscribeToShardEvent;
            this.shardId = shardId;
        }

        @Override
        public String getShardId() {
            return shardId;
        }

        @Override
        public boolean isSubscribeToShardEvent() {
            return true;
        }

        @Override
        public SubscribeToShardEvent getSubscribeToShardEvent() {
            return subscribeToShardEvent;
        }
    }
}
