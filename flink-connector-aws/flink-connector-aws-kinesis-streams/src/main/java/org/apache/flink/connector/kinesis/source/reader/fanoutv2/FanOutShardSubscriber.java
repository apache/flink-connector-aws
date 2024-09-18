package org.apache.flink.connector.kinesis.source.reader.fanoutv2;

import org.apache.flink.connector.kinesis.source.reader.fanout.FanOutKinesisShardSubscription;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

/**
 * Implementation of {@link Subscriber} to retrieve events from Kinesis stream using Reactive
 * Streams.
 */
class FanOutShardSubscriber implements Subscriber<SubscribeToShardEventStream> {

    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSubscription.class);
    private static final int BUFFER_SIZE = 2;

    // these are here just for debugging purposes
    private final String consumerArn;
    private final String shardId;
    private final StartingPosition startingPosition;

    // our buffer
    private final Queue<SubscribeToShardEvent> eventQueue = new ArrayDeque<>(BUFFER_SIZE);

    // subscription to shard
    private Subscription subscription;

    // maybe we can use an enum for this?
    private boolean subscribed;
    private boolean terminated;
    private boolean failed;
    private Throwable subscriptionException;

    FanOutShardSubscriber(String consumerArn, String shardId, StartingPosition startingPosition) {
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
    }

    boolean isSubscribed() {
        return subscribed;
    }

    boolean isTerminated() {
        return terminated;
    }

    boolean isFailed() {
        return failed;
    }

    Optional<Throwable> getFailure() {
        return Optional.ofNullable(subscriptionException);
    }

    @Override
    public void onSubscribe(Subscription acquiredSubscription) {
        LOG.info(
                "Successfully subscribed to shard {} at {} using consumer {}.",
                shardId,
                startingPosition,
                consumerArn);
        subscription = acquiredSubscription;
        subscribed = true;
        subscription.request(BUFFER_SIZE);
    }

    public void cancel() {
        if (terminated) {
            LOG.warn("Trying to cancel inactive subscription. Ignoring.");
            return;
        }
        terminated = true;
        subscription.cancel();
    }

    @Override
    public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
        subscribeToShardEventStream.accept(
                new SubscribeToShardResponseHandler.Visitor() {
                    @Override
                    public void visit(SubscribeToShardEvent event) {
                        LOG.debug("Received event: {}, {}", event.getClass().getSimpleName(), event);
                        eventQueue.add(event);
                    }
                });
    }

    @Override
    public void onError(Throwable throwable) {
        failed = true;
        terminated = true;
        subscriptionException = throwable;
    }

    @Override
    public void onComplete() {
        terminated = true;
    }

    public SubscribeToShardEvent poll() {
        SubscribeToShardEvent event = eventQueue.poll();
        if (event != null && !terminated) {
            subscription.request(1);
        }
        return event;
    }
    
}