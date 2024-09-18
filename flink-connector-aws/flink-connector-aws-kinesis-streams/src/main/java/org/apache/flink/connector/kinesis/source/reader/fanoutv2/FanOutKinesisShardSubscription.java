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

package org.apache.flink.connector.kinesis.source.reader.fanoutv2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.time.Duration;

/**
 * FanOutSubscription class responsible for handling the subscription to a single shard of the
 * Kinesis stream. Given a shardId, it will manage the lifecycle of the subscription, and eagerly
 * keep the next batch of records available for consumption when next polled.
 */
@Internal
class FanOutKinesisShardSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSubscription.class);

    private final AsyncStreamProxy kinesis;
    private final String consumerArn;
    private final String shardId;

    private final Duration subscriptionTimeout = Duration.ofSeconds(60);

    private FanOutShardSubscriber subscriber;

    // Store the current starting position for this subscription. Will be updated each time new
    // batch of records is consumed
    private StartingPosition startingPosition;
    private Throwable subscriptionFailure;

    FanOutKinesisShardSubscription(
            AsyncStreamProxy kinesis,
            String consumerArn,
            String shardId,
            StartingPosition startingPosition) {
        this.kinesis = kinesis;
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
    }

    /** Method to allow eager activation of the subscription. */
    public void activateSubscription() {
        LOG.info(
                "Activating subscription to shard {} with starting position {} for consumer {}.",
                shardId,
                startingPosition,
                consumerArn);

        subscriber = new FanOutShardSubscriber(consumerArn, shardId, startingPosition);

        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder()
                        .subscriber(() -> subscriber)
                        .onError(
                                throwable -> {
                                    // After subscription is acquired, these errors can be ignored.
                                    if (!subscriber.isSubscribed()) {
                                        terminateSubscription(throwable);
                                    }
                                })
                        .build();

        // We don't need to keep track of the future here because we monitor subscription success
        // using our own CountDownLatch
        kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                .exceptionally(
                        throwable -> {
                            LOG.error(
                                    "Error subscribing to shard {} with starting position {} for consumer {}.",
                                    shardId,
                                    startingPosition,
                                    consumerArn,
                                    throwable);
                            terminateSubscription(throwable);
                            return null;
                        });
    }

    private void terminateSubscription(Throwable t) {
        this.subscriptionFailure = t;
        subscriber.cancel();
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
        // TODO handle race conditions between nextEvent and


        if (subscriber == null) {
            activateSubscription();
            return null;
        }

        if (subscriptionFailure != null) {
            LOG.error("Subscription encountered unrecoverable exception.", subscriptionFailure);
            throw new KinesisStreamsSourceException(
                    "Subscription encountered unrecoverable exception.", subscriptionFailure);
        }

        if (subscriber.isFailed()) {
            subscriber.getFailure().map((failure) -> {
                LOG.error("Subscription encountered unrecoverable exception.", failure);
                throw new KinesisStreamsSourceException(
                        "Subscription encountered unrecoverable exception.", failure);
            }).orElseThrow(() -> new KinesisStreamsSourceException(
                    "Subscription encountered unexpected failure.", null));
        }

        if (!subscriber.isSubscribed()) {
            LOG.debug(
                    "Subscription to shard {} for consumer {} is not yet active. Skipping.",
                    shardId,
                    consumerArn);
            // TODO handle timeouts ... somewhere

            return null;
        }

        SubscribeToShardEvent event = subscriber.poll();
        if (event != null) {
            startingPosition =
                    StartingPosition.continueFromSequenceNumber(event.continuationSequenceNumber());
            return event;
        }

        if (subscriber.isTerminated()) {
            LOG.debug(
                    "Subscription to shard {} for consumer {} is terminated. Skipping.",
                    shardId,
                    consumerArn);
            activateSubscription();
        }

        return null;
    }

}
