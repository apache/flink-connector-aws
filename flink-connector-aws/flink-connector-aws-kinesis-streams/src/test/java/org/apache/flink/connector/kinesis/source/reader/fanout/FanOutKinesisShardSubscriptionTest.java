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
import org.apache.flink.connector.kinesis.source.util.FakeKinesisFanOutBehaviorsFactory;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FanOutKinesisShardSubscription}. */
class FanOutKinesisShardSubscriptionTest {

    private static final String TEST_SHARD_ID = generateShardId(1);
    private static final Duration SUBSCRIPTION_TIMEOUT = Duration.ofSeconds(5);

    @Test
    void testNextEventReturnsNullBeforeActivation() {
        AsyncStreamProxy proxy = FakeKinesisFanOutBehaviorsFactory.boundedShard().build();
        FanOutKinesisShardSubscription subscription =
                new FanOutKinesisShardSubscription(
                        proxy,
                        CONSUMER_ARN,
                        TEST_SHARD_ID,
                        StartingPosition.fromStart(),
                        SUBSCRIPTION_TIMEOUT);

        assertThat(subscription.nextEvent()).isNull();
    }

    @Test
    void testResourceNotFoundExceptionThrown() throws Exception {
        AsyncStreamProxy proxy =
                FakeKinesisFanOutBehaviorsFactory.resourceNotFoundWhenObtainingSubscription();
        FanOutKinesisShardSubscription subscription =
                new FanOutKinesisShardSubscription(
                        proxy,
                        CONSUMER_ARN,
                        TEST_SHARD_ID,
                        StartingPosition.fromStart(),
                        SUBSCRIPTION_TIMEOUT);

        subscription.activateSubscription();

        // Poll until exception surfaces
        assertThatThrownBy(
                        () -> {
                            for (int i = 0; i < 200; i++) {
                                subscription.nextEvent();
                                Thread.sleep(50);
                            }
                        })
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void testUnrecoverableExceptionWrappedInSourceException() throws Exception {
        AsyncStreamProxy proxy =
                new AsyncStreamProxy() {
                    @Override
                    public CompletableFuture<Void> subscribeToShard(
                            String consumerArn,
                            String shardId,
                            StartingPosition startingPosition,
                            SubscribeToShardResponseHandler responseHandler) {
                        responseHandler.exceptionOccurred(
                                new IllegalStateException("unrecoverable"));
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void close() {}
                };
        FanOutKinesisShardSubscription subscription =
                new FanOutKinesisShardSubscription(
                        proxy,
                        CONSUMER_ARN,
                        TEST_SHARD_ID,
                        StartingPosition.fromStart(),
                        SUBSCRIPTION_TIMEOUT);

        subscription.activateSubscription();

        assertThatThrownBy(
                        () -> {
                            for (int i = 0; i < 200; i++) {
                                subscription.nextEvent();
                                Thread.sleep(50);
                            }
                        })
                .isInstanceOf(KinesisStreamsSourceException.class)
                .hasMessageContaining("unrecoverable");
    }

    @Test
    void testSubscriptionTimeoutTerminatesSubscription() throws Exception {
        AsyncStreamProxy proxy =
                new AsyncStreamProxy() {
                    @Override
                    public CompletableFuture<Void> subscribeToShard(
                            String consumerArn,
                            String shardId,
                            StartingPosition startingPosition,
                            SubscribeToShardResponseHandler responseHandler) {
                        return new CompletableFuture<>();
                    }

                    @Override
                    public void close() {}
                };
        FanOutKinesisShardSubscription subscription =
                new FanOutKinesisShardSubscription(
                        proxy,
                        CONSUMER_ARN,
                        TEST_SHARD_ID,
                        StartingPosition.fromStart(),
                        Duration.ofMillis(200));

        subscription.activateSubscription();

        // Wait for timeout to trigger, then poll - should recover
        Thread.sleep(500);
        SubscribeToShardEvent event = subscription.nextEvent();
        assertThat(event).isNull();
    }

    @Test
    void testOnCompleteDoesNotResubscribe() throws Exception {
        AtomicInteger subscribeCount = new AtomicInteger(0);
        AsyncStreamProxy proxy =
                new AsyncStreamProxy() {
                    @Override
                    public CompletableFuture<Void> subscribeToShard(
                            String consumerArn,
                            String shardId,
                            StartingPosition startingPosition,
                            SubscribeToShardResponseHandler responseHandler) {
                        subscribeCount.incrementAndGet();
                        return CompletableFuture.supplyAsync(
                                () -> {
                                    responseHandler.responseReceived(
                                            SubscribeToShardResponse.builder().build());
                                    responseHandler.onEventStream(
                                            subscriber -> {
                                                subscriber.onSubscribe(
                                                        new Subscription() {
                                                            @Override
                                                            public void request(long n) {
                                                                subscriber.onComplete();
                                                            }

                                                            @Override
                                                            public void cancel() {}
                                                        });
                                            });
                                    return null;
                                });
                    }

                    @Override
                    public void close() {}
                };

        FanOutKinesisShardSubscription subscription =
                new FanOutKinesisShardSubscription(
                        proxy,
                        CONSUMER_ARN,
                        TEST_SHARD_ID,
                        StartingPosition.fromStart(),
                        SUBSCRIPTION_TIMEOUT);

        subscription.activateSubscription();

        // Wait for the async subscription to activate and complete
        Thread.sleep(500);

        // Verify subscribeToShard was called exactly once - onComplete must not re-subscribe
        assertThat(subscribeCount.get()).isEqualTo(1);

        // Subscription should be inactive after onComplete
        assertThat(subscription.nextEvent()).isNull();
    }
}
