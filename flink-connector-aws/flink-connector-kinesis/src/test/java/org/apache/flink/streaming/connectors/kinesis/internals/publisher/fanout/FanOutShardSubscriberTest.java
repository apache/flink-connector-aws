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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout;

import org.apache.flink.streaming.connectors.kinesis.config.ExceptionConfig;
import org.apache.flink.streaming.connectors.kinesis.config.RecoverableErrorsConfig;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyAsyncV2Interface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.SubscriptionErrorKinesisAsyncV2;

import com.amazonaws.http.timers.client.SdkInterruptedException;
import io.netty.handler.timeout.ReadTimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.INCOMPLETE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FanOutShardSubscriber}. */
public class FanOutShardSubscriberTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRecoverableErrorThrownToConsumer() throws Exception {
        thrown.expect(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class);
        thrown.expectMessage("io.netty.handler.timeout.ReadTimeoutException");

        SubscriptionErrorKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                        ReadTimeoutException.INSTANCE);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        null);

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testRecoverableErrorThrownToConsumerWhenUserConfiguresExceptionToBeRecoverable()
            throws Exception {
        thrown.expect(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class);
        thrown.expectMessage("java.lang.ArithmeticException");

        SubscriptionErrorKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                        new ArithmeticException());

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        new RecoverableErrorsConfig(
                                Collections.singletonList(
                                        new ExceptionConfig(ArithmeticException.class))));

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testRecoverableErrorThrownToConsumerWhenUserConfiguredExceptionIsWrapped()
            throws Exception {
        thrown.expect(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class);
        thrown.expectMessage("java.lang.ArithmeticException");

        SubscriptionErrorKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                        new RuntimeException(new ArithmeticException()));

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        new RecoverableErrorsConfig(
                                Collections.singletonList(
                                        new ExceptionConfig(ArithmeticException.class))));

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testRetryableErrorThrownToConsumerWhenUserConfiguredExceptionIsNotThrown()
            throws Exception {
        thrown.expect(FanOutShardSubscriber.RetryableFanOutSubscriberException.class);
        thrown.expectMessage("java.lang.ArithmeticException");

        SubscriptionErrorKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                        new RuntimeException(new ArithmeticException()));

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        new RecoverableErrorsConfig(
                                Collections.singletonList(
                                        new ExceptionConfig(java.net.UnknownHostException.class))));

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testRetryableErrorThrownToConsumer() throws Exception {
        thrown.expect(FanOutShardSubscriber.RetryableFanOutSubscriberException.class);
        thrown.expectMessage("Error!");

        RuntimeException error = new RuntimeException("Error!");
        SubscriptionErrorKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(error);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        null);

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testInterruptedErrorThrownToConsumer() throws Exception {
        thrown.expect(FanOutShardSubscriber.FanOutSubscriberInterruptedException.class);

        SdkInterruptedException error = new SdkInterruptedException(null);
        SubscriptionErrorKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(error);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        null);

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testMultipleErrorsThrownPassesFirstErrorToConsumer() throws Exception {
        thrown.expect(FanOutShardSubscriber.FanOutSubscriberException.class);
        thrown.expectMessage("Error 1!");

        RuntimeException error1 = new RuntimeException("Error 1!");
        RuntimeException error2 = new RuntimeException("Error 2!");
        SubscriptionErrorKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(error1, error2);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        null);

        StartingPosition startingPosition = StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testSubscriptionCompletion() throws Exception {
        FakeKinesisFanOutBehavioursFactory.AbstractSingleShardFanOutKinesisAsyncV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.emptyBatchFollowedBySingleRecord();

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        errorKinesisV2,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        () -> true,
                        null);

        StartingPosition startingPosition = StartingPosition.builder().build();
        RecordPublisherRunResult result =
                subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});

        assertThat(result).isEqualTo(INCOMPLETE);
    }

    @Test
    public void testTimeoutSubscribingToShard() throws Exception {
        thrown.expect(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class);
        thrown.expectMessage("Timed out acquiring subscription");

        KinesisProxyAsyncV2Interface kinesis =
                FakeKinesisFanOutBehavioursFactory.failsToAcquireSubscription();

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn", "shardId", kinesis, Duration.ofMillis(1), () -> true, null);

        StartingPosition startingPosition = StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testTimeoutEnqueuingEvent() throws Exception {
        thrown.expect(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class);
        thrown.expectMessage("Timed out enqueuing event SubscriptionNextEvent");

        KinesisProxyAsyncV2Interface kinesis =
                FakeKinesisFanOutBehavioursFactory.shardThatCreatesBackpressureOnQueue();

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        kinesis,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        Duration.ofMillis(100),
                        () -> true,
                        null);

        StartingPosition startingPosition = StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(
                startingPosition,
                event -> {
                    try {
                        Thread.sleep(120);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void testCancelExitsGracefully() throws Exception {
        FakeKinesisFanOutBehavioursFactory.AbstractSingleShardFanOutKinesisAsyncV2 unboundedStream =
                FakeKinesisFanOutBehavioursFactory.boundedShard().withBatchCount(128).build();

        AtomicBoolean run = new AtomicBoolean(true);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber(
                        "consumerArn",
                        "shardId",
                        unboundedStream,
                        DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                        run::get,
                        null);

        final AtomicInteger batches = new AtomicInteger(0);

        subscriber.subscribeToShardAndConsumeRecords(
                StartingPosition.builder().build(),
                event -> {
                    batches.incrementAndGet();

                    if (batches.get() == 8) {
                        // Set running to false, this will cancel record consumption
                        run.set(false);
                    }
                });

        // Since we are setting run=false in the above callback
        // we expect 8/128 batches to be received
        assertThat(batches.get()).isEqualTo(8);
    }
}
