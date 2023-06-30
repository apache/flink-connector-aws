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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisAsyncClient;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_RETRIES;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for methods in the {@link KinesisProxySyncV2} class. */
public class KinesisProxyAsyncV2Test {

    private static final long EXPECTED_SUBSCRIBE_TO_SHARD_MAX = 1;
    private static final long EXPECTED_SUBSCRIBE_TO_SHARD_BASE = 2;
    private static final double EXPECTED_SUBSCRIBE_TO_SHARD_POW = 0.1;

    private static final long EXPECTED_REGISTRATION_MAX = 2;
    private static final long EXPECTED_REGISTRATION_BASE = 3;
    private static final double EXPECTED_REGISTRATION_POW = 0.2;

    private static final long EXPECTED_DEREGISTRATION_MAX = 3;
    private static final long EXPECTED_DEREGISTRATION_BASE = 4;
    private static final double EXPECTED_DEREGISTRATION_POW = 0.3;

    private static final long EXPECTED_DESCRIBE_CONSUMER_MAX = 4;
    private static final long EXPECTED_DESCRIBE_CONSUMER_BASE = 5;
    private static final double EXPECTED_DESCRIBE_CONSUMER_POW = 0.4;

    private static final long EXPECTED_DESCRIBE_STREAM_MAX = 5;
    private static final long EXPECTED_DESCRIBE_STREAM_BASE = 6;
    private static final double EXPECTED_DESCRIBE_STREAM_POW = 0.5;
    private static final int EXPECTED_DESCRIBE_STREAM_RETRIES = 10;

    @Rule public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testSubscribeToShard() {
        SubscribeToShardRequest request = SubscribeToShardRequest.builder().build();
        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder().subscriber(event -> {}).build();

        TestableKinesisProxyAsyncV2 proxy =
                TestableKinesisProxyAsyncV2.createTestableKinesisProxyAsyncV2();

        proxy.subscribeToShard(request, responseHandler);

        assertThat(((FakeKinesisAsyncClient) proxy.kinesisAsyncClient).subscribeToShardRequest)
                .isEqualTo(request);
        assertThat(((FakeKinesisAsyncClient) proxy.kinesisAsyncClient).asyncResponseHandler)
                .isEqualTo(responseHandler);
    }

    @Test
    public void testCloseInvokesClientClose() {
        TestableKinesisProxyAsyncV2 proxy =
                TestableKinesisProxyAsyncV2.createTestableKinesisProxyAsyncV2();

        proxy.close();

        assertThat(proxy.isClosed).isTrue();
        assertThat(((FakeKinesisAsyncClient) proxy.kinesisAsyncClient).isClosed).isTrue();
        assertThat(((FakeSdkAsyncHttpClient) proxy.sdkAsyncHttpClient).isClosed).isTrue();
    }

    private static FanOutRecordPublisherConfiguration createConfiguration() {
        return new FanOutRecordPublisherConfiguration(createEfoProperties(), emptyList());
    }

    private static Properties createEfoProperties() {
        Properties config = new Properties();
        config.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
        config.setProperty(EFO_CONSUMER_NAME, "dummy-efo-consumer");
        config.setProperty(
                SUBSCRIBE_TO_SHARD_BACKOFF_BASE, String.valueOf(EXPECTED_SUBSCRIBE_TO_SHARD_BASE));
        config.setProperty(
                SUBSCRIBE_TO_SHARD_BACKOFF_MAX, String.valueOf(EXPECTED_SUBSCRIBE_TO_SHARD_MAX));
        config.setProperty(
                SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_SUBSCRIBE_TO_SHARD_POW));
        config.setProperty(
                REGISTER_STREAM_BACKOFF_BASE, String.valueOf(EXPECTED_REGISTRATION_BASE));
        config.setProperty(REGISTER_STREAM_BACKOFF_MAX, String.valueOf(EXPECTED_REGISTRATION_MAX));
        config.setProperty(
                REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_REGISTRATION_POW));
        config.setProperty(
                DEREGISTER_STREAM_BACKOFF_BASE, String.valueOf(EXPECTED_DEREGISTRATION_BASE));
        config.setProperty(
                DEREGISTER_STREAM_BACKOFF_MAX, String.valueOf(EXPECTED_DEREGISTRATION_MAX));
        config.setProperty(
                DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_DEREGISTRATION_POW));
        config.setProperty(
                DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE,
                String.valueOf(EXPECTED_DESCRIBE_CONSUMER_BASE));
        config.setProperty(
                DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX,
                String.valueOf(EXPECTED_DESCRIBE_CONSUMER_MAX));
        config.setProperty(
                DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_DESCRIBE_CONSUMER_POW));
        config.setProperty(
                STREAM_DESCRIBE_BACKOFF_BASE, String.valueOf(EXPECTED_DESCRIBE_STREAM_BASE));
        config.setProperty(
                STREAM_DESCRIBE_BACKOFF_MAX, String.valueOf(EXPECTED_DESCRIBE_STREAM_MAX));
        config.setProperty(
                STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_DESCRIBE_STREAM_POW));
        config.setProperty(
                STREAM_DESCRIBE_RETRIES, String.valueOf(EXPECTED_DESCRIBE_STREAM_RETRIES));
        return config;
    }

    private static class TestableKinesisProxyAsyncV2 extends KinesisProxyAsyncV2 {
        public KinesisAsyncClient kinesisAsyncClient;
        public SdkAsyncHttpClient sdkAsyncHttpClient;
        public boolean isClosed;

        /**
         * Create a new KinesisProxyV2.
         *
         * @param kinesisAsyncClient AWS SDK v2 Kinesis client used to communicate with AWS services
         * @param asyncHttpClient the underlying HTTP client, reference required for close only
         * @param fanOutRecordPublisherConfiguration the configuration for Fan Out features
         */
        public TestableKinesisProxyAsyncV2(
                KinesisAsyncClient kinesisAsyncClient,
                SdkAsyncHttpClient asyncHttpClient,
                FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration) {
            super(kinesisAsyncClient, asyncHttpClient, fanOutRecordPublisherConfiguration);
            this.kinesisAsyncClient = kinesisAsyncClient;
            this.sdkAsyncHttpClient = asyncHttpClient;
        }

        @Override
        public void close() {
            isClosed = true;
            super.close();
        }

        public static TestableKinesisProxyAsyncV2 createTestableKinesisProxyAsyncV2() {
            return new TestableKinesisProxyAsyncV2(
                    new FakeKinesisAsyncClient(),
                    new FakeSdkAsyncHttpClient(),
                    createConfiguration());
        }
    }

    private static class FakeSdkAsyncHttpClient implements SdkAsyncHttpClient {
        public boolean isClosed;

        @Override
        public CompletableFuture<Void> execute(AsyncExecuteRequest asyncExecuteRequest) {
            return null;
        }

        @Override
        public void close() {
            isClosed = true;
        }
    }
}
