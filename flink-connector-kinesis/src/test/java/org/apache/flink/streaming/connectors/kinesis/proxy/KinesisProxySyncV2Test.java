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
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisSyncClient;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.util.Properties;

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
public class KinesisProxySyncV2Test {

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
    public void testCloseInvokesClientClose() {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2.createTestableKinesisProxyAsyncV2();

        proxy.close();

        assertThat(proxy.isClosed).isTrue();
        assertThat(((FakeKinesisSyncClient) proxy.kinesisClient).isClosed).isTrue();
        assertThat(((KinesisProxySyncV2Test.FakeSdkSyncHttpClient) proxy.sdkHttpClient).isClosed)
                .isTrue();
    }

    @Test
    public void testRegisterStreamConsumer() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2.createTestableKinesisProxyAsyncV2();

        RegisterStreamConsumerResponse expected = RegisterStreamConsumerResponse.builder().build();

        ((FakeKinesisSyncClient) proxy.kinesisClient)
                .setFakeRegisterStreamConsumerResponse(expected);

        RegisterStreamConsumerResponse actual = proxy.registerStreamConsumer("arn", "name");

        assertThat(actual).isEqualTo(expected);

        assertThat(
                        ((FakeKinesisSyncClient) proxy.kinesisClient)
                                .registerStreamConsumerRequest.streamARN())
                .isEqualTo("arn");
        assertThat(
                        ((FakeKinesisSyncClient) proxy.kinesisClient)
                                .registerStreamConsumerRequest.consumerName())
                .isEqualTo("name");
    }

    @Test
    public void testRegisterStreamConsumerBackoffJitter() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2
                        .createTestableKinesisProxyAsyncV2FailRegisterStreamConsumer();

        proxy.registerStreamConsumer("arn", "name");

        FakeFullJitterBackoff actualBackoff = (FakeFullJitterBackoff) proxy.fullJitterBackoff;

        assertThat(actualBackoff.backoffDuration).isNotNull();
        assertThat(actualBackoff.baseMillis).isEqualTo(EXPECTED_REGISTRATION_BASE);
        assertThat(actualBackoff.maxMillis).isEqualTo(EXPECTED_REGISTRATION_MAX);
        assertThat(actualBackoff.power).isEqualTo(EXPECTED_REGISTRATION_POW);
        assertThat(actualBackoff.attempt).isEqualTo(1);
    }

    @Test
    public void testDeregisterStreamConsumer() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2.createTestableKinesisProxyAsyncV2();

        DeregisterStreamConsumerResponse expected =
                DeregisterStreamConsumerResponse.builder().build();

        ((FakeKinesisSyncClient) proxy.kinesisClient)
                .setFakeDeregisterStreamConsumerResponse(expected);

        DeregisterStreamConsumerResponse actual = proxy.deregisterStreamConsumer("arn");

        assertThat(actual).isEqualTo(expected);

        assertThat(
                        ((FakeKinesisSyncClient) proxy.kinesisClient)
                                .deregisterStreamConsumerRequest.consumerARN())
                .isEqualTo("arn");
    }

    @Test
    public void testDeregisterStreamConsumerBackoffJitter() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2
                        .createTestableKinesisProxyAsyncV2FailDeregisterStreamConsumer();

        proxy.deregisterStreamConsumer("arn");

        FakeFullJitterBackoff actualBackoff = (FakeFullJitterBackoff) proxy.fullJitterBackoff;

        assertThat(actualBackoff.backoffDuration).isNotNull();
        assertThat(actualBackoff.baseMillis).isEqualTo(EXPECTED_DEREGISTRATION_BASE);
        assertThat(actualBackoff.maxMillis).isEqualTo(EXPECTED_DEREGISTRATION_MAX);
        assertThat(actualBackoff.power).isEqualTo(EXPECTED_DEREGISTRATION_POW);
        assertThat(actualBackoff.attempt).isEqualTo(1);
    }

    @Test
    public void testDescribeStreamConsumerWithStreamConsumerArn() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2.createTestableKinesisProxyAsyncV2();

        DescribeStreamConsumerResponse expected = DescribeStreamConsumerResponse.builder().build();

        ((FakeKinesisSyncClient) proxy.kinesisClient)
                .setFakeDescribeStreamConsumerResponse(expected);

        DescribeStreamConsumerResponse actual = proxy.describeStreamConsumer("arn");

        assertThat(actual).isEqualTo(expected);

        assertThat(
                        ((FakeKinesisSyncClient) proxy.kinesisClient)
                                .describeStreamConsumerRequest.consumerARN())
                .isEqualTo("arn");
    }

    @Test
    public void testDescribeStreamConsumerWithStreamArnAndConsumerName() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2.createTestableKinesisProxyAsyncV2();

        DescribeStreamConsumerResponse expected = DescribeStreamConsumerResponse.builder().build();
        ((FakeKinesisSyncClient) proxy.kinesisClient)
                .setFakeDescribeStreamConsumerResponse(expected);

        DescribeStreamConsumerResponse actual = proxy.describeStreamConsumer("arn", "name");

        assertThat(actual).isEqualTo(expected);

        assertThat(
                        ((FakeKinesisSyncClient) proxy.kinesisClient)
                                .describeStreamConsumerRequest.streamARN())
                .isEqualTo("arn");
        assertThat(
                        ((FakeKinesisSyncClient) proxy.kinesisClient)
                                .describeStreamConsumerRequest.consumerName())
                .isEqualTo("name");
    }

    @Test
    public void testDescribeStreamConsumerBackoffJitter() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2
                        .createTestableKinesisProxyAsyncV2FailDescribeStreamConsumer();

        proxy.describeStreamConsumer("arn");

        FakeFullJitterBackoff actualBackoff = (FakeFullJitterBackoff) proxy.fullJitterBackoff;

        assertThat(actualBackoff.backoffDuration).isNotNull();
        assertThat(actualBackoff.baseMillis).isEqualTo(EXPECTED_DESCRIBE_CONSUMER_BASE);
        assertThat(actualBackoff.maxMillis).isEqualTo(EXPECTED_DESCRIBE_CONSUMER_MAX);
        assertThat(actualBackoff.power).isEqualTo(EXPECTED_DESCRIBE_CONSUMER_POW);
        assertThat(actualBackoff.attempt).isEqualTo(1);
    }

    @Test
    public void testDescribeStreamSummary() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2.createTestableKinesisProxyAsyncV2();

        DescribeStreamSummaryResponse expected = DescribeStreamSummaryResponse.builder().build();
        ((FakeKinesisSyncClient) proxy.kinesisClient)
                .setFakeDescribeStreamSummaryResponse(expected);
        DescribeStreamSummaryResponse actual = proxy.describeStreamSummary("stream");

        assertThat(actual).isEqualTo(expected);

        assertThat(
                        ((FakeKinesisSyncClient) proxy.kinesisClient)
                                .describeStreamSummaryRequest.streamName())
                .isEqualTo("stream");
    }

    @Test
    public void testDescribeStreamSummaryBackoffJitter() throws Exception {
        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2
                        .createTestableKinesisProxyAsyncV2FailDescribeStreamSummary();

        proxy.describeStreamSummary("arn");

        FakeFullJitterBackoff actualBackoff = (FakeFullJitterBackoff) proxy.fullJitterBackoff;

        assertThat(actualBackoff.backoffDuration).isNotNull();
        assertThat(actualBackoff.baseMillis).isEqualTo(EXPECTED_DESCRIBE_STREAM_BASE);
        assertThat(actualBackoff.maxMillis).isEqualTo(EXPECTED_DESCRIBE_STREAM_MAX);
        assertThat(actualBackoff.power).isEqualTo(EXPECTED_DESCRIBE_STREAM_POW);
        assertThat(actualBackoff.attempt).isEqualTo(1);
    }

    @Test
    public void testDescribeStreamSummaryFailsAfterMaxRetries() throws Exception {
        exception.expect(RuntimeException.class);
        exception.expectMessage("Retries exceeded - all 10 retry attempts failed.");

        TestableKinesisProxySyncV2 proxy =
                TestableKinesisProxySyncV2
                        .createTestableKinesisProxyAsyncV2ConstantFailDescribeStreamSummary();

        proxy.describeStreamSummary("arn");
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

    private static class TestableKinesisProxySyncV2 extends KinesisProxySyncV2 {
        public KinesisClient kinesisClient;
        public SdkHttpClient sdkHttpClient;
        public FullJitterBackoff fullJitterBackoff;
        public boolean isClosed;

        /**
         * Create a new KinesisProxyV2.
         *
         * @param kinesisClient AWS SDK v2 Kinesis client used to communicate with AWS services
         * @param httpClient the underlying HTTP client, reference required for close only
         * @param fanOutRecordPublisherConfiguration the configuration for Fan Out features
         * @param backoff the backoff utility used to introduce Full Jitter delays
         */
        public TestableKinesisProxySyncV2(
                KinesisClient kinesisClient,
                SdkHttpClient httpClient,
                FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration,
                FullJitterBackoff backoff) {
            super(kinesisClient, httpClient, fanOutRecordPublisherConfiguration, backoff);
            this.kinesisClient = kinesisClient;
            this.sdkHttpClient = httpClient;
            this.fullJitterBackoff = backoff;
        }

        @Override
        public void close() {
            isClosed = true;
            super.close();
        }

        public static TestableKinesisProxySyncV2 createTestableKinesisProxyAsyncV2() {
            return new TestableKinesisProxySyncV2(
                    new FakeKinesisSyncClient(),
                    new FakeSdkSyncHttpClient(),
                    createConfiguration(),
                    new FakeFullJitterBackoff());
        }

        public static TestableKinesisProxySyncV2
                createTestableKinesisProxyAsyncV2FailRegisterStreamConsumer() {
            FakeKinesisSyncClient fakeKinesisSyncClient =
                    new FakeKinesisSyncClient() {
                        @Override
                        public RegisterStreamConsumerResponse registerStreamConsumer(
                                RegisterStreamConsumerRequest registerStreamConsumerRequest)
                                throws InvalidArgumentException, LimitExceededException,
                                        ResourceInUseException, ResourceNotFoundException,
                                        AwsServiceException, SdkClientException, KinesisException {
                            if (!isRegisterStreamConsumerCalled) {
                                isRegisterStreamConsumerCalled = true;
                                throw new RuntimeException(
                                        LimitExceededException.builder().build());
                            } else {
                                return RegisterStreamConsumerResponse.builder().build();
                            }
                        }
                    };

            return new TestableKinesisProxySyncV2(
                    fakeKinesisSyncClient,
                    new FakeSdkSyncHttpClient(),
                    createConfiguration(),
                    new FakeFullJitterBackoff());
        }

        public static TestableKinesisProxySyncV2
                createTestableKinesisProxyAsyncV2FailDeregisterStreamConsumer() {
            FakeKinesisSyncClient fakeKinesisSyncClient =
                    new FakeKinesisSyncClient() {
                        @Override
                        public DeregisterStreamConsumerResponse deregisterStreamConsumer(
                                DeregisterStreamConsumerRequest deregisterStreamConsumerRequest)
                                throws InvalidArgumentException, LimitExceededException,
                                        ResourceInUseException, ResourceNotFoundException,
                                        AwsServiceException, SdkClientException, KinesisException {
                            if (!isDeregisterStreamConsumerCalled) {
                                isDeregisterStreamConsumerCalled = true;
                                throw new RuntimeException(
                                        LimitExceededException.builder().build());
                            } else {
                                return DeregisterStreamConsumerResponse.builder().build();
                            }
                        }
                    };

            return new TestableKinesisProxySyncV2(
                    fakeKinesisSyncClient,
                    new FakeSdkSyncHttpClient(),
                    createConfiguration(),
                    new FakeFullJitterBackoff());
        }

        public static TestableKinesisProxySyncV2
                createTestableKinesisProxyAsyncV2FailDescribeStreamConsumer() {
            FakeKinesisSyncClient fakeKinesisSyncClient =
                    new FakeKinesisSyncClient() {
                        @Override
                        public DescribeStreamConsumerResponse describeStreamConsumer(
                                DescribeStreamConsumerRequest describeStreamConsumerRequest)
                                throws InvalidArgumentException, LimitExceededException,
                                        ResourceInUseException, ResourceNotFoundException,
                                        AwsServiceException, SdkClientException, KinesisException {
                            if (!isDescribeStreamConsumerCalled) {
                                isDescribeStreamConsumerCalled = true;
                                throw new RuntimeException(
                                        LimitExceededException.builder().build());
                            } else {
                                return DescribeStreamConsumerResponse.builder().build();
                            }
                        }
                    };

            return new TestableKinesisProxySyncV2(
                    fakeKinesisSyncClient,
                    new FakeSdkSyncHttpClient(),
                    createConfiguration(),
                    new FakeFullJitterBackoff());
        }

        public static TestableKinesisProxySyncV2
                createTestableKinesisProxyAsyncV2FailDescribeStreamSummary() {
            FakeKinesisSyncClient fakeKinesisSyncClient =
                    new FakeKinesisSyncClient() {
                        @Override
                        public DescribeStreamSummaryResponse describeStreamSummary(
                                DescribeStreamSummaryRequest describeStreamSummaryRequest)
                                throws InvalidArgumentException, LimitExceededException,
                                        ResourceInUseException, ResourceNotFoundException,
                                        AwsServiceException, SdkClientException, KinesisException {
                            if (!isDescribeStreamSummaryCalled) {
                                isDescribeStreamSummaryCalled = true;
                                throw new RuntimeException(
                                        LimitExceededException.builder().build());
                            } else {
                                return DescribeStreamSummaryResponse.builder().build();
                            }
                        }
                    };

            return new TestableKinesisProxySyncV2(
                    fakeKinesisSyncClient,
                    new FakeSdkSyncHttpClient(),
                    createConfiguration(),
                    new FakeFullJitterBackoff());
        }

        public static TestableKinesisProxySyncV2
                createTestableKinesisProxyAsyncV2ConstantFailDescribeStreamSummary() {
            FakeKinesisSyncClient fakeKinesisSyncClient =
                    new FakeKinesisSyncClient() {
                        @Override
                        public DescribeStreamSummaryResponse describeStreamSummary(
                                DescribeStreamSummaryRequest describeStreamSummaryRequest)
                                throws InvalidArgumentException, LimitExceededException,
                                        ResourceInUseException, ResourceNotFoundException,
                                        AwsServiceException, SdkClientException, KinesisException {
                            throw new RuntimeException(LimitExceededException.builder().build());
                        }
                    };

            return new TestableKinesisProxySyncV2(
                    fakeKinesisSyncClient,
                    new FakeSdkSyncHttpClient(),
                    createConfiguration(),
                    new FakeFullJitterBackoff());
        }
    }

    private static class FakeSdkSyncHttpClient implements SdkHttpClient {
        public boolean isClosed;

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest httpExecuteRequest) {
            return null;
        }
    }

    private static class FakeFullJitterBackoff extends FullJitterBackoff {
        public long backoffDuration;
        public long baseMillis;
        public long maxMillis;
        public double power;
        public int attempt;

        public FakeFullJitterBackoff() {
            super();
            this.backoffDuration = 0L;
        }

        @Override
        public void sleep(long millisToSleep) throws InterruptedException {
            backoffDuration += millisToSleep;
            super.sleep(millisToSleep);
        }

        @Override
        public long calculateFullJitterBackoff(
                long baseMillis, long maxMillis, double power, int attempt) {
            this.baseMillis = baseMillis;
            this.maxMillis = maxMillis;
            this.power = power;
            this.attempt = attempt;

            return super.calculateFullJitterBackoff(baseMillis, maxMillis, power, attempt);
        }
    }
}
