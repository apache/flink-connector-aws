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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.proxy.ListShardsStartingPosition;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.TestKinesisStreamProxy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.ConsumerDescription;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ConsumerLifecycle.JOB_MANAGED;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ConsumerLifecycle.SELF_MANAGED;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_LIFECYCLE;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_NAME;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DEREGISTER_CONSUMER_TIMEOUT;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_DELAY_OPTION;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MIN_DELAY_OPTION;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.READER_TYPE;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ReaderType.EFO;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ReaderType.POLLING;
import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class StreamConsumerRegistrarTest {

    private static final String CONSUMER_NAME = "kon-soo-mer";

    private TestKinesisStreamProxy testKinesisStreamProxy;
    private StreamConsumerRegistrar streamConsumerRegistrar;

    private Configuration sourceConfiguration;

    @BeforeEach
    void setUp() {
        testKinesisStreamProxy = getTestStreamProxy();
        sourceConfiguration = new Configuration();
        streamConsumerRegistrar =
                new StreamConsumerRegistrar(
                        sourceConfiguration, STREAM_ARN, testKinesisStreamProxy);
    }

    @Test
    void testRegisterStreamConsumerSkippedWhenNotEfo() {
        // Given POLLING reader type
        sourceConfiguration.set(READER_TYPE, POLLING);

        // When registerStreamConsumer is called
        // Then we skip registering consumer
        assertThatNoException().isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer());
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN)).hasSize(0);
    }

    @Test
    void testConsumerNameMustBeSpecified() {
        // Given JOB_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        // Consumer name not provided

        // When registerStreamConsumer is called
        // Then exception is thrown
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer())
                .withMessageContaining("For EFO reader type, EFO consumer name must be specified");
    }

    @Test
    void testConsumerNameMustNotBeEmpty() {
        // Given JOB_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        // Consumer name is empty
        sourceConfiguration.set(EFO_CONSUMER_NAME, "");

        // When registerStreamConsumer is called
        // Then exception is thrown
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer())
                .withMessageContaining("For EFO reader type, EFO consumer name cannot be empty.");
    }

    @Test
    void testDeregisterStreamConsumerSkippedWhenNotEfo() {
        // Given POLLING reader type
        sourceConfiguration.set(READER_TYPE, POLLING);
        // And consumer is registered
        testKinesisStreamProxy.registerStreamConsumer(STREAM_ARN, CONSUMER_NAME);

        // When registerStreamConsumer is called
        // Then we skip registering consumer
        // We validate this by showing that no exception is thrown by checks that consumerName was
        // not specified
        assertThatNoException().isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer());
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);
    }

    @Test
    void testRegisterStreamConsumerSkippedWhenSelfManaged() {
        // Given SELF_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, SELF_MANAGED);
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        // And consumer is registered
        testKinesisStreamProxy.registerStreamConsumer(STREAM_ARN, CONSUMER_NAME);

        // When registerStreamConsumer is called
        // Then we skip registering the consumer
        assertThatNoException().isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer());
    }

    @Test
    void testRegisterStreamConsumerFailsFastWhenSelfManaged() {
        // Given SELF_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, SELF_MANAGED);
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        // And consumer not registered

        // When registerStreamConsumer is called
        // Then we fail fast to indicate consumer doesn't exist
        assertThatExceptionOfType(ResourceNotFoundException.class)
                .isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer())
                .withMessageContaining("Consumer " + CONSUMER_NAME)
                .withMessageContaining("not found.");
    }

    @Test
    void testDeregisterStreamConsumerSkippedWhenSelfManaged() {
        // Given SELF_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, SELF_MANAGED);
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        // And consumer is registered
        testKinesisStreamProxy.registerStreamConsumer(STREAM_ARN, CONSUMER_NAME);

        // When registerStreamConsumer is called
        // Then we skip registering the consumer
        assertThatNoException().isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer());
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);
    }

    @Test
    void testRegisterStreamConsumerWhenJobManaged() {
        // Given JOB_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);

        // When registerStreamConsumer is called
        streamConsumerRegistrar.registerStreamConsumer();

        // Then consumer is registered
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);
    }

    @Test
    void testRegisterStreamConsumerHandledGracefullyWhenConsumerExists() {
        // Given JOB_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        // And consumer already exists
        testKinesisStreamProxy.registerStreamConsumer(STREAM_ARN, CONSUMER_NAME);
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);

        // When registerStreamConsumer is called
        assertThatNoException().isThrownBy(() -> streamConsumerRegistrar.registerStreamConsumer());

        // Then consumer is registered
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);
    }

    @Test
    void testDeregisterStreamConsumerWhenJobManaged() {
        // Given JOB_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        // And consumer is registered
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        streamConsumerRegistrar.registerStreamConsumer();
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);

        // When deregisterStreamConsumer is called
        streamConsumerRegistrar.deregisterStreamConsumer();

        // Then consumer is deregistered
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN)).hasSize(0);
    }

    @Test
    void testDeregisterStreamConsumerProceedsWhenTimeoutDeregistering() {
        // Given JOB_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        sourceConfiguration.set(EFO_DEREGISTER_CONSUMER_TIMEOUT, Duration.ofMillis(50));
        // And consumer is registered
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        streamConsumerRegistrar.registerStreamConsumer();
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);
        // And consumer is stuck in DELETING
        testKinesisStreamProxy.setConsumersCurrentlyDeleting(CONSUMER_NAME);

        // When deregisterStreamConsumer is called
        streamConsumerRegistrar.deregisterStreamConsumer();

        // Then consumer is deregistered
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN)).hasSize(0);
    }

    // ----- Self-healing (ensureActiveConsumer) -----

    @Test
    void testConsumerArnRecordedEvenWhenConsumerAlreadyExistsWhenJobManaged() throws Exception {
        // Given JOB_MANAGED consumer lifecycle
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        // And the consumer already exists — as if left behind by a still-active previous
        // attempt, not created by this registrar
        testKinesisStreamProxy.registerStreamConsumer(STREAM_ARN, CONSUMER_NAME);

        // When registerStreamConsumer is called (hits the "already exists" conflict path)
        streamConsumerRegistrar.registerStreamConsumer();

        // Then the consumer ARN was recorded from the conflict path too — proven by
        // deregistration actually working, instead of silently no-op'ing because consumerArn
        // was never captured (the bug this test guards against).
        streamConsumerRegistrar.deregisterStreamConsumer();
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN)).hasSize(0);
    }

    @Test
    void testRegisterStreamConsumerWaitsOutDeletingBeforeRegisteringWhenJobManaged()
            throws Exception {
        // Given JOB_MANAGED consumer lifecycle, with a fast poll interval for the test
        sourceConfiguration.set(READER_TYPE, EFO);
        sourceConfiguration.set(EFO_CONSUMER_LIFECYCLE, JOB_MANAGED);
        sourceConfiguration.set(EFO_CONSUMER_NAME, CONSUMER_NAME);
        sourceConfiguration.set(
                EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MIN_DELAY_OPTION, Duration.ofMillis(5));
        sourceConfiguration.set(
                EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_DELAY_OPTION, Duration.ofMillis(20));
        // And a same-named consumer is currently DELETING (e.g. a previous attempt's
        // deregistration is still converging)
        testKinesisStreamProxy.setConsumersCurrentlyDeleting(CONSUMER_NAME);

        // Deletion finishes shortly after we start waiting.
        ScheduledExecutorService clearer = Executors.newSingleThreadScheduledExecutor();
        clearer.schedule(
                () -> testKinesisStreamProxy.unsetConsumersCurrentlyDeleting(CONSUMER_NAME),
                50,
                TimeUnit.MILLISECONDS);

        // When registerStreamConsumer is called
        try {
            streamConsumerRegistrar.registerStreamConsumer();
        } finally {
            clearer.shutdownNow();
        }

        // Then it waited out DELETING instead of registering (or erroring) against a consumer
        // mid-teardown, and created a fresh one once it was actually gone
        assertThat(testKinesisStreamProxy.getRegisteredConsumers(STREAM_ARN))
                .containsExactly(CONSUMER_NAME);
    }

    @Test
    void testEnsureActiveConsumerToleratesConcurrentResourceInUseOnRegister() {
        // Simulates a TOCTOU race: describe says "not found", but register then hits
        // ResourceInUseException because something else created it in between.
        AtomicInteger describeCallCount = new AtomicInteger();
        StreamProxy raceSimulatingProxy =
                new StreamProxy() {
                    @Override
                    public StreamDescriptionSummary getStreamDescriptionSummary(String streamArn) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public List<Shard> listShards(
                            String streamArn, ListShardsStartingPosition startingPosition) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public GetRecordsResponse getRecords(
                            String streamArn,
                            String shardId,
                            StartingPosition startingPosition,
                            int maxRecordsToGet) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public RegisterStreamConsumerResponse registerStreamConsumer(
                            String streamArn, String consumerName) {
                        throw ResourceInUseException.builder()
                                .message("appeared concurrently")
                                .build();
                    }

                    @Override
                    public DeregisterStreamConsumerResponse deregisterStreamConsumer(
                            String consumerArn) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DescribeStreamConsumerResponse describeStreamConsumer(
                            String streamArn, String consumerName) {
                        if (describeCallCount.getAndIncrement() == 0) {
                            throw ResourceNotFoundException.builder()
                                    .message("not found yet")
                                    .build();
                        }
                        return DescribeStreamConsumerResponse.builder()
                                .consumerDescription(
                                        ConsumerDescription.builder()
                                                .consumerName(consumerName)
                                                .consumerStatus(ConsumerStatus.ACTIVE)
                                                .consumerARN(
                                                        streamArn + "/consumer/" + consumerName)
                                                .build())
                                .build();
                    }

                    @Override
                    public void close() {}
                };

        String arn =
                StreamConsumerRegistrar.ensureActiveConsumer(
                        raceSimulatingProxy,
                        STREAM_ARN,
                        CONSUMER_NAME,
                        Duration.ofMillis(1),
                        Duration.ofMillis(5),
                        10);

        assertThat(arn).isEqualTo(STREAM_ARN + "/consumer/" + CONSUMER_NAME);
    }
}
