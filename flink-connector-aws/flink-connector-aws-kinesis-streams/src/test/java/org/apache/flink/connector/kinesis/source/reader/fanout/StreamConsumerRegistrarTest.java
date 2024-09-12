package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.TestKinesisStreamProxy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.time.Duration;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ConsumerLifecycle.JOB_MANAGED;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ConsumerLifecycle.SELF_MANAGED;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_LIFECYCLE;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_NAME;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DEREGISTER_CONSUMER_TIMEOUT;
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
}
