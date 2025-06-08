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

package org.apache.flink.connector.kinesis.sink.test;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.connector.kinesis.testutils.AWSEndToEndTestUtils;
import org.apache.flink.connector.kinesis.testutils.AWSKinesisResourceManager;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;

/** End-to-end tests for Kinesis Data Streams Sink. */
@ExtendWith(MiniClusterExtension.class)
@Execution(ExecutionMode.CONCURRENT)
@Tag("requires-aws-credentials")
@Disabled
class KinesisStreamsSinkITCase {

    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";

    private final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
    private final PartitionKeyGenerator<String> partitionKeyGenerator =
            element -> String.valueOf(element.hashCode());
    private final PartitionKeyGenerator<String> longPartitionKeyGenerator = element -> element;

    private StreamExecutionEnvironment env;
    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;
    private AWSKinesisResourceManager kinesisResourceManager;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSEndToEndTestUtils.createAwsSyncClient(httpClient, KinesisClient.builder());
        kinesisResourceManager = new AWSKinesisResourceManager(kinesisClient);
    }

    @AfterEach
    void teardown() {
        kinesisResourceManager.close();
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    @Test
    void elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached() throws Exception {
        new Scenario().runScenario();
    }

    @Test
    void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Exception {
        new Scenario()
                .withNumberOfElementsToSend(10)
                .withMaxBatchSize(100)
                .withExpectedElements(10)
                .runScenario();
    }

    @Test
    void veryLargeMessagesSucceedInBeingPersisted() throws Exception {
        new Scenario()
                .withNumberOfElementsToSend(5)
                .withSizeOfMessageBytes(2500)
                .withMaxBatchSize(10)
                .withExpectedElements(5)
                .runScenario();
    }

    @Test
    void multipleInFlightRequestsResultsInCorrectNumberOfElementsPersisted() throws Exception {
        new Scenario()
                .withNumberOfElementsToSend(150)
                .withSizeOfMessageBytes(2500)
                .withBufferMaxTimeMS(2000)
                .withMaxInflightReqs(10)
                .withMaxBatchSize(20)
                .withExpectedElements(150)
                .runScenario();
    }

    @Test
    void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOn() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(true);
    }

    @Test
    void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOff() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(false);
    }

    private void testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(
            boolean failOnError) {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withSinkConnectionStreamName(
                                                "flink-test-stream-not-exists")
                                        .withFailOnError(failOnError)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .havingCause()
                .withMessageContaining("Stream flink-test-stream-not-exists under account");
    }

    @Test
    void veryLargeMessagesFailGracefullyWithBrokenElementConverter() {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withNumberOfElementsToSend(5)
                                        .withSizeOfMessageBytes(2500)
                                        .withExpectedElements(5)
                                        .withSerializationSchema(serializationSchema)
                                        .withPartitionKeyGenerator(longPartitionKeyGenerator)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining(
                        "Encountered an exception while persisting records, not retrying due to {failOnError} being set.");
    }

    @Test
    void badEndpointShouldResultInFailureWhenInFailOnErrorIsOn() {
        badEndpointShouldResultInFailureWhenInFailOnErrorIs(true);
    }

    @Test
    void badEndpointShouldResultInFailureWhenInFailOnErrorIsOff() {
        badEndpointShouldResultInFailureWhenInFailOnErrorIs(false);
    }

    private void badEndpointShouldResultInFailureWhenInFailOnErrorIs(boolean failOnError) {
        Properties properties = getDefaultProperties();
        properties.setProperty(AWS_ENDPOINT, "https://bad-endpoint-with-uri");
        assertRunWithPropertiesAndStreamShouldFailWithExceptionOfType(
                failOnError,
                properties,
                "UnknownHostException when attempting to interact with a service.");
    }

    @Test
    void accessDeniedShouldFailJobWhenFailOnErrorIsOn() {
        accessDeniedShouldFailJobWhenFailOnErrorIs(true);
    }

    @Test
    void accessDeniedShouldFailJobWhenFailOnErrorIsOff() {
        accessDeniedShouldFailJobWhenFailOnErrorIs(false);
    }

    private void accessDeniedShouldFailJobWhenFailOnErrorIs(boolean failOnError) {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withSinkConnectionStreamName(
                                                "no-permission-for-this-stream")
                                        .withFailOnError(failOnError)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining(
                        "Encountered an exception while persisting records, not retrying due to {failOnError} being set.")
                .havingCause()
                .withMessageContaining(
                        "is not authorized to perform: kinesis:PutRecords on resource");
    }

    private void assertRunWithPropertiesAndStreamShouldFailWithExceptionOfType(
            boolean failOnError, Properties properties, String expectedMessage) {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withFailOnError(failOnError)
                                        .withProperties(properties)
                                        .runScenario())
                .withStackTraceContaining(expectedMessage);
    }

    private Properties getDefaultProperties() {
        return AWSEndToEndTestUtils.createTestConfig();
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String kinesisDataStreamARN = null;
        private String sinkConnectionStreamName = null;
        private String sinkConnectionStreamArn = null;
        private SerializationSchema<String> serializationSchema =
                KinesisStreamsSinkITCase.this.serializationSchema;
        private PartitionKeyGenerator<String> partitionKeyGenerator =
                KinesisStreamsSinkITCase.this.partitionKeyGenerator;
        private Properties properties = KinesisStreamsSinkITCase.this.getDefaultProperties();

        public void runScenario() throws Exception {
            kinesisDataStreamARN = kinesisResourceManager.createKinesisDataStream();
            if (sinkConnectionStreamName == null) {
                sinkConnectionStreamArn = kinesisDataStreamARN;
            }

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            KinesisStreamsSink<String> kdsSink =
                    KinesisStreamsSink.<String>builder()
                            .setSerializationSchema(serializationSchema)
                            .setPartitionKeyGenerator(partitionKeyGenerator)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setStreamName(sinkConnectionStreamName)
                            .setStreamArn(sinkConnectionStreamArn)
                            .setKinesisClientProperties(properties)
                            .setFailOnError(true)
                            .build();

            stream.sinkTo(kdsSink);

            env.execute("KDS Async Sink Example Program");

            String shardIterator =
                    kinesisClient
                            .getShardIterator(
                                    GetShardIteratorRequest.builder()
                                            .shardId(DEFAULT_FIRST_SHARD_NAME)
                                            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                            .streamARN(kinesisDataStreamARN)
                                            .build())
                            .shardIterator();

            Assertions.assertThat(
                            kinesisClient
                                    .getRecords(
                                            GetRecordsRequest.builder()
                                                    .shardIterator(shardIterator)
                                                    .build())
                                    .records()
                                    .size())
                    .isEqualTo(expectedElements);
        }

        public Scenario withNumberOfElementsToSend(int numberOfElementsToSend) {
            this.numberOfElementsToSend = numberOfElementsToSend;
            return this;
        }

        public Scenario withSizeOfMessageBytes(int sizeOfMessageBytes) {
            this.sizeOfMessageBytes = sizeOfMessageBytes;
            return this;
        }

        public Scenario withBufferMaxTimeMS(int bufferMaxTimeMS) {
            this.bufferMaxTimeMS = bufferMaxTimeMS;
            return this;
        }

        public Scenario withMaxInflightReqs(int maxInflightReqs) {
            this.maxInflightReqs = maxInflightReqs;
            return this;
        }

        public Scenario withMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Scenario withExpectedElements(int expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Scenario withFailOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public Scenario withSinkConnectionStreamName(String sinkConnectionStreamName) {
            this.sinkConnectionStreamName = sinkConnectionStreamName;
            return this;
        }

        public Scenario withSerializationSchema(SerializationSchema<String> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        public Scenario withPartitionKeyGenerator(
                PartitionKeyGenerator<String> partitionKeyGenerator) {
            this.partitionKeyGenerator = partitionKeyGenerator;
            return this;
        }

        public Scenario withProperties(Properties properties) {
            this.properties = properties;
            return this;
        }
    }
}
