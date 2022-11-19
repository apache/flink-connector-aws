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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.dynamodb.testutils.DynamoDBHelpers;
import org.apache.flink.connector.dynamodb.testutils.DynamoDbContainer;
import org.apache.flink.connector.dynamodb.testutils.Item;
import org.apache.flink.connector.dynamodb.testutils.Items;
import org.apache.flink.connector.dynamodb.util.DockerImageVersions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

/** Integration test for {@link DynamoDbSink}. */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class DynamoDbSinkITCase {
    private static final String PARTITION_KEY = "key";
    private static final String SORT_KEY = "sort_key";
    private static DynamoDBHelpers dynamoDBHelpers;
    private static String testTableName;

    private static StreamExecutionEnvironment env;

    // shared between test methods
    @Container
    public static final DynamoDbContainer LOCALSTACK =
            new DynamoDbContainer(DockerImageName.parse(DockerImageVersions.DYNAMODB))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("dynamodb");

    @BeforeEach
    public void setup() throws URISyntaxException {
        testTableName = UUID.randomUUID().toString();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);

        dynamoDBHelpers = new DynamoDBHelpers(LOCALSTACK.getHostClient());
    }

    @Test
    public void testRandomDataSuccessfullyWritten() throws Exception {
        int expectedNumOfElements = 10;
        int bytesPerMessage = 10;
        new Scenario(getRandomDataGenerator(bytesPerMessage, expectedNumOfElements))
                .withTableName(testTableName)
                .withExpectedElements(expectedNumOfElements)
                .runScenario();
    }

    @Test
    public void veryLargeMessagesFailsGracefullyWhenRejectedByDynamoDb() throws Exception {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                // 500 * 1000 bytes is more than DynamoDB allows for a single record
                                new Scenario(getRandomDataGenerator(500 * 1000, 5))
                                        .withExpectedElements(5)
                                        .withTableName(testTableName)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .havingCause()
                .withMessageContaining("Item size has exceeded the maximum allowed size");
    }

    @Test
    public void nonExistentTableNameShouldResultInFailureWhenFailOnErrorIsFalse() {
        List<Map<String, AttributeValue>> items =
                Items.builder().item(Item.builder().attr("1", "1").build()).build();
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario(env.fromCollection(items))
                                        .withTableName("NonExistentTableName")
                                        .withFailOnError(false)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining("Encountered non-recoverable exception");
    }

    @Test
    public void batchRequestFailsOnDuplicates() {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario(
                                                env.fromCollection(
                                                        getItemsWithDuplicatedCompositeKey()))
                                        .withTableName(testTableName)
                                        .withBufferMaxTimeMS(60 * 1000)
                                        .withExpectedElements(1)
                                        .withMaxInflightReqs(1)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .havingCause()
                .withMessageContaining("Provided list of item keys contains duplicates");
    }

    @Test
    public void deduplicatesOnPartitionKey() throws Exception {
        new Scenario(env.fromCollection(getItemsWithDuplicatedPartitionKey()))
                .withTableName(testTableName)
                .withOverwriteByPartitionKeys(ImmutableList.of(PARTITION_KEY))
                .withBufferMaxTimeMS(60 * 1000)
                .withExpectedElements(1)
                .withMaxInflightReqs(1)
                .runScenario();
    }

    @Test
    public void deduplicatesOnCompositeKeyAndNewerItemTakesPrecedence() throws Exception {
        new Scenario(env.fromCollection(getItemsWithDuplicatedCompositeKey()))
                .withTableName(testTableName)
                .withOverwriteByPartitionKeys(ImmutableList.of(PARTITION_KEY, SORT_KEY))
                .withBufferMaxTimeMS(60 * 1000)
                // more than one in-flight request may cause a race condition
                // where the first request to complete will take precedence
                .withMaxInflightReqs(1)
                .withExpectedElements(1)
                .withExpectedAttribute("payload", "value3")
                .runScenario();
    }

    private List<Map<String, AttributeValue>> getItemsWithDuplicatedCompositeKey() {
        return Items.builder()
                .item(
                        Item.builder()
                                .attr(PARTITION_KEY, "2")
                                .attr(SORT_KEY, "1")
                                .attr("payload", "value1")
                                .build())
                .item(
                        Item.builder()
                                .attr(PARTITION_KEY, "2")
                                .attr(SORT_KEY, "1")
                                .attr("payload", "value2")
                                .build())
                .item(
                        Item.builder()
                                .attr(PARTITION_KEY, "2")
                                .attr(SORT_KEY, "1")
                                .attr("payload", "value3")
                                .build())
                .build();
    }

    private List<Map<String, AttributeValue>> getItemsWithDuplicatedPartitionKey() {
        return Items.builder()
                .item(
                        Item.builder()
                                .attr(PARTITION_KEY, "1")
                                .attr(SORT_KEY, "1")
                                .attr("payload", "value1")
                                .build())
                .item(
                        Item.builder()
                                .attr(PARTITION_KEY, "1")
                                .attr(SORT_KEY, "2")
                                .attr("payload", "value2")
                                .build())
                .item(
                        Item.builder()
                                .attr(PARTITION_KEY, "1")
                                .attr(SORT_KEY, "3")
                                .attr("payload", "value3")
                                .build())
                .build();
    }

    private DataStream<Map<String, AttributeValue>> getRandomDataGenerator(
            int sizeOfMessageBytes, long numberOfElementsToSend) {
        return env.addSource(
                        new DataGeneratorSource<>(
                                RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                100,
                                numberOfElementsToSend))
                .returns(String.class)
                .map(new TestRequestMapper(PARTITION_KEY, SORT_KEY));
    }

    private Properties getDefaultPropertiesWithoutCredentialsSetAndCredentialProvider(
            String credentialsProvider) {
        Properties properties = getDefaultProperties();
        properties.setProperty(AWS_CREDENTIALS_PROVIDER, credentialsProvider);
        properties.remove(AWS_SECRET_ACCESS_KEY);
        properties.remove(AWS_ACCESS_KEY_ID);
        return properties;
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(AWS_ENDPOINT, LOCALSTACK.getHostEndpointUrl());
        properties.setProperty(AWS_ACCESS_KEY_ID, LOCALSTACK.getAccessKey());
        properties.setProperty(AWS_SECRET_ACCESS_KEY, LOCALSTACK.getSecretKey());
        properties.setProperty(AWS_REGION, LOCALSTACK.getRegion().toString());
        properties.setProperty(TRUST_ALL_CERTIFICATES, "true");
        properties.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        return properties;
    }

    private class Scenario {

        private final DataStream<Map<String, AttributeValue>> dataGenerator;

        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 50;
        private int maxBatchSize = 25;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String tableName;
        private String expectedAttributeName;
        private String expectedAttributeValue;
        private Properties properties = DynamoDbSinkITCase.this.getDefaultProperties();

        private final List<String> deduplicateOnKeys = new ArrayList<>();

        public Scenario(DataStream<Map<String, AttributeValue>> dataGenerator) {
            this.dataGenerator = dataGenerator;
        }

        public void runScenario() throws Exception {
            dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);

            DynamoDbSink<Map<String, AttributeValue>> dynamoDbSink =
                    DynamoDbSink.<Map<String, AttributeValue>>builder()
                            .setElementConverter(new TestDynamoDbElementConverter())
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setTableName(tableName)
                            .setOverwriteByPartitionKeys(deduplicateOnKeys)
                            .setDynamoDbProperties(properties)
                            .build();

            dataGenerator.sinkTo(dynamoDbSink);

            env.execute("DynamoDbSink Async Sink Example Program");

            Assertions.assertThat(dynamoDBHelpers.getItemsCount(tableName))
                    .isEqualTo(expectedElements);

            if (!StringUtils.isNullOrWhitespaceOnly(expectedAttributeName)) {
                Assertions.assertThat(
                                dynamoDBHelpers.containsAttributeValue(
                                        tableName, expectedAttributeName, expectedAttributeValue))
                        .isTrue();
            }
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

        public Scenario withExpectedAttribute(String name, String value) {
            this.expectedAttributeName = name;
            this.expectedAttributeValue = value;
            return this;
        }

        public Scenario withFailOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public Scenario withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Scenario withOverwriteByPartitionKeys(List<String> deduplicateOnKeys) {
            this.deduplicateOnKeys.addAll(deduplicateOnKeys);
            return this;
        }

        public Scenario withClientProperties(Properties properties) {
            this.properties = properties;
            return this;
        }
    }
}
