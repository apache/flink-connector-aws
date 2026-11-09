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

package org.apache.flink.connector.dynamodb.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants;
import org.apache.flink.connector.dynamodb.testutils.DynamoDBHelpers;
import org.apache.flink.connector.dynamodb.testutils.DynamoDbContainer;
import org.apache.flink.connector.dynamodb.testutils.DynamoDbStreamsSourceITDeserializationSchema;
import org.apache.flink.connector.dynamodb.testutils.DynamoDbStreamsSourceITEvent;
import org.apache.flink.connector.dynamodb.testutils.Item;
import org.apache.flink.connector.dynamodb.util.DockerImageVersions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION;

/** Integration test for {@link DynamoDbStreamsSource}. */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class DynamoDbStreamsSourceITCase {

    private static final String PARTITION_KEY = "key";
    private static final String SORT_KEY = "sort_key";
    private static DynamoDBHelpers dynamoDBHelpers;
    private static String testTableName;

    private static StreamExecutionEnvironment env;

    // shared between test methods
    @Container
    private static final DynamoDbContainer LOCALSTACK =
            new DynamoDbContainer(DockerImageName.parse(DockerImageVersions.DYNAMODB))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("dynamodb");

    @BeforeEach
    void setup() throws URISyntaxException {
        testTableName = UUID.randomUUID().toString();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY.getMainValue());
        env.configure(config);
        env.setParallelism(1);
        dynamoDBHelpers = new DynamoDBHelpers(LOCALSTACK.getHostClient());
    }

    private Configuration getDefaultConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(AWS_ENDPOINT, LOCALSTACK.getHostEndpointUrl());
        configuration.setString(AWS_ACCESS_KEY_ID, LOCALSTACK.getAccessKey());
        configuration.setString(AWS_SECRET_ACCESS_KEY, LOCALSTACK.getSecretKey());
        configuration.setString(AWS_REGION, LOCALSTACK.getRegion().toString());
        configuration.setString(TRUST_ALL_CERTIFICATES, "true");
        configuration.setString(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        configuration.set(
                STREAM_INITIAL_POSITION,
                DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON);
        return configuration;
    }

    private Map<String, AttributeValue> insertItem() {
        return Item.builder()
                .attr(PARTITION_KEY, "2")
                .attr(SORT_KEY, "1")
                .attr("payload", "value")
                .build();
    }

    private Map<String, AttributeValue> updateItem1() {
        return Item.builder()
                .attr(PARTITION_KEY, "2")
                .attr(SORT_KEY, "1")
                .attr("payload", "updated value")
                .build();
    }

    private Map<String, AttributeValue> updateItem2() {
        return Item.builder()
                .attr(PARTITION_KEY, "2")
                .attr(SORT_KEY, "1")
                .attr("payload", "updated2 value")
                .build();
    }

    private Map<String, AttributeValue> deleteItem() {
        return Item.builder().attr(PARTITION_KEY, "2").attr(SORT_KEY, "1").build();
    }

    @Test
    void nonExistentStreamArnShouldResultInFailure() throws Exception {
        org.assertj.core.api.Assertions.assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withTableName(testTableName)
                                        .withElements(Arrays.asList(insertItem()))
                                        .withOperations(Arrays.asList("INSERT"))
                                        .withExpectedElements(1)
                                        .withSourceConnectionStreamArn(
                                                "arn:aws:dynamodb:ap-southeast-1:000000000000:table/testddb-table/stream/2025-01-01T01:01:01.001")
                                        .runScenario())
                .withStackTraceContaining(
                        "Stream: arn:aws:dynamodb:ap-southeast-1:000000000000:table/testddb-table/stream/2025-01-01T01:01:01.001 not found");
    }

    @Test
    void dynamodbInsert() throws Exception {
        new Scenario()
                .withTableName(testTableName)
                .withElements(Arrays.asList(insertItem()))
                .withOperations(Arrays.asList("INSERT"))
                .withExpectedElements(1)
                .runScenario();
    }

    @Test
    void dynamodbInsertModify() throws Exception {
        new Scenario()
                .withTableName(testTableName)
                .withElements(Arrays.asList(insertItem(), updateItem1()))
                .withOperations(Arrays.asList("INSERT", "MODIFY"))
                .withExpectedElements(2)
                .runScenario();
    }

    @Test
    void dynamodbInsertDelete() throws Exception {
        new Scenario()
                .withTableName(testTableName)
                .withElements(Arrays.asList(insertItem(), deleteItem()))
                .withOperations(Arrays.asList("INSERT", "REMOVE"))
                .withExpectedElements(2)
                .runScenario();
    }

    @Test
    void dynamodbInsertModifyDelete() throws Exception {
        new Scenario()
                .withTableName(testTableName)
                .withElements(Arrays.asList(insertItem(), updateItem1(), deleteItem()))
                .withOperations(Arrays.asList("INSERT", "MODIFY", "REMOVE"))
                .withExpectedElements(3)
                .runScenario();
    }

    @Test
    void dynamodbInsertDeleteReinsert() throws Exception {
        new Scenario()
                .withTableName(testTableName)
                .withElements(Arrays.asList(insertItem(), deleteItem(), insertItem()))
                .withOperations(Arrays.asList("INSERT", "REMOVE", "INSERT"))
                .withExpectedElements(2)
                .runScenario();
    }

    @Test
    void dynamodbInsertMultipleModifyDelete() throws Exception {
        new Scenario()
                .withTableName(testTableName)
                .withElements(
                        Arrays.asList(insertItem(), updateItem1(), updateItem2(), deleteItem()))
                .withOperations(Arrays.asList("INSERT", "MODIFY", "MODIFY", "REMOVE"))
                .withExpectedElements(3)
                .runScenario();
    }

    private class Scenario {

        private String tableName;
        private String sourceConnectionStreamArn;
        private List<String> operations;
        private List<Map<String, AttributeValue>> elements;
        private int expectedElements;
        private Configuration configuration =
                DynamoDbStreamsSourceITCase.this.getDefaultConfiguration();

        public void runScenario() throws Exception {
            dynamoDBHelpers.createTable(tableName, PARTITION_KEY, SORT_KEY, true);
            performOperations(expectedElements, operations, elements);

            if (sourceConnectionStreamArn == null) {
                sourceConnectionStreamArn = dynamoDBHelpers.getTableStreamArn(tableName);
            }

            DynamoDbStreamsSource<DynamoDbStreamsSourceITEvent> dynamoDbStreamsSource =
                    DynamoDbStreamsSource.<DynamoDbStreamsSourceITEvent>builder()
                            .setStreamArn(sourceConnectionStreamArn)
                            .setSourceConfig(configuration)
                            .setDeserializationSchema(
                                    new DynamoDbStreamsSourceITDeserializationSchema())
                            .build();

            List<DynamoDbStreamsSourceITEvent> result =
                    env.fromSource(
                                    dynamoDbStreamsSource,
                                    WatermarkStrategy.noWatermarks(),
                                    "Dynamodb Streams source")
                            .returns(TypeInformation.of(DynamoDbStreamsSourceITEvent.class))
                            .executeAndCollect(expectedElements);

            // Test every change to dynamodb table is captured while reading dynamodb streams
            testAssertions(result, expectedElements, operations, elements);

            // Verify the correct order of events using sequence numbers
            verifyOrder(result);
        }

        private void performOperations(
                int expectedElements,
                List<String> operations,
                List<Map<String, AttributeValue>> elements)
                throws ExecutionException, InterruptedException {
            for (int i = 0; i < expectedElements; i++) {
                String operation = operations.get(i);
                switch (operation) {
                    case "REMOVE":
                        dynamoDBHelpers.deleteItem(tableName, elements.get(i));
                        break;
                    case "INSERT":
                    case "MODIFY":
                        dynamoDBHelpers.putItem(tableName, elements.get(i));
                }
            }
        }

        private void testAssertions(
                List<DynamoDbStreamsSourceITEvent> result,
                int expectedElements,
                List<String> operations,
                List<Map<String, AttributeValue>> elements) {

            Assertions.assertThat(result.size()).isEqualTo(expectedElements);
            for (int i = 0; i < expectedElements; i++) {
                String operation = operations.get(i);
                Assertions.assertThat(result.get(i).getEventType()).isEqualTo(operation);
                switch (operation) {
                    case "INSERT":
                        Assertions.assertThat(result.get(i).getOldImage()).isEmpty();
                        break;
                    case "REMOVE":
                        Assertions.assertThat(result.get(i).getNewImage()).isEmpty();
                        break;
                    case "MODIFY":
                        Assertions.assertThat(result.get(i).getOldImage()).isNotNull().isNotEmpty();
                        Assertions.assertThat(result.get(i).getNewImage()).isNotNull().isNotEmpty();
                }
            }
        }

        private void verifyOrder(List<DynamoDbStreamsSourceITEvent> result) {
            for (int i = 1; i < result.size(); i++) {
                String prevSeq = result.get(i - 1).getSequenceNumber();
                String currSeq = result.get(i).getSequenceNumber();
                Assertions.assertThat(prevSeq).isLessThan(currSeq);
            }
        }

        public Scenario withExpectedElements(int expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Scenario withElements(List<Map<String, AttributeValue>> elements) {
            this.elements = elements;
            return this;
        }

        public Scenario withOperations(List<String> operations) {
            this.operations = operations;
            return this;
        }

        public Scenario withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Scenario withSourceConnectionStreamArn(String sourceConnectionStreamArn) {
            this.sourceConnectionStreamArn = sourceConnectionStreamArn;
            return this;
        }
    }
}
