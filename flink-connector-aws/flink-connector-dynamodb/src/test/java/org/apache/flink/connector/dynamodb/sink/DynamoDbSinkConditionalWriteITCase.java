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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.testutils.DynamoDBHelpers;
import org.apache.flink.connector.dynamodb.testutils.DynamoDbContainer;
import org.apache.flink.connector.dynamodb.testutils.TestItem;
import org.apache.flink.connector.dynamodb.util.DockerImageVersions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

/**
 * Integration test for batch and single writes in {@link DynamoDbSink} using {@link DynamoDbBeanElementConverter}.
 */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class DynamoDbSinkConditionalWriteITCase {

    private static final String PARTITION_KEY = "pk";
    private static final String SORT_KEY = "sk";
    private static DynamoDBHelpers dynamoDBHelpers;
    private static String testTableName;

    @Container
    public static final DynamoDbContainer LOCALSTACK =
            new DynamoDbContainer(DockerImageName.parse(DockerImageVersions.DYNAMODB))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("dynamodb");

    @BeforeEach
    public void setup() throws URISyntaxException {
        testTableName = "test_" + UUID.randomUUID().toString().replace("-", "");
        dynamoDBHelpers = new DynamoDBHelpers(LOCALSTACK.getHostClient());
    }

    @Test
    public void testBatchPutWithBeanConverter() throws Exception {
        dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);

        List<TestItem> items =
                Arrays.asList(
                        new TestItem("1", "a", "data1", "0"),
                        new TestItem("2", "b", "data2", "0"));

        StreamExecutionEnvironment env = createEnv();
        DataStream<TestItem> stream = env.fromCollection(items);
        stream.sinkTo(buildSink(new DynamoDbBeanElementConverter<>(TestItem.class)));
        env.execute("Batch PUT via BeanConverter");

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName)).isEqualTo(2);
    }

    @Test
    public void testUpdateWithBeanUpdateConverter() throws Exception {
        dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);

        List<TestItem> items =
                Arrays.asList(
                        new TestItem("1", "a", "data1", "0"),
                        new TestItem("2", "b", "data2", "0"));

        StreamExecutionEnvironment env = createEnv();
        DataStream<TestItem> putStream = env.fromCollection(items);
        putStream.sinkTo(buildSink(new DynamoDbBeanElementConverter<>(TestItem.class)));
        env.execute("Initial PUT");

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName)).isEqualTo(2);

        Map<String, String> expressionNames = Collections.singletonMap("#c", "counter");
        Map<String, AttributeValue> expressionValues =
                Collections.singletonMap(":val", AttributeValue.builder().s("42").build());

        env = createEnv();
        DataStream<TestItem> updateStream =
                env.fromCollection(
                        Collections.singletonList(new TestItem("1", "a", null, null)));
        updateStream.sinkTo(
                buildSink(
                        DynamoDbBeanElementConverter.builder(TestItem.class)
                                .setType(DynamoDbWriteRequestType.UPDATE)
                                .setUpdateExpression("SET #c = :val")
                                .setExpressionAttributeNames(expressionNames)
                                .setExpressionAttributeValues(expressionValues)
                                .build()));
        env.execute("UPDATE via BeanConverter");

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName)).isEqualTo(2);
        Assertions.assertThat(
                        dynamoDBHelpers.containsAttributeValue(testTableName, "counter", "42"))
                .isTrue();
    }

    @Test
    public void testConditionalPutWithBeanConverterPreventsOverwrite() throws Exception {
        dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);

        StreamExecutionEnvironment env = createEnv();
        DataStream<TestItem> putStream =
                env.fromCollection(
                        Collections.singletonList(new TestItem("1", "a", "original", "0")));
        putStream.sinkTo(buildSink(new DynamoDbBeanElementConverter<>(TestItem.class)));
        env.execute("Initial PUT");

        Map<String, String> exprNames = new HashMap<>();
        exprNames.put("#pk", PARTITION_KEY);
        exprNames.put("#sk", SORT_KEY);

        StreamExecutionEnvironment conditionalEnv = createEnv();
        DataStream<TestItem> conditionalStream =
                conditionalEnv.fromCollection(
                        Collections.singletonList(new TestItem("1", "a", "overwritten", "0")));
        conditionalStream.sinkTo(
                buildSink(
                        DynamoDbBeanElementConverter.builder(TestItem.class)
                                .setType(DynamoDbWriteRequestType.PUT)
                                .setConditionExpression(
                                        "attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                                .setExpressionAttributeNames(exprNames)
                                .build()));

        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(() -> conditionalEnv.execute("Conditional PUT should fail"))
                .havingCause()
                .havingCause()
                .withMessageContaining("conditional check");

        Assertions.assertThat(
                        dynamoDBHelpers.containsAttributeValue(testTableName, "payload", "original"))
                .isTrue();
    }

    @Test
    public void testConditionalDeleteWithBeanConverter() throws Exception {
        dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);

        List<TestItem> items =
                Arrays.asList(
                        new TestItem("1", "a", "keep", "0"),
                        new TestItem("2", "b", "remove", "0"));

        StreamExecutionEnvironment env = createEnv();
        DataStream<TestItem> putStream = env.fromCollection(items);
        putStream.sinkTo(buildSink(new DynamoDbBeanElementConverter<>(TestItem.class)));
        env.execute("Insert items");

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName)).isEqualTo(2);

        env = createEnv();
        DataStream<TestItem> deleteStream =
                env.fromCollection(
                        Collections.singletonList(new TestItem("2", "b", null, null)));
        deleteStream.sinkTo(
                buildSink(
                        DynamoDbBeanElementConverter.builder(TestItem.class)
                                .setType(DynamoDbWriteRequestType.DELETE)
                                .setConditionExpression("#p = :val")
                                .setExpressionAttributeNames(
                                        Collections.singletonMap("#p", "payload"))
                                .setExpressionAttributeValues(
                                        Collections.singletonMap(
                                                ":val",
                                                AttributeValue.builder()
                                                        .s("remove")
                                                        .build()))
                                .build()));
        env.execute("Conditional DELETE");

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName)).isEqualTo(1);
        Assertions.assertThat(
                        dynamoDBHelpers.containsAttributeValue(testTableName, "payload", "keep"))
                .isTrue();
    }

    private DynamoDbSink<TestItem> buildSink(
            ElementConverter<TestItem, DynamoDbWriteRequest> converter) {
        return DynamoDbSink.<TestItem>builder()
                .setElementConverter(converter)
                .setMaxTimeInBufferMS(1000)
                .setMaxInFlightRequests(1)
                .setMaxBatchSize(25)
                .setFailOnError(true)
                .setMaxBufferedRequests(1000)
                .setTableName(testTableName)
                .setOverwriteByPartitionKeys(Collections.emptyList())
                .setDynamoDbProperties(getProperties())
                .build();
    }

    private StreamExecutionEnvironment createEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY.getMainValue());
        env.configure(config);
        env.setParallelism(1);
        return env;
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(AWS_ENDPOINT, LOCALSTACK.getHostEndpointUrl());
        properties.setProperty(AWS_ACCESS_KEY_ID, LOCALSTACK.getAccessKey());
        properties.setProperty(AWS_SECRET_ACCESS_KEY, LOCALSTACK.getSecretKey());
        properties.setProperty(AWS_REGION, LOCALSTACK.getRegion().toString());
        properties.setProperty(TRUST_ALL_CERTIFICATES, "true");
        properties.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        return properties;
    }
}
