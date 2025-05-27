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

package org.apache.flink.connector.dynamodb.table.test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.dynamodb.table.DynamoDbDynamicSink;
import org.apache.flink.connector.dynamodb.testutils.DockerImageVersions;
import org.apache.flink.connector.dynamodb.testutils.DynamoDBHelpers;
import org.apache.flink.connector.dynamodb.testutils.DynamoDbContainer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/** Integration test for {@link DynamoDbDynamicSink}. */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class DynamoDbDynamicSinkITCase {
    private static final String PARTITION_KEY = "partition_key";
    private static final String SORT_KEY = "sort_key";
    private static DynamoDBHelpers dynamoDBHelpers;
    private static String testTableName;
    private static StreamExecutionEnvironment env;

    // shared between test methods
    @Container
    public static final DynamoDbContainer LOCALSTACK =
            new DynamoDbContainer(DockerImageName.parse(DockerImageVersions.DYNAMODB))
                    .withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb")
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
    public void testSQLSink() throws ExecutionException, InterruptedException, IOException {

        int expectedNumOfElements = 50;

        dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

        final String createTableStmt = getCreateTableStmt();
        streamTableEnvironment.executeSql(createTableStmt);

        final String datagenStmt = getDatagenStmt(expectedNumOfElements);
        streamTableEnvironment.executeSql(datagenStmt);

        final String insertSql = "INSERT INTO dynamo_db_table SELECT * from datagen;";
        streamTableEnvironment.executeSql(insertSql).await();

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName))
                .isEqualTo(expectedNumOfElements);
    }

    @Test
    public void testTableAPISink() throws ExecutionException, InterruptedException, IOException {

        int expectedNumOfElements = 50;

        dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        final String createTableStmt = getCreateTableStmt();
        tEnv.executeSql(createTableStmt);

        final String datagenStmt = getDatagenStmt(expectedNumOfElements);
        tEnv.executeSql(datagenStmt);

        Table resultTable = tEnv.from("datagen");

        resultTable.executeInsert("dynamo_db_table").await();

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName))
                .isEqualTo(expectedNumOfElements);
    }

    private static String getDatagenStmt(Integer expectedNumOfElements) throws IOException {
        String datagenStmt =
                IOUtils.toString(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("datagen.sql")),
                        StandardCharsets.UTF_8);

        Map valuesMap = new HashMap<>();
        valuesMap.put("expectedNumOfElements", expectedNumOfElements.toString());
        return new StrSubstitutor(valuesMap).replace(datagenStmt);
    }

    private static String getCreateTableStmt() throws IOException {
        String createTableStmt =
                IOUtils.toString(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("create-table.sql")),
                        StandardCharsets.UTF_8);

        Map valuesMap = new HashMap<>();
        valuesMap.put("region", LOCALSTACK.getRegion().toString());
        valuesMap.put("endpoint", LOCALSTACK.getHostEndpointUrl());
        valuesMap.put("tableName", testTableName);
        valuesMap.put("accessKey", LOCALSTACK.getAccessKey());
        valuesMap.put("secretKey", LOCALSTACK.getSecretKey());

        return new StrSubstitutor(valuesMap).replace(createTableStmt);
    }
}
