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

package org.apache.flink.connector.dynamodb.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.dynamodb.testutils.DynamoDBHelpers;
import org.apache.flink.connector.dynamodb.testutils.DynamoDbContainer;
import org.apache.flink.connector.dynamodb.util.DockerImageVersions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

/** Integration test for {@link org.apache.flink.connector.dynamodb.table.DynamoDbDynamicSink}. */
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
    public void testSQLSink() throws ExecutionException, InterruptedException {

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
    public void testTableAPISink() throws ExecutionException, InterruptedException {

        int expectedNumOfElements = 50;

        dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

        final String createTableStmt = getCreateTableStmt();
        streamTableEnvironment.executeSql(createTableStmt);

        final String datagenStmt = getDatagenStmt(expectedNumOfElements);
        streamTableEnvironment.executeSql(datagenStmt);

        Table resultTable = streamTableEnvironment.sqlQuery("SELECT * FROM datagen");

        resultTable.executeInsert("dynamo_db_table").await();

        Assertions.assertThat(dynamoDBHelpers.getItemsCount(testTableName))
                .isEqualTo(expectedNumOfElements);
    }

    private static String getDatagenStmt(int expectedNumOfElements) {
        return "CREATE TEMPORARY TABLE datagen\n"
                + "WITH (\n"
                + "    'connector' = 'datagen',\n"
                + "    'number-of-rows' = '"
                + expectedNumOfElements
                + "'\n"
                + ")\n"
                + "LIKE dynamo_db_table (EXCLUDING ALL);";
    }

    private static String getCreateTableStmt() {
        return "CREATE TABLE dynamo_db_table (\n"
                + "    `partition_key` STRING,\n"
                + "    `sort_key` STRING,\n"
                + "    `some_char` CHAR,\n"
                + "    `some_varchar` VARCHAR,\n"
                + "    `some_string` STRING,\n"
                + "    `some_boolean` BOOLEAN,\n"
                + "    `some_decimal` DECIMAL,\n"
                + "    `some_tinyint` TINYINT,\n"
                + "    `some_smallint` SMALLINT,\n"
                + "    `some_int` INT,\n"
                + "    `some_bigint` BIGINT,\n"
                + "    `some_float` FLOAT,\n"
                + "    `some_date` DATE,\n"
                + "    `some_time` TIME,\n"
                + "    `some_timestamp` TIMESTAMP(3),\n"
                + "    `some_timestamp_ltz` TIMESTAMP_LTZ(5),\n"
                + "    `some_char_array` ARRAY<CHAR>,\n"
                + "    `some_varchar_array` ARRAY<VARCHAR>,\n"
                + "    `some_string_array` ARRAY<STRING>,\n"
                + "    `some_boolean_array` ARRAY<BOOLEAN>,\n"
                + "    `some_decimal_array` ARRAY<DECIMAL>,\n"
                + "    `some_tinyint_array` ARRAY<TINYINT>,\n"
                + "    `some_smallint_array` ARRAY<SMALLINT>,\n"
                + "    `some_int_array` ARRAY<INT>,\n"
                + "    `some_bigint_array` ARRAY<BIGINT>,\n"
                + "    `some_float_array` ARRAY<FLOAT>,\n"
                + "    `some_date_array` ARRAY<DATE>,\n"
                + "    `some_time_array` ARRAY<TIME>,\n"
                + "    `some_timestamp_array` ARRAY<TIMESTAMP(3)>,\n"
                + "    `some_timestamp_ltz_array` ARRAY<TIMESTAMP_LTZ(5)>,\n"
                + "    `some_string_map` MAP<STRING,STRING>,\n"
                + "    `some_boolean_map` MAP<STRING,BOOLEAN>\n"
                + ") PARTITIONED BY ( partition_key )\n"
                + "    WITH (\n"
                + "        'connector' = 'dynamodb',\n"
                + "        'table-name' = '"
                + testTableName
                + "',\n"
                + "        'aws.region' = '"
                + LOCALSTACK.getRegion().toString()
                + "',\n"
                + "        'aws.endpoint' = '"
                + LOCALSTACK.getHostEndpointUrl()
                + "',\n"
                + "        '"
                + AWS_CREDENTIALS_PROVIDER
                + "' = 'BASIC',\n"
                + "        'aws.credentials.basic.accesskeyid' = '"
                + LOCALSTACK.getAccessKey()
                + "',\n"
                + "        'aws.credentials.basic.secretkey' = '"
                + LOCALSTACK.getSecretKey()
                + "',\n"
                + "        '"
                + TRUST_ALL_CERTIFICATES
                + "' = 'true',\n"
                + "        '"
                + HTTP_PROTOCOL_VERSION
                + "' = 'HTTP1_1'\n"
                + "        );";
    }
}
