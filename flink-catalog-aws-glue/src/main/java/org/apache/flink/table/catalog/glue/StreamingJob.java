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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example job demonstrating the usage of the AWS Glue Catalog with Flink SQL.
 * This job creates a Glue catalog and executes various SQL operations.
 */
public class StreamingJob {

    // Logger to log messages for debugging or info
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // Create a new Flink configuration object
        Configuration configuration = new Configuration();

        // Create a local streaming execution environment with web UI for monitoring
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // Create the TableEnvironment using the provided execution environment and settings
        TableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().build());

        tEnv.executeSql("SHOW TABLES").print();
        tEnv.executeSql("SHOW DATABASES").print();
        // Register the Glue catalog using SQL
        tEnv.executeSql(
                "CREATE CATALOG glue_catalog WITH (" +
                        "'type' = 'glue', " +
                        "'region' = 'us-east-1', " +
                        "'default-database' = 'default' " +
                        ")"
        );
        tEnv.executeSql("USE CATALOG glue_catalog;").print();

        tEnv.executeSql("DROP DATABASE IF EXISTS test").print();
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test").print();

        tEnv.executeSql("SHOW DATABASES").print();

        tEnv.executeSql("USE test").print();

        tEnv.executeSql("SHOW TABLES").print();

        // Create a new table 'fran' with specified schema and configuration
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS gen (" +
                "  `order_Number` BIGINT," +
                "  `price` DECIMAL(32,2)," +
                "  `order_Time` TIMESTAMP(3) " +
                ")" +
                "WITH (" +
                "  'connector' = 'datagen'" +
                ");").print();

        tEnv.executeSql("SHOW TABLES").print();

        // =========================================================================
        // TEST CASE-SENSITIVITY EXAMPLES - Uncomment one at a time to test
        // These examples test how the GlueCatalog handles column name case sensitivity
        // =========================================================================

        // NOTE: AWS Glue automatically converts column names to lowercase, but our connector
        // preserves the original case in column properties for accurate JSON parsing.

        // -------------------------------------------------------------------------
        // EXAMPLE 1: ALL LOWERCASE COLUMNS
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_lowercase (" +
//                 "  `id` INT," +
//                 "  `username` VARCHAR(255)," +
//                 "  `timestamp` TIMESTAMP(3)," +
//                 "  `data_value` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT * FROM case_test_lowercase").print();
//
        // -------------------------------------------------------------------------
        // EXAMPLE 2: ALL UPPERCASE COLUMNS
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_uppercase (" +
//                 "  `ID` INT," +
//                 "  `USERNAME` VARCHAR(255)," +
//                 "  `TIMESTAMP` TIMESTAMP(3)," +
//                 "  `DATA_VALUE` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT ID FROM case_test_uppercase limit 10").print();

        // -------------------------------------------------------------------------
        // EXAMPLE 3: MIXED CASE COLUMNS (camelCase)
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_mixed (" +
//                 "  `userId` INT," +
//                 "  `userName` VARCHAR(255)," +
//                 "  `eventTimestamp` TIMESTAMP(3)," +
//                 "  `dataValue` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT userId FROM case_test_mixed").print();

        // -------------------------------------------------------------------------
        // EXAMPLE 4: MIXED CASE COLUMNS (PascalCase)
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_pascal (" +
//                 "  `UserId` INT," +
//                 "  `UserName` VARCHAR(255)," +
//                 "  `EventTimestamp` TIMESTAMP(3)," +
//                 "  `DataValue` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT UserId FROM case_test_pascal limit 5").print();

        // -------------------------------------------------------------------------
//         EXAMPLE 5: JSON SOURCE TEST - Tests real JSON parsing with case-sensitive fields
//         -------------------------------------------------------------------------
         // First create a test file with JSON data that has case-sensitive field names
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS json_test (" +
//                 "  `UserId` INT," +
//                 "  `UserName` VARCHAR(255)," +
//                 "  `Timestamp` TIMESTAMP(3)," +
//                 "  `DATA_VALUE` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'filesystem'," +
//                 "  'path' = '/tmp/json_test.json'," +
//                 "  'format' = 'json'" +
//                 ");").print();
//
//         // Create the file manually with:
//         // echo '{"UserId":1,"UserName":"john","Timestamp":"2023-04-01 12:00:00","DATA_VALUE":"test"}' > /tmp/json_test.json
//         // echo '{"UserId":2,"UserName":"jane","Timestamp":"2023-04-01 12:01:00","DATA_VALUE":"test2"}' >> /tmp/json_test.json
//
//         // Query the data to verify case-sensitivity handling
//         tEnv.executeSql("SELECT UserId FROM json_test").print();

        // -------------------------------------------------------------------------
        // EXAMPLE 6: NESTED JSON TEST - Tests case sensitivity in nested structures
        // -------------------------------------------------------------------------
        // // Create a table with a complex nested structure using different case styles
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS nested_json_test (" +
//                 "  `Id` INT," +
//                 "  `UserProfile` ROW<" +
//                 "     `FirstName` VARCHAR(255), " +
//                 "     `lastName` VARCHAR(255)" +
//                 "  >," +
//                 "  `EventData` ROW<" +
//                 "     `EventType` VARCHAR(50)," +
//                 "     `eventTimestamp` TIMESTAMP(3)," +
//                 "      `METADATA` MAP<VARCHAR(100), VARCHAR(255)>>" +
    }
}
