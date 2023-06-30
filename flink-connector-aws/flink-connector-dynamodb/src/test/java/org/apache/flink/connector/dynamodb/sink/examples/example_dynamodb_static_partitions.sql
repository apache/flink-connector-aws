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

CREATE TABLE dynamo_db_table (
    `partition_key` STRING,
    `sort_key` STRING,
    `some_char` CHAR,
    `some_varchar` VARCHAR,
    `some_string` STRING,
    `some_boolean` BOOLEAN,
    `some_decimal` DECIMAL,
    `some_tinyint` TINYINT,
    `some_smallint` SMALLINT,
    `some_int` INT,
    `some_bigint` BIGINT,
    `some_float` FLOAT,
    `some_date` DATE,
    `some_time` TIME,
    `some_timestamp` TIMESTAMP(3),
    `some_timestamp_ltz` TIMESTAMP_LTZ(5),
    `some_char_array` ARRAY<CHAR>,
    `some_varchar_array` ARRAY<VARCHAR>,
    `some_string_array` ARRAY<STRING>,
    `some_boolean_array` ARRAY<BOOLEAN>,
    `some_decimal_array` ARRAY<DECIMAL>,
    `some_tinyint_array` ARRAY<TINYINT>,
    `some_smallint_array` ARRAY<SMALLINT>,
    `some_int_array` ARRAY<INT>,
    `some_bigint_array` ARRAY<BIGINT>,
    `some_float_array` ARRAY<FLOAT>,
    `some_date_array` ARRAY<DATE>,
    `some_time_array` ARRAY<TIME>,
    `some_timestamp_array` ARRAY<TIMESTAMP(3)>,
    `some_timestamp_ltz_array` ARRAY<TIMESTAMP_LTZ(5)>,
    `some_string_map` MAP<STRING,STRING>,
    `some_boolean_map` MAP<STRING,BOOLEAN>
) PARTITIONED BY ( partition_key, sort_key )
    WITH (
        'connector' = 'dynamodb',
        'table-name' = 'DynamoDBSinkWithKeys',
        'aws.region' = 'us-east-1'
        );

-- create a bounded mock table
CREATE TEMPORARY TABLE datagen
WITH (
    'connector' = 'datagen',
    'number-of-rows' = '50'
)
LIKE dynamo_db_table (EXCLUDING ALL);

INSERT INTO dynamo_db_table PARTITION (partition_key='123',sort_key='345') SELECT `some_char`,`some_varchar`,`some_string`,`some_boolean`,`some_decimal`,`some_tinyint`,`some_smallint`,`some_int`,`some_bigint`,`some_float`,`some_date`,`some_time`,`some_timestamp`,`some_timestamp_ltz`,`some_char_array`,`some_varchar_array`,`some_string_array`,`some_boolean_array`,`some_decimal_array`,`some_tinyint_array`,`some_smallint_array`,`some_int_array`,`some_bigint_array`,`some_float_array`,`some_date_array`,`some_time_array`,`some_timestamp_array`,`some_timestamp_ltz_array`,`some_string_map`,`some_boolean_map` from datagen;