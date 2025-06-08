--/*
-- * Licensed to the Apache Software Foundation (ASF) under one
-- * or more contributor license agreements.  See the NOTICE file
-- * distributed with this work for additional information
-- * regarding copyright ownership.  The ASF licenses this file
-- * to you under the Apache License, Version 2.0 (the
-- * "License"); you may not use this file except in compliance
-- * with the License.  You may obtain a copy of the License at
-- *
-- *     http://www.apache.org/licenses/LICENSE-2.0
-- *
-- * Unless required by applicable law or agreed to in writing, software
-- * distributed under the License is distributed on an "AS IS" BASIS,
-- * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- * See the License for the specific language governing permissions and
-- * limitations under the License.
-- */

CREATE TABLE CloudWatchTable
(
    `my_metric_name`   STRING,
    `${dimension_key_1}` STRING,
    `${dimension_key_2}` STRING,
    `sample_value` DOUBLE,
    `sample_count` DOUBLE,
    `unit`             STRING,
    `storage_res`      INT,
    `stats_max` DOUBLE,
    `stats_min` DOUBLE,
    `stats_sum` DOUBLE,
    `stats_count` DOUBLE
)
    WITH (
        'connector' = 'cloudwatch',
        'aws.region' = 'ap-southeast-1',
        'aws.endpoint' = 'https://localstack:4566',
        'aws.credentials.provider' = 'BASIC',
        'aws.credentials.provider.basic.accesskeyid' = 'accessKeyId',
        'aws.credentials.provider.basic.secretkey' = 'secretAccessKey',
        'aws.trust.all.certificates' = 'true',
        'sink.http-client.protocol.version' = 'HTTP1_1',
        'sink.batch.max-size' = '1',
        'metric.namespace' = '${namespace}',
        'metric.name.key' = 'my_metric_name',
        'metric.dimension.keys' = '${dimension_keys}',
        'metric.value.key' = 'sample_value',
        'metric.count.key' = 'sample_count',
        'metric.unit.key' = 'unit',
        'metric.storage-resolution.key' = 'storage_res',
        'metric.statistic.max.key' = 'stats_max',
        'metric.statistic.min.key' = 'stats_min',
        'metric.statistic.sum.key' = 'stats_sum',
        'metric.statistic.sample-count.key' = 'stats_count',
        'sink.invalid-metric.retry-mode' = 'RETRY'
        );

INSERT INTO CloudWatchTable
VALUES ('${metric_name}', '${dimension_val_1}', '${dimension_val_2}', 123, 1, 'Seconds', 60, 999, 1, 10, 11);