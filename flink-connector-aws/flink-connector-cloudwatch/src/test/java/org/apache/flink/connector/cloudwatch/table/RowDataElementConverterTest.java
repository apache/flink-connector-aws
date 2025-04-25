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

package org.apache.flink.connector.cloudwatch.table;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.cloudwatch.sink.MetricWriteRequest;
import org.apache.flink.connector.cloudwatch.utils.TestConstants;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.AWS_REGION;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_COUNT_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_DIMENSION_KEYS;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_NAMESPACE;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_NAME_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_STATISTIC_MAX_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_STATISTIC_MIN_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_STATISTIC_SAMPLE_COUNT_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_STATISTIC_SUM_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_STORAGE_RESOLUTION_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_TIMESTAMP_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_UNIT_KEY;
import static org.apache.flink.connector.cloudwatch.table.CloudWatchConnectorOptions.METRIC_VALUE_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.DATA_TYPE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_COUNT_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_VALUE_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_VALUE_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_METRIC_NAME;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_NAMESPACE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_REGION;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_COUNT;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_TS_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_COUNT_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_COUNT_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MAX_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MAX_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MIN_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MIN_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_SUM_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_SUM_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STORAGE_RESOLUTION_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STORAGE_RESOLUTION_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_TIMESTAMP_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_UNIT_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_UNIT_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_VALUE_KEY;
import static org.assertj.core.api.Assertions.assertThat;

class RowDataElementConverterTest {
    private CloudWatchTableConfig cloudWatchTableConfig;
    private RowDataElementConverter elementConverter;
    private static final SinkWriter.Context context = new UnusedSinkWriterContext();

    @BeforeEach
    void setUp() {
        TableConfig tableConfig = TableConfig.getDefault();

        tableConfig.set(METRIC_NAMESPACE, TEST_NAMESPACE);
        tableConfig.set(METRIC_NAME_KEY, TEST_METRIC_NAME);
        tableConfig.set(
                METRIC_DIMENSION_KEYS, Arrays.asList(TEST_DIMENSION_KEY_1, TEST_DIMENSION_KEY_2));
        tableConfig.set(METRIC_VALUE_KEY, TEST_VALUE_KEY);
        tableConfig.set(METRIC_COUNT_KEY, TEST_COUNT_KEY);
        tableConfig.set(METRIC_TIMESTAMP_KEY, TEST_TIMESTAMP_KEY);
        tableConfig.set(METRIC_UNIT_KEY, TEST_UNIT_KEY);
        tableConfig.set(METRIC_STORAGE_RESOLUTION_KEY, TEST_STORAGE_RESOLUTION_KEY);
        tableConfig.set(METRIC_STATISTIC_MAX_KEY, TEST_STATS_MAX_KEY);
        tableConfig.set(METRIC_STATISTIC_MIN_KEY, TEST_STATS_MIN_KEY);
        tableConfig.set(METRIC_STATISTIC_SUM_KEY, TEST_STATS_SUM_KEY);
        tableConfig.set(METRIC_STATISTIC_SAMPLE_COUNT_KEY, TEST_STATS_COUNT_KEY);
        tableConfig.set(AWS_REGION, TEST_REGION);

        cloudWatchTableConfig = new CloudWatchTableConfig(tableConfig);
        elementConverter = new RowDataElementConverter(DATA_TYPE, cloudWatchTableConfig);
    }

    @Test
    void testApply() {
        RowData rowData = createElement();
        MetricWriteRequest actualTimeSeries = elementConverter.apply(rowData, context);
        MetricWriteRequest expectedTimeSeries =
                MetricWriteRequest.builder()
                        .withMetricName(TEST_METRIC_NAME)
                        .addDimension(TEST_DIMENSION_KEY_1, TEST_DIMENSION_VALUE_1)
                        .addDimension(TEST_DIMENSION_KEY_2, TEST_DIMENSION_VALUE_2)
                        .addValue(TEST_SAMPLE_VALUE)
                        .addCount(TEST_SAMPLE_COUNT)
                        .withTimestamp(Instant.ofEpochMilli(TEST_SAMPLE_TS_VALUE))
                        .withUnit(TEST_UNIT_VALUE)
                        .withStorageResolution(TEST_STORAGE_RESOLUTION_VALUE)
                        .withStatisticMax(TEST_STATS_MAX_VALUE)
                        .withStatisticMin(TEST_STATS_MIN_VALUE)
                        .withStatisticSum(TEST_STATS_SUM_VALUE)
                        .withStatisticCount(TEST_STATS_COUNT_VALUE)
                        .build();

        assertThat(actualTimeSeries).usingRecursiveComparison().isEqualTo(expectedTimeSeries);
    }

    private static RowData createElement() {
        GenericRowData element = new GenericRowData(RowKind.INSERT, 12);
        element.setField(0, StringData.fromString(TestConstants.TEST_METRIC_NAME));
        element.setField(1, StringData.fromString(TestConstants.TEST_DIMENSION_VALUE_1));
        element.setField(2, StringData.fromString(TestConstants.TEST_DIMENSION_VALUE_2));
        element.setField(3, TestConstants.TEST_SAMPLE_VALUE);
        element.setField(4, TestConstants.TEST_SAMPLE_COUNT);
        element.setField(5, TimestampData.fromEpochMillis(TestConstants.TEST_SAMPLE_TS_VALUE));
        element.setField(6, StringData.fromString(TestConstants.TEST_UNIT_VALUE));
        element.setField(7, TestConstants.TEST_STORAGE_RESOLUTION_VALUE);
        element.setField(8, TestConstants.TEST_STATS_MAX_VALUE);
        element.setField(9, TestConstants.TEST_STATS_MIN_VALUE);
        element.setField(10, TestConstants.TEST_STATS_SUM_VALUE);
        element.setField(11, TestConstants.TEST_STATS_COUNT_VALUE);
        return element;
    }

    private static class UnusedSinkWriterContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long timestamp() {
            throw new UnsupportedOperationException();
        }
    }
}
