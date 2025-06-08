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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.cloudwatch.sink.CloudWatchSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
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
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEYS;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_METRIC_NAME;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_NAMESPACE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_REGION;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_COUNT_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MAX_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MIN_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_SUM_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STORAGE_RESOLUTION_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_TIMESTAMP_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_UNIT_KEY;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_VALUE_KEY;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class CloudWatchDynamicTableFactoryTest {
    @Test
    void testGoodTableSink() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptions().build();

        // Construct actual sink
        CloudWatchDynamicSink actualSink =
                (CloudWatchDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected sink
        Properties cloudWatchClientProperties = new Properties();
        cloudWatchClientProperties.setProperty(AWS_REGION.key(), TEST_REGION);

        CloudWatchDynamicSink expectedSink =
                (CloudWatchDynamicSink)
                        CloudWatchDynamicSink.builder()
                                .setCloudWatchMetricNamespace(TEST_NAMESPACE)
                                .setCloudWatchTableConfig(
                                        new CloudWatchTableConfig(
                                                Configuration.fromMap(sinkOptions)))
                                .setCloudWatchClientProperties(cloudWatchClientProperties)
                                .setPhysicalDataType(DATA_TYPE)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
        assertThat(actualSink.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.upsert());

        Sink<RowData> createdSink =
                ((SinkV2Provider)
                                actualSink.getSinkRuntimeProvider(
                                        new SinkRuntimeProviderContext(false)))
                        .createSink();
        assertThat(createdSink).isInstanceOf(CloudWatchSink.class);
    }

    @Test
    void testGoodTableSinkWithAsyncOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption(MAX_BATCH_SIZE, "100")
                        .withTableOption(MAX_IN_FLIGHT_REQUESTS, "200")
                        .withTableOption(MAX_BUFFERED_REQUESTS, "300")
                        .withTableOption(FLUSH_BUFFER_SIZE, "1024")
                        .withTableOption(FLUSH_BUFFER_TIMEOUT, "1000")
                        .build();

        // Construct actual sink
        CloudWatchDynamicSink actualSink =
                (CloudWatchDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected sink
        Properties cloudWatchClientProperties = new Properties();
        cloudWatchClientProperties.setProperty(AWS_REGION.key(), TEST_REGION);

        CloudWatchDynamicSink expectedSink =
                (CloudWatchDynamicSink)
                        CloudWatchDynamicSink.builder()
                                .setCloudWatchMetricNamespace(TEST_NAMESPACE)
                                .setCloudWatchTableConfig(
                                        new CloudWatchTableConfig(
                                                Configuration.fromMap(sinkOptions)))
                                .setCloudWatchClientProperties(cloudWatchClientProperties)
                                .setMaxBatchSize(100)
                                .setMaxInFlightRequests(200)
                                .setMaxBufferedRequests(300)
                                .setMaxBufferSizeInBytes(1024)
                                .setMaxTimeInBufferMS(1000)
                                .setPhysicalDataType(DATA_TYPE)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
        assertThat(actualSink.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.upsert());

        Sink<RowData> createdSink =
                ((SinkV2Provider)
                                actualSink.getSinkRuntimeProvider(
                                        new SinkRuntimeProviderContext(false)))
                        .createSink();
        assertThat(createdSink).isInstanceOf(CloudWatchSink.class);
    }

    @Test
    void testBadTableSinkWithoutRequiredOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                new TableOptionsBuilder(
                                CloudWatchDynamicTableFactory.IDENTIFIER,
                                TestFormatFactory.IDENTIFIER)
                        .build();

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, Collections.emptyList(), sinkOptions))
                .havingCause()
                .withMessageContaining("One or more required options are missing.")
                .withMessageContaining(METRIC_NAMESPACE.key())
                .withMessageContaining(METRIC_NAME_KEY.key())
                .withMessageContaining(AWS_REGION.key());
    }

    private ResolvedSchema defaultSinkSchema() {
        return ResolvedSchema.of(
                Column.physical(TEST_METRIC_NAME, DataTypes.STRING()),
                Column.physical(TEST_DIMENSION_KEY_1, DataTypes.STRING()),
                Column.physical(TEST_DIMENSION_KEY_2, DataTypes.STRING()),
                Column.physical(TEST_VALUE_KEY, DataTypes.DOUBLE()),
                Column.physical(TEST_COUNT_KEY, DataTypes.DOUBLE()),
                Column.physical(TEST_TIMESTAMP_KEY, DataTypes.TIMESTAMP(6)),
                Column.physical(TEST_UNIT_KEY, DataTypes.STRING()),
                Column.physical(TEST_STORAGE_RESOLUTION_KEY, DataTypes.INT()),
                Column.physical(TEST_STATS_MAX_KEY, DataTypes.DOUBLE()),
                Column.physical(TEST_STATS_MIN_KEY, DataTypes.DOUBLE()),
                Column.physical(TEST_STATS_SUM_KEY, DataTypes.DOUBLE()),
                Column.physical(TEST_STATS_COUNT_KEY, DataTypes.DOUBLE()));
    }

    private TableOptionsBuilder defaultTableOptions() {
        String connector = CloudWatchDynamicTableFactory.IDENTIFIER;
        String format = TestFormatFactory.IDENTIFIER;
        return new TableOptionsBuilder(connector, format)
                // default table options
                .withTableOption(METRIC_NAMESPACE, TEST_NAMESPACE)
                .withTableOption(METRIC_NAME_KEY, TEST_METRIC_NAME)
                .withTableOption(METRIC_DIMENSION_KEYS, TEST_DIMENSION_KEYS)
                .withTableOption(METRIC_VALUE_KEY, TEST_VALUE_KEY)
                .withTableOption(METRIC_COUNT_KEY, TEST_COUNT_KEY)
                .withTableOption(METRIC_TIMESTAMP_KEY, TEST_TIMESTAMP_KEY)
                .withTableOption(METRIC_UNIT_KEY, TEST_UNIT_KEY)
                .withTableOption(METRIC_STORAGE_RESOLUTION_KEY, TEST_STORAGE_RESOLUTION_KEY)
                .withTableOption(METRIC_STATISTIC_MAX_KEY, TEST_STATS_MAX_KEY)
                .withTableOption(METRIC_STATISTIC_MIN_KEY, TEST_STATS_MIN_KEY)
                .withTableOption(METRIC_STATISTIC_SUM_KEY, TEST_STATS_SUM_KEY)
                .withTableOption(METRIC_STATISTIC_SAMPLE_COUNT_KEY, TEST_STATS_COUNT_KEY)
                .withTableOption(AWS_REGION, TEST_REGION);
    }
}
