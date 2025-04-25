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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/** Factory for creating configured instances of {@link CloudWatchDynamicSink}. */
@Internal
public class CloudWatchDynamicTableFactory extends AsyncDynamicTableSinkFactory {
    static final String IDENTIFIER = "cloudwatch";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper factoryHelper =
                FactoryUtil.createTableFactoryHelper(this, context);
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();

        FactoryUtil.validateFactoryOptions(this, factoryHelper.getOptions());

        Properties clientProperties = getCloudWatchClientProperties(factoryHelper.getOptions());
        AWSGeneralUtil.validateAwsConfiguration(clientProperties);

        CloudWatchTableConfig cloudWatchTableConfig =
                new CloudWatchTableConfig(factoryHelper.getOptions());

        CloudWatchDynamicSink.CloudWatchDynamicSinkBuilder builder =
                CloudWatchDynamicSink.builder()
                        .setCloudWatchMetricNamespace(cloudWatchTableConfig.getNamespace())
                        .setCloudWatchTableConfig(cloudWatchTableConfig)
                        .setCloudWatchClientProperties(clientProperties)
                        .setPhysicalDataType(physicalDataType)
                        .setInvalidMetricDataRetryMode(
                                factoryHelper
                                        .getOptions()
                                        .get(
                                                CloudWatchConnectorOptions
                                                        .INVALID_METRIC_DATA_RETRY_MODE));

        addAsyncOptionsToBuilder(getAsyncSinkOptions(factoryHelper.getOptions()), builder);
        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CloudWatchConnectorOptions.AWS_REGION);
        options.add(CloudWatchConnectorOptions.METRIC_NAMESPACE);
        options.add(CloudWatchConnectorOptions.METRIC_NAME_KEY);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = super.optionalOptions();
        options.add(CloudWatchConnectorOptions.INVALID_METRIC_DATA_RETRY_MODE);
        options.add(CloudWatchConnectorOptions.AWS_CONFIG_PROPERTIES);

        options.add(CloudWatchConnectorOptions.METRIC_VALUE_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_COUNT_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_DIMENSION_KEYS);
        options.add(CloudWatchConnectorOptions.METRIC_STORAGE_RESOLUTION_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_TIMESTAMP_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_UNIT_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_STATISTIC_MAX_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_STATISTIC_MIN_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_STATISTIC_SUM_KEY);
        options.add(CloudWatchConnectorOptions.METRIC_STATISTIC_SAMPLE_COUNT_KEY);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CloudWatchConnectorOptions.AWS_REGION);
        options.add(CloudWatchConnectorOptions.AWS_CONFIG_PROPERTIES);
        return options;
    }

    private Properties getCloudWatchClientProperties(ReadableConfig config) {
        Properties properties = new Properties();
        properties.putAll(
                appendAwsPrefixToOptions(
                        config.get(CloudWatchConnectorOptions.AWS_CONFIG_PROPERTIES)));

        return properties;
    }

    private Map<String, String> appendAwsPrefixToOptions(Map<String, String> options) {
        Map<String, String> prefixedProperties = new HashMap<>();
        options.forEach((key, value) -> prefixedProperties.put("aws" + "." + key, value));
        return prefixedProperties;
    }

    private Properties getAsyncSinkOptions(ReadableConfig config) {
        Properties properties = new Properties();
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE))
                .ifPresent(
                        flushBufferSize ->
                                properties.put(
                                        AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE.key(),
                                        flushBufferSize));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.MAX_BATCH_SIZE))
                .ifPresent(
                        maxBatchSize ->
                                properties.put(
                                        AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key(),
                                        maxBatchSize));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS))
                .ifPresent(
                        maxInflightRequests ->
                                properties.put(
                                        AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key(),
                                        maxInflightRequests));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS))
                .ifPresent(
                        maxBufferedRequests ->
                                properties.put(
                                        AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS.key(),
                                        maxBufferedRequests));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT))
                .ifPresent(
                        timeout ->
                                properties.put(
                                        AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key(),
                                        timeout));
        return properties;
    }
}
