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

package org.apache.flink.connector.sqs.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Factory for creating configured instances of {@link SqsDynamicSink}. */
@Internal
public class SqsDynamicTableFactory extends AsyncDynamicTableSinkFactory {
    private static final String IDENTIFIER = "sqs";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);
        factoryContext.getFactoryHelper().validate();
        ReadableConfig config = factoryContext.getTableOptions();
        Properties clientProperties = getSqsClientProperties(config);
        AWSGeneralUtil.validateAwsConfiguration(clientProperties);

        SqsDynamicSink.SqsDynamicSinkBuilder builder =
                SqsDynamicSink.builder()
                        .setSqsQueueUrl(config.get(SqsConnectorOptions.QUEUE_URL))
                        .setEncodingFormat(factoryContext.getEncodingFormat())
                        .setSqsClientProperties(clientProperties)
                        .setConsumedDataType(factoryContext.getPhysicalDataType())
                        .setFailOnError(config.get(SqsConnectorOptions.FAIL_ON_ERROR));

        addAsyncOptionsToBuilder(getAsyncSinkOptions(config), builder);
        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SqsConnectorOptions.QUEUE_URL);
        options.add(SqsConnectorOptions.AWS_REGION);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = super.optionalOptions();
        options.add(SqsConnectorOptions.FAIL_ON_ERROR);
        options.add(SqsConnectorOptions.AWS_CONFIG_PROPERTIES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SqsConnectorOptions.QUEUE_URL);
        options.add(SqsConnectorOptions.AWS_REGION);
        options.add(SqsConnectorOptions.AWS_CONFIG_PROPERTIES);
        return options;
    }

    private Properties getSqsClientProperties(ReadableConfig config) {
        Properties properties = new Properties();
        properties.putAll(
                appendAwsPrefixToOptions(config.get(SqsConnectorOptions.AWS_CONFIG_PROPERTIES)));
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
