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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.cloudwatch.sink.InvalidMetricDataRetryMode;

import java.util.List;
import java.util.Map;

/** Options for the CloudWatch connector. */
@PublicEvolving
public class CloudWatchConnectorOptions {
    public static final ConfigOption<String> AWS_REGION =
            ConfigOptions.key("aws.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region of used Cloudwatch metric.");

    public static final ConfigOption<Map<String, String>> AWS_CONFIG_PROPERTIES =
            ConfigOptions.key("aws")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("AWS configuration properties.");

    public static final ConfigOption<String> METRIC_NAMESPACE =
            ConfigOptions.key("metric.namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric namespace.");

    public static final ConfigOption<String> METRIC_NAME_KEY =
            ConfigOptions.key("metric.name.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric name.");

    public static final ConfigOption<List<String>> METRIC_DIMENSION_KEYS =
            ConfigOptions.key("metric.dimension.keys")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric dimension key name.");

    public static final ConfigOption<String> METRIC_VALUE_KEY =
            ConfigOptions.key("metric.value.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric value key.");

    public static final ConfigOption<String> METRIC_COUNT_KEY =
            ConfigOptions.key("metric.count.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric count key.");

    public static final ConfigOption<String> METRIC_UNIT_KEY =
            ConfigOptions.key("metric.unit.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric unit key.");

    public static final ConfigOption<String> METRIC_STORAGE_RESOLUTION_KEY =
            ConfigOptions.key("metric.storage-resolution.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric storage resolution key.");

    public static final ConfigOption<String> METRIC_TIMESTAMP_KEY =
            ConfigOptions.key("metric.timestamp.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric sample timestamp.");

    public static final ConfigOption<String> METRIC_STATISTIC_MAX_KEY =
            ConfigOptions.key("metric.statistic.max.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric statistic max key.");

    public static final ConfigOption<String> METRIC_STATISTIC_MIN_KEY =
            ConfigOptions.key("metric.statistic.min.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric statistic min key.");

    public static final ConfigOption<String> METRIC_STATISTIC_SUM_KEY =
            ConfigOptions.key("metric.statistic.sum.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric statistic sum key.");

    public static final ConfigOption<String> METRIC_STATISTIC_SAMPLE_COUNT_KEY =
            ConfigOptions.key("metric.statistic.sample-count.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CloudWatch metric statistic sample count key.");

    public static final ConfigOption<InvalidMetricDataRetryMode> INVALID_METRIC_DATA_RETRY_MODE =
            ConfigOptions.key("sink.invalid-metric.retry-mode")
                    .enumType(InvalidMetricDataRetryMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Retry mode when an invalid CloudWatch Metric is encountered.");
}
