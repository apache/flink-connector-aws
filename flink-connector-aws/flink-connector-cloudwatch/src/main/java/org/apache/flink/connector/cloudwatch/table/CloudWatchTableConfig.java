/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.connector.cloudwatch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

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

/** CloudWatch specific configuration. */
@Internal
public class CloudWatchTableConfig implements Serializable {
    private final ReadableConfig options;

    public CloudWatchTableConfig(ReadableConfig options) {
        this.options = options;
    }

    public String getNamespace() {
        return options.get(METRIC_NAMESPACE);
    }

    public String getMetricName() {
        return options.get(METRIC_NAME_KEY);
    }

    public Set<String> getMetricDimensionKeys() {
        return new HashSet<>(options.get(METRIC_DIMENSION_KEYS));
    }

    public String getMetricTimestampKey() {
        return options.get(METRIC_TIMESTAMP_KEY);
    }

    public String getMetricUnitKey() {
        return options.get(METRIC_UNIT_KEY);
    }

    public String getMetricCountKey() {
        return options.get(METRIC_COUNT_KEY);
    }

    public String getMetricValueKey() {
        return options.get(METRIC_VALUE_KEY);
    }

    public String getMetricStatisticMaxKey() {
        return options.get(METRIC_STATISTIC_MAX_KEY);
    }

    public String getMetricStatisticMinKey() {
        return options.get(METRIC_STATISTIC_MIN_KEY);
    }

    public String getMetricStatisticSumKey() {
        return options.get(METRIC_STATISTIC_SUM_KEY);
    }

    public String getMetricStatisticSampleCountKey() {
        return options.get(METRIC_STATISTIC_SAMPLE_COUNT_KEY);
    }

    public String getMetricStorageResolutionKey() {
        return options.get(METRIC_STORAGE_RESOLUTION_KEY);
    }
}
