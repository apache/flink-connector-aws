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
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.cloudwatch.sink.CloudWatchSinkBuilder;
import org.apache.flink.connector.cloudwatch.sink.InvalidMetricDataRetryMode;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** A {@link DynamicTableSink} for CloudWatch. */
@Internal
public class CloudWatchDynamicSink extends AsyncDynamicTableSink<MetricDatum> {

    /** Physical data type of the table. */
    private final DataType physicalDataType;

    /** Url of CloudWatch queue to write to. */
    private final String namespace;

    /** Properties for the CloudWatch Aws Client. */
    private final Properties cloudWatchClientProps;

    /** Retry mode when an invalid CloudWatch Metric is encountered. */
    private final InvalidMetricDataRetryMode invalidMetricDataRetryMode;

    /** Properties for the CloudWatch Table Config. */
    private final CloudWatchTableConfig cloudWatchTableConfig;

    protected CloudWatchDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            @Nullable InvalidMetricDataRetryMode invalidMetricDataRetryMode,
            @Nullable DataType physicalDataType,
            String namespace,
            @Nullable Properties cloudWatchClientProps,
            CloudWatchTableConfig cloudWatchTableConfig) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        this.cloudWatchTableConfig = cloudWatchTableConfig;
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(namespace),
                "CloudWatch namespace must not be null or empty when creating CloudWatch sink.");
        this.physicalDataType = physicalDataType;
        this.namespace = namespace;
        this.cloudWatchClientProps = cloudWatchClientProps;
        this.invalidMetricDataRetryMode = invalidMetricDataRetryMode;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        CloudWatchSinkBuilder<RowData> builder =
                new CloudWatchSinkBuilder<RowData>()
                        .setNamespace(namespace)
                        .setElementConverter(
                                new RowDataElementConverter(
                                        physicalDataType, cloudWatchTableConfig));

        Optional.ofNullable(cloudWatchClientProps)
                .ifPresent(builder::setCloudWatchClientProperties);
        Optional.ofNullable(invalidMetricDataRetryMode)
                .ifPresent(builder::setInvalidMetricDataRetryMode);
        return SinkV2Provider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new CloudWatchDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                invalidMetricDataRetryMode,
                physicalDataType,
                namespace,
                cloudWatchClientProps,
                cloudWatchTableConfig);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CloudWatchDynamicSink that = (CloudWatchDynamicSink) o;
        return super.equals(o)
                && Objects.equals(invalidMetricDataRetryMode, that.invalidMetricDataRetryMode)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(namespace, that.namespace)
                && Objects.equals(cloudWatchClientProps, that.cloudWatchClientProps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                physicalDataType,
                namespace,
                cloudWatchClientProps,
                invalidMetricDataRetryMode);
    }

    @Override
    public String asSummaryString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CloudWatchDynamicSink{");
        sb.append("cloudWatchUrl='").append(namespace).append('\'');
        sb.append(", physicalDataType=").append(physicalDataType);
        sb.append(", cloudWatchTableConfig=").append(cloudWatchTableConfig);
        sb.append(", invalidMetricDataRetryMode=").append(invalidMetricDataRetryMode);
        Optional.ofNullable(cloudWatchClientProps)
                .ifPresent(
                        props ->
                                props.forEach(
                                        (k, v) -> sb.append(", ").append(k).append("=").append(v)));
        sb.append(", maxBatchSize=").append(maxBatchSize);
        sb.append(", maxInFlightRequests=").append(maxInFlightRequests);
        sb.append(", maxBufferedRequests=").append(maxBufferedRequests);
        sb.append(", maxBufferSizeInBytes=").append(maxBufferSizeInBytes);
        sb.append(", maxTimeInBufferMS=").append(maxTimeInBufferMS);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String toString() {
        return asSummaryString();
    }

    public static CloudWatchDynamicSinkBuilder builder() {
        return new CloudWatchDynamicSinkBuilder();
    }

    /** Builder for {@link CloudWatchDynamicSink}. */
    @Internal
    public static class CloudWatchDynamicSinkBuilder
            extends AsyncDynamicTableSinkBuilder<MetricDatum, CloudWatchDynamicSinkBuilder> {

        private String namespace;

        private Properties cloudWatchClientProps;

        private InvalidMetricDataRetryMode invalidMetricDataRetryMode;

        private DataType physicalDataType;

        private CloudWatchTableConfig cloudWatchTableConfig;

        public CloudWatchDynamicSinkBuilder setCloudWatchMetricNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public CloudWatchDynamicSinkBuilder setCloudWatchTableConfig(
                CloudWatchTableConfig cloudWatchTableConfig) {
            this.cloudWatchTableConfig = cloudWatchTableConfig;
            return this;
        }

        public CloudWatchDynamicSinkBuilder setInvalidMetricDataRetryMode(
                InvalidMetricDataRetryMode invalidMetricDataRetryMode) {
            this.invalidMetricDataRetryMode = invalidMetricDataRetryMode;
            return this;
        }

        public CloudWatchDynamicSinkBuilder setCloudWatchClientProperties(
                Properties cloudWatchClientProps) {
            this.cloudWatchClientProps = cloudWatchClientProps;
            return this;
        }

        public CloudWatchDynamicSinkBuilder setPhysicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        @Override
        public CloudWatchDynamicSink build() {
            return new CloudWatchDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    invalidMetricDataRetryMode,
                    physicalDataType,
                    namespace,
                    cloudWatchClientProps,
                    cloudWatchTableConfig);
        }
    }
}
