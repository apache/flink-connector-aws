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

package org.apache.flink.connector.cloudwatch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import software.amazon.awssdk.http.Protocol;

import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static software.amazon.awssdk.http.Protocol.HTTP1_1;

/**
 * Builder to construct {@link CloudWatchSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link CloudWatchSink} that writes
 * String values to a CloudWatch named cloudWatchUrl.
 *
 * <pre>{@code
 * Properties sinkProperties = new Properties();
 * sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
 *
 * CloudWatchSink<String> cloudWatchSink =
 *         CloudWatchSink.<String>builder()
 *                 .setNamespace("namespace")
 *                 .setCloudWatchClientProperties(sinkProperties)
 *                 .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 100
 *   <li>{@code maxInFlightRequests} will be 50
 *   <li>{@code maxBufferedRequests} will be 5000
 *   <li>{@code maxBatchSizeInBytes} will be 100 KB
 *   <li>{@code maxTimeInBufferMs} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} will be 1 KB
 *   <li>{@code invalidMetricDataRetryMode} will be FAIL_ON_ERROR
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class CloudWatchSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, MetricWriteRequest, CloudWatchSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 100;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 5_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 100 * 1000;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1000;
    private static final InvalidMetricDataRetryMode DEFAULT_INVALID_METRIC_DATA_RETRY_MODE =
            InvalidMetricDataRetryMode.FAIL_ON_ERROR;
    private static final Protocol DEFAULT_HTTP_PROTOCOL = HTTP1_1;

    private String namespace;
    private Properties cloudWatchClientProperties;
    private InvalidMetricDataRetryMode invalidMetricDataRetryMode;

    private ElementConverter<InputT, MetricWriteRequest> elementConverter;

    public CloudWatchSinkBuilder<InputT> setElementConverter(
            final ElementConverter<InputT, MetricWriteRequest> elementConverter) {
        this.elementConverter = elementConverter;
        return this;
    }

    public CloudWatchSinkBuilder<InputT> setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * If writing to CloudWatch results in a failure being returned due to Invalid Metric Data
     * provided, the retry mode will be determined based on this config.
     *
     * @param invalidMetricDataRetryMode retry mode
     * @return {@link CloudWatchSinkBuilder} itself
     */
    public CloudWatchSinkBuilder<InputT> setInvalidMetricDataRetryMode(
            InvalidMetricDataRetryMode invalidMetricDataRetryMode) {
        this.invalidMetricDataRetryMode = invalidMetricDataRetryMode;
        return this;
    }

    /**
     * A set of properties used by the sink to create the CloudWatch client. This may be used to set
     * the aws region, credentials etc. See the docs for usage and syntax.
     *
     * @param cloudWatchClientProps CloudWatch client properties
     * @return {@link CloudWatchSinkBuilder} itself
     */
    public CloudWatchSinkBuilder<InputT> setCloudWatchClientProperties(
            final Properties cloudWatchClientProps) {
        cloudWatchClientProperties = cloudWatchClientProps;
        return this;
    }

    protected InvalidMetricDataRetryMode getInvalidMetricDataRetryMode() {
        return this.invalidMetricDataRetryMode;
    }

    @VisibleForTesting
    Properties getClientPropertiesWithDefaultHttpProtocol() {
        Properties clientProperties =
                Optional.ofNullable(cloudWatchClientProperties).orElse(new Properties());
        clientProperties.putIfAbsent(HTTP_PROTOCOL_VERSION, DEFAULT_HTTP_PROTOCOL.toString());
        return clientProperties;
    }

    @Override
    public CloudWatchSink<InputT> build() {
        return new CloudWatchSink<>(
                Optional.ofNullable(elementConverter)
                        .orElse(
                                (DefaultMetricWriteRequestElementConverter<InputT>)
                                        DefaultMetricWriteRequestElementConverter.builder()
                                                .build()),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                namespace,
                getClientPropertiesWithDefaultHttpProtocol(),
                Optional.ofNullable(getInvalidMetricDataRetryMode())
                        .orElse(DEFAULT_INVALID_METRIC_DATA_RETRY_MODE));
    }
}
