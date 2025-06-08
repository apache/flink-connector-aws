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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.cloudwatch.sink.client.CloudWatchAsyncClientProvider;
import org.apache.flink.connector.cloudwatch.sink.client.SdkClientProvider;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * A CloudWatch Sink that performs async requests against a destination CloudWatch using the
 * buffering protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link CloudWatchAsyncClient} to communicate with the AWS endpoint.
 *
 * <p>Please see the writer implementation in {@link CloudWatchSinkWriter}
 *
 * <p>maxBatchSize is calculated in terms of requestEntries (MetricWriteRequest). In CloudWatch,
 * each PutMetricDataRequest can have maximum of 1000 MetricWriteRequest, hence the maxBatchSize
 * cannot be more than 1000.
 *
 * <p>maxBatchSizeInBytes is calculated in terms of size of requestEntries (MetricWriteRequest). In
 * CloudWatch, each PutMetricDataRequest can have maximum of 1MB of payload, hence the
 * maxBatchSizeInBytes cannot be more than 1 MB.
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class CloudWatchSink<InputT> extends AsyncSinkBase<InputT, MetricWriteRequest> {

    private final String namespace;
    private final Properties cloudWatchClientProperties;
    private transient SdkClientProvider<CloudWatchAsyncClient> asyncClientSdkClientProviderOverride;
    private final InvalidMetricDataRetryMode invalidMetricDataRetryMode;

    CloudWatchSink(
            ElementConverter<InputT, MetricWriteRequest> elementConverter,
            Integer maxBatchSize,
            Integer maxInFlightRequests,
            Integer maxBufferedRequests,
            Long maxBatchSizeInBytes,
            Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            String namespace,
            Properties cloudWatchClientProperties,
            InvalidMetricDataRetryMode invalidMetricDataRetryMode) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.namespace =
                Preconditions.checkNotNull(
                        namespace,
                        "The cloudWatch namespace must not be null when initializing the CloudWatch Sink.");
        this.invalidMetricDataRetryMode = invalidMetricDataRetryMode;
        Preconditions.checkArgument(
                !this.namespace.isEmpty(),
                "The cloudWatch namespace must be set when initializing the CloudWatch Sink.");

        Preconditions.checkArgument(
                (this.getMaxBatchSize() <= 1000),
                "The cloudWatch MaxBatchSize must not be greater than 1,000.");

        Preconditions.checkArgument(
                (this.getMaxBatchSizeInBytes() <= 1000 * 1000),
                "The cloudWatch MaxBatchSizeInBytes must not be greater than 1,000,000.");

        this.cloudWatchClientProperties = cloudWatchClientProperties;
    }

    /**
     * Create a {@link CloudWatchSinkBuilder} to allow the fluent construction of a new {@code
     * CloudWatchSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link CloudWatchSinkBuilder}
     */
    public static <InputT> CloudWatchSinkBuilder<InputT> builder() {
        return new CloudWatchSinkBuilder<>();
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<MetricWriteRequest>> createWriter(
            InitContext context) throws IOException {
        return new CloudWatchSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                namespace,
                getAsyncClientProvider(cloudWatchClientProperties),
                Collections.emptyList(),
                invalidMetricDataRetryMode);
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<MetricWriteRequest>> restoreWriter(
            InitContext context,
            Collection<BufferedRequestState<MetricWriteRequest>> recoveredState)
            throws IOException {
        return new CloudWatchSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                namespace,
                getAsyncClientProvider(cloudWatchClientProperties),
                recoveredState,
                invalidMetricDataRetryMode);
    }

    private SdkClientProvider<CloudWatchAsyncClient> getAsyncClientProvider(
            Properties clientProperties) {
        if (asyncClientSdkClientProviderOverride != null) {
            return asyncClientSdkClientProviderOverride;
        }
        return new CloudWatchAsyncClientProvider(clientProperties);
    }

    @Internal
    @VisibleForTesting
    void setCloudWatchAsyncClientProvider(
            SdkClientProvider<CloudWatchAsyncClient> asyncClientSdkClientProviderOverride) {
        this.asyncClientSdkClientProviderOverride = asyncClientSdkClientProviderOverride;
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<MetricWriteRequest>>
            getWriterStateSerializer() {
        return new CloudWatchStateSerializer();
    }
}
