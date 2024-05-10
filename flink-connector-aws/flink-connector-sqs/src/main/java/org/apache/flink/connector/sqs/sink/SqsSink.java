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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.sqs.sink.client.SdkClientProvider;
import org.apache.flink.connector.sqs.sink.client.SqsAsyncClientProvider;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * A SQS Sink that performs async requests against a destination SQS using the buffering protocol
 * specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link software.amazon.awssdk.services.sqs.SqsAsyncClient} to
 * communicate with the AWS endpoint.
 *
 * <p>Please see the writer implementation in {@link SqsSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class SqsSink<InputT> extends AsyncSinkBase<InputT, SendMessageBatchRequestEntry> {

    private final boolean failOnError;
    private final String sqsUrl;
    private final Properties sqsClientProperties;
    private transient SdkClientProvider<SqsAsyncClient> asyncClientSdkClientProviderOverride;

    SqsSink(
            ElementConverter<InputT, SendMessageBatchRequestEntry> elementConverter,
            Integer maxBatchSize,
            Integer maxInFlightRequests,
            Integer maxBufferedRequests,
            Long maxBatchSizeInBytes,
            Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            boolean failOnError,
            String sqsUrl,
            Properties sqsClientProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.sqsUrl =
                Preconditions.checkNotNull(
                        sqsUrl, "The sqs url must not be null when initializing the SQS Sink.");
        Preconditions.checkArgument(
                !this.sqsUrl.isEmpty(), "The sqs url must be set when initializing the SQS Sink.");

        Preconditions.checkArgument(
                (this.getMaxBatchSize() <= 10),
                "The sqs MaxBatchSize must not be greater than 10.");
        this.failOnError = failOnError;
        this.sqsClientProperties = sqsClientProperties;
    }

    /**
     * Create a {@link SqsSinkBuilder} to allow the fluent construction of a new {@code SqsSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link SqsSinkBuilder}
     */
    public static <InputT> SqsSinkBuilder<InputT> builder() {
        return new SqsSinkBuilder<>();
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<SendMessageBatchRequestEntry>>
            createWriter(InitContext context) throws IOException {
        return new SqsSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                sqsUrl,
                getAsyncClientProvider(sqsClientProperties),
                Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<SendMessageBatchRequestEntry>>
            restoreWriter(
                    InitContext context,
                    Collection<BufferedRequestState<SendMessageBatchRequestEntry>> recoveredState)
                    throws IOException {
        return new SqsSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                sqsUrl,
                getAsyncClientProvider(sqsClientProperties),
                recoveredState);
    }

    private SdkClientProvider<SqsAsyncClient> getAsyncClientProvider(Properties clientProperties) {
        if (asyncClientSdkClientProviderOverride != null) {
            return asyncClientSdkClientProviderOverride;
        }
        return new SqsAsyncClientProvider(clientProperties);
    }

    @Internal
    @VisibleForTesting
    void setSqsAsyncClientProvider(
            SdkClientProvider<SqsAsyncClient> asyncClientSdkClientProviderOverride) {
        this.asyncClientSdkClientProviderOverride = asyncClientSdkClientProviderOverride;
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<SendMessageBatchRequestEntry>>
            getWriterStateSerializer() {
        return new SqsStateSerializer();
    }
}
