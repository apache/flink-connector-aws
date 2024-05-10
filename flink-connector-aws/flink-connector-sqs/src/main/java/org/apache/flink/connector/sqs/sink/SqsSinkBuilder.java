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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static software.amazon.awssdk.http.Protocol.HTTP1_1;

/**
 * Builder to construct {@link SqsSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link SqsSink} that writes String
 * values to a SQS named sqsUrl.
 *
 * <pre>{@code
 * Properties sinkProperties = new Properties();
 * sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
 *
 * SqsSink<String> sqsSink =
 *         SqsSink.<String>builder()
 *                 .setSqsUrl("sqsUrl")
 *                 .setSqsClientProperties(sinkProperties)
 *                 .setSqsSinkElementConverter(SqsSinkElementConverter.<String>builder()
 *                                 .setSerializationSchema(new SimpleStringSchema())
 *                                 .build())
 *                 .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 10
 *   <li>{@code maxInFlightRequests} will be 50
 *   <li>{@code maxBufferedRequests} will be 5000
 *   <li>{@code maxBatchSizeInBytes} will be 256 KB i.e. {@code 256 * 1000}
 *   <li>{@code maxTimeInBufferMs} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} will be 256 KB i.e. {@code 256 * 1000}
 *   <li>{@code failOnError} will be false
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class SqsSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, SendMessageBatchRequestEntry, SqsSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 10;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 5_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 256 * 1000;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 256 * 1000;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;
    private static final Protocol DEFAULT_HTTP_PROTOCOL = HTTP1_1;

    private Boolean failOnError;
    private String sqsUrl;
    private Properties sqsClientProperties;

    private SqsSinkElementConverter<InputT> sqsSinkElementConverter;

    SqsSinkBuilder() {}

    /**
     * Sets the url of the SQS that the sink will connect to. There is no default for this
     * parameter, therefore, this must be provided at sink creation time otherwise the build will
     * fail.
     *
     * @param sqsUrl the url of the Sqs
     * @return {@link SqsSinkBuilder} itself
     */
    public SqsSinkBuilder<InputT> setSqsUrl(String sqsUrl) {
        this.sqsUrl = sqsUrl;
        return this;
    }

    public SqsSinkBuilder<InputT> setSqsSinkElementConverter(
            final SqsSinkElementConverter<InputT> sqsSinkElementConverter) {
        this.sqsSinkElementConverter = sqsSinkElementConverter;
        return this;
    }

    /**
     * If writing to SQS results in a partial or full failure being returned, the job will fail
     * immediately with a {@link SqsSinkException} if failOnError is set.
     *
     * @param failOnError whether to fail on error
     * @return {@link SqsSinkBuilder} itself
     */
    public SqsSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    /**
     * A set of properties used by the sink to create the SQS client. This may be used to set the
     * aws region, credentials etc. See the docs for usage and syntax.
     *
     * @param sqsClientProps SQS client properties
     * @return {@link SqsSinkBuilder} itself
     */
    public SqsSinkBuilder<InputT> setSqsClientProperties(final Properties sqsClientProps) {
        sqsClientProperties = sqsClientProps;
        return this;
    }

    @VisibleForTesting
    Properties getClientPropertiesWithDefaultHttpProtocol() {
        Properties clientProperties =
                Optional.ofNullable(sqsClientProperties).orElse(new Properties());
        clientProperties.putIfAbsent(HTTP_PROTOCOL_VERSION, DEFAULT_HTTP_PROTOCOL.toString());
        return clientProperties;
    }

    @Override
    public SqsSink<InputT> build() {
        return new SqsSink<>(
                Optional.ofNullable(sqsSinkElementConverter)
                        .orElse(
                                (SqsSinkElementConverter<InputT>)
                                        SqsSinkElementConverter.<String>builder()
                                                .setSerializationSchema(new SimpleStringSchema())
                                                .build()),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                Optional.ofNullable(failOnError).orElse(DEFAULT_FAIL_ON_ERROR),
                sqsUrl,
                getClientPropertiesWithDefaultHttpProtocol());
    }
}
