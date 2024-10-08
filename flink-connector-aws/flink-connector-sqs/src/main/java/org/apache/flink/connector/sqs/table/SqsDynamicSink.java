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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.sqs.sink.SqsSink;
import org.apache.flink.connector.sqs.sink.SqsSinkBuilder;
import org.apache.flink.connector.sqs.sink.SqsSinkElementConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** A {@link DynamicTableSink} for SQS. */
@Internal
public class SqsDynamicSink extends AsyncDynamicTableSink<SendMessageBatchRequestEntry> {

    /** Consumed data type of the table. */
    private final DataType consumedDataType;

    /** Url of Sqs queue to write to. */
    private final String sqsUrl;

    /** Properties for the Sqs Aws Client. */
    private final Properties sqsClientProps;

    /** Encoding format to convert between row data and byte array. */
    EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /** Flag to determine whether to fail on error. */
    private final Boolean failOnError;

    protected SqsDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            @Nullable Boolean failOnError,
            @Nullable DataType consumedDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            String sqsUrl,
            @Nullable Properties sqsClientProps) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        Preconditions.checkNotNull(
                encodingFormat, "Encoding format must not be null when creating SQS sink.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(sqsUrl),
                "Sqs url must not be null or empty when creating SQS sink.");
        this.consumedDataType = consumedDataType;
        this.sqsUrl = sqsUrl;
        this.sqsClientProps = sqsClientProps;
        this.failOnError = failOnError;
        this.encodingFormat = encodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SqsSinkBuilder<RowData> builder = SqsSink.builder();
        builder.setSqsUrl(sqsUrl);
        Optional.ofNullable(sqsClientProps).ifPresent(builder::setSqsClientProperties);
        builder.setSqsSinkElementConverter(
                SqsSinkElementConverter.<RowData>builder()
                        .setSerializationSchema(
                                encodingFormat.createRuntimeEncoder(context, consumedDataType))
                        .build());
        Optional.ofNullable(failOnError).ifPresent(builder::setFailOnError);
        return SinkV2Provider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new SqsDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                failOnError,
                consumedDataType,
                encodingFormat,
                sqsUrl,
                sqsClientProps);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqsDynamicSink that = (SqsDynamicSink) o;
        return super.equals(o)
                && failOnError == that.failOnError
                && Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(sqsUrl, that.sqsUrl)
                && Objects.equals(sqsClientProps, that.sqsClientProps)
                && Objects.equals(encodingFormat, that.encodingFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                consumedDataType,
                sqsUrl,
                sqsClientProps,
                encodingFormat,
                failOnError);
    }

    @Override
    public String asSummaryString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SqsDynamicSink{");
        sb.append("sqsUrl='").append(sqsUrl).append('\'');
        sb.append(", consumedDataType=").append(consumedDataType);
        sb.append(", encodingFormat=").append(encodingFormat);
        sb.append(", failOnError=").append(failOnError);
        Optional.ofNullable(sqsClientProps)
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

    public static SqsQueueUrlConfigurator builder() {
        return new SqsDynamicSinkBuilder();
    }

    /** Builder for {@link SqsDynamicSink}. */
    @Internal
    public static class SqsDynamicSinkBuilder
            extends AsyncDynamicTableSinkBuilder<
                    SendMessageBatchRequestEntry, SqsDynamicSinkBuilder>
            implements SqsQueueUrlConfigurator, SqsSinkEncodingFormatConfigurator {

        private String sqsUrl;

        private Properties sqsClientProps;

        private EncodingFormat<SerializationSchema<RowData>> encodingFormat;

        private Boolean failOnError;

        private DataType consumedDataType;

        @Override
        public SqsSinkEncodingFormatConfigurator setSqsQueueUrl(String sqsUrl) {
            this.sqsUrl = sqsUrl;
            return this;
        }

        @Override
        public SqsDynamicSinkBuilder setEncodingFormat(
                EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
            this.encodingFormat = encodingFormat;
            return this;
        }

        public SqsDynamicSinkBuilder setFailOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public SqsDynamicSinkBuilder setSqsClientProperties(Properties sqsClientProps) {
            this.sqsClientProps = sqsClientProps;
            return this;
        }

        public SqsDynamicSinkBuilder setConsumedDataType(DataType consumedDataType) {
            this.consumedDataType = consumedDataType;
            return this;
        }

        @Override
        public SqsDynamicSink build() {
            return new SqsDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    failOnError,
                    consumedDataType,
                    encodingFormat,
                    sqsUrl,
                    sqsClientProps);
        }
    }

    /** Configurator for the required Sqs queue url. */
    @Internal
    public interface SqsQueueUrlConfigurator {
        /**
         * Configures the Sqs queue url.
         *
         * @param sqsUrl the url of the Sqs queue
         */
        SqsSinkEncodingFormatConfigurator setSqsQueueUrl(String sqsUrl);
    }

    /** Configurator for the required encoding format. */
    @Internal
    public interface SqsSinkEncodingFormatConfigurator {
        /**
         * Configures the encoding format.
         *
         * @param encodingFormat the encoding format
         */
        SqsDynamicSinkBuilder setEncodingFormat(
                EncodingFormat<SerializationSchema<RowData>> encodingFormat);
    }
}
