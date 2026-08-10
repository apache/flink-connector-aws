/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSinkBuilder;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** Kinesis backed {@link AsyncDynamicTableSink}. */
@Internal
public class KinesisDynamicSink extends AsyncDynamicTableSink<PutRecordsRequestEntry>
        implements SupportsPartitioning {

    /** Consumed data type of the table. */
    private final DataType consumedDataType;

    /** The Kinesis stream to write to. */
    private final String streamArn;

    /** Properties for the Kinesis DataStream Sink. */
    private final Properties kinesisClientProperties;

    /** Sink format for encoding records to Kinesis. */
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /** Partitioner to select Kinesis partition for each item. */
    private final PartitionKeyGenerator<RowData> partitioner;

    private final Boolean failOnError;

    /** Whether this sink supports upsert mode (primary key is defined on the table). */
    private final boolean upsertMode;

    public KinesisDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            @Nullable Boolean failOnError,
            @Nullable DataType consumedDataType,
            String streamArn,
            @Nullable Properties kinesisClientProperties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            PartitionKeyGenerator<RowData> partitioner,
            boolean upsertMode) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        this.failOnError = failOnError;
        this.kinesisClientProperties = kinesisClientProperties;
        this.consumedDataType =
                Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null");
        this.streamArn =
                Preconditions.checkNotNull(streamArn, "Kinesis streamArn name must not be null");
        this.encodingFormat =
                Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null");
        this.partitioner =
                Preconditions.checkNotNull(
                        partitioner, "Kinesis partition key generator must not be null");
        this.upsertMode = upsertMode;
        Preconditions.checkArgument(
                !upsertMode || maxInFlightRequests == null || maxInFlightRequests == 1,
                "Upsert mode requires maxInFlightRequests = 1 to preserve per-key ordering, but was %s.",
                maxInFlightRequests);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (upsertMode) {
            // DELETE is intentionally not supported: Kinesis records have no key/value
            // separation, so a delete event cannot carry the deleted key in a
            // format-agnostic way. Queries that can produce DELETE events (e.g. CDC
            // sources, Top-N queries) are rejected during planning instead of writing
            // tombstones that downstream consumers cannot interpret.
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .build();
        }
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                encodingFormat.createRuntimeEncoder(context, consumedDataType);

        SerializationSchema<RowData> actualSchema =
                upsertMode
                        ? new UpsertSerializationSchemaWrapper(serializationSchema)
                        : serializationSchema;

        KinesisStreamsSinkBuilder<RowData> builder =
                KinesisStreamsSink.<RowData>builder()
                        .setSerializationSchema(actualSchema)
                        .setPartitionKeyGenerator(partitioner)
                        .setKinesisClientProperties(kinesisClientProperties)
                        .setStreamArn(streamArn);

        Optional.ofNullable(failOnError).ifPresent(builder::setFailOnError);
        addAsyncOptionsToSinkBuilder(builder);
        KinesisStreamsSink<RowData> kdsSink = builder.build();
        return SinkV2Provider.of(kdsSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new KinesisDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                failOnError,
                consumedDataType,
                streamArn,
                kinesisClientProperties,
                encodingFormat,
                partitioner,
                upsertMode);
    }

    @Override
    public String asSummaryString() {
        return "Kinesis";
    }

    // --------------------------------------------------------------------------------------------
    // SupportsPartitioning
    // --------------------------------------------------------------------------------------------

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        if (partitioner instanceof RowDataFieldsKinesisPartitionKeyGenerator) {
            ((RowDataFieldsKinesisPartitionKeyGenerator) partitioner).setStaticFields(partition);
        } else {
            String msg =
                    ""
                            + "Cannot apply static partition optimization to a partition class "
                            + "that does not inherit from "
                            + "org.apache.flink.streaming.connectors.kinesis.table.RowDataKinesisPartitioner.";
            throw new RuntimeException(msg);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Value semantics for equals and hashCode
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KinesisDynamicSink that = (KinesisDynamicSink) o;
        return super.equals(o)
                && Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(streamArn, that.streamArn)
                && Objects.equals(kinesisClientProperties, that.kinesisClientProperties)
                && Objects.equals(encodingFormat, that.encodingFormat)
                && Objects.equals(partitioner, that.partitioner)
                && Objects.equals(failOnError, that.failOnError)
                && upsertMode == that.upsertMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                consumedDataType,
                streamArn,
                kinesisClientProperties,
                encodingFormat,
                partitioner,
                failOnError,
                upsertMode);
    }

    /**
     * Wraps a serialization schema for upsert mode: rewrites the {@link RowKind} of {@code
     * UPDATE_AFTER} rows to {@code INSERT} before delegating to the wrapped schema, so that
     * insert-only formats (e.g. json, csv) can encode the row. {@code DELETE} and {@code
     * UPDATE_BEFORE} rows are rejected defensively; the planner already excludes them because
     * {@link KinesisDynamicSink#getChangelogMode(ChangelogMode)} does not advertise them in upsert
     * mode.
     */
    @Internal
    static class UpsertSerializationSchemaWrapper implements SerializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        private final SerializationSchema<RowData> inner;

        UpsertSerializationSchemaWrapper(SerializationSchema<RowData> inner) {
            this.inner = inner;
        }

        @Override
        public void open(InitializationContext context) throws Exception {
            inner.open(context);
        }

        @Override
        public byte[] serialize(RowData element) {
            RowKind kind = element.getRowKind();
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                throw new UnsupportedOperationException(
                        String.format(
                                "The Kinesis sink in upsert mode does not support %s records. "
                                        + "Queries producing DELETE or UPDATE_BEFORE events should "
                                        + "have been rejected during planning; receiving one here "
                                        + "indicates a planner/connector inconsistency.",
                                kind));
            }
            if (kind == RowKind.INSERT) {
                return inner.serialize(element);
            }
            // Normalize UPDATE_AFTER to INSERT for insert-only formats. Restore the
            // original RowKind even if serialization fails, since RowData instances may
            // be reused.
            element.setRowKind(RowKind.INSERT);
            try {
                return inner.serialize(element);
            } finally {
                element.setRowKind(kind);
            }
        }
    }

    /** Builder class for {@link KinesisDynamicSink}. */
    @Internal
    public static class KinesisDynamicTableSinkBuilder
            extends AsyncDynamicTableSinkBuilder<
                    PutRecordsRequestEntry, KinesisDynamicTableSinkBuilder> {

        private DataType consumedDataType = null;
        private String streamArn = null;
        private Properties kinesisClientProperties = null;
        private EncodingFormat<SerializationSchema<RowData>> encodingFormat = null;
        private PartitionKeyGenerator<RowData> partitioner = null;
        private Boolean failOnError = null;
        private boolean upsertMode = false;

        public KinesisDynamicTableSinkBuilder setConsumedDataType(DataType consumedDataType) {
            this.consumedDataType = consumedDataType;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setStreamArn(String streamArn) {
            this.streamArn = streamArn;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setStream(String streamArn) {
            this.streamArn = streamArn;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setKinesisClientProperties(
                Properties kinesisClientProperties) {
            this.kinesisClientProperties = kinesisClientProperties;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setEncodingFormat(
                EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
            this.encodingFormat = encodingFormat;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setFailOnError(Boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setPartitioner(
                PartitionKeyGenerator<RowData> partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setUpsertMode(boolean upsertMode) {
            this.upsertMode = upsertMode;
            return this;
        }

        @Override
        public KinesisDynamicSink build() {
            return new KinesisDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    failOnError,
                    consumedDataType,
                    streamArn,
                    kinesisClientProperties,
                    encodingFormat,
                    partitioner,
                    upsertMode);
        }
    }
}
