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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.model.Metadata;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Kinesis-backed {@link ScanTableSource}. */
public class KinesisDynamicSource implements ScanTableSource, SupportsReadingMetadata {

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType physicalDataType;
    private final String stream;

    /** Configuration for the Kinesis consumer. */
    private final Configuration sourceConfig;

    /** List of read-only metadata fields that the source can provide upstream upon request. */
    private static final Map<String, DataType> READABLE_METADATA =
            new HashMap<String, DataType>() {
                {
                    for (Metadata metadata : Metadata.values()) {
                        put(metadata.getFieldName(), metadata.getDataType());
                    }
                }
            };

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    private DataType producedDataType;

    /** Metadata that is requested to be appended at the end of a physical source row. */
    private List<Metadata> requestedMetadataFields;

    public KinesisDynamicSource(
            @Nullable DataType physicalDataType,
            String stream,
            Configuration sourceConfig,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this(
                physicalDataType,
                stream,
                sourceConfig,
                decodingFormat,
                physicalDataType,
                Collections.emptyList());
    }

    public KinesisDynamicSource(
            @Nullable DataType physicalDataType,
            String stream,
            Configuration sourceConfig,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType,
            List<Metadata> requestedMetadataFields) {
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.stream = Preconditions.checkNotNull(stream, "Stream must not be null.");
        this.sourceConfig =
                Preconditions.checkNotNull(
                        sourceConfig,
                        "Properties for the Flink Kinesis consumer must not be null.");
        this.decodingFormat =
                Preconditions.checkNotNull(decodingFormat, "Decoding format must not be null.");
        this.producedDataType =
                Preconditions.checkNotNull(
                        producedDataType, "Produced data type must not be null.");
        this.requestedMetadataFields =
                Preconditions.checkNotNull(
                        requestedMetadataFields, "Requested metadata fields must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        DeserializationSchema<RowData> deserializationSchema =
                decodingFormat.createRuntimeDecoder(scanContext, physicalDataType);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {

                KinesisStreamsSource<RowData> kdsSource =
                        KinesisStreamsSource.<RowData>builder()
                                .setStreamArn(stream)
                                .setSourceConfig(sourceConfig)
                                .setDeserializationSchema(deserializationSchema)
                                .build();

                DataStreamSource<RowData> sourceStream =
                        execEnv.fromSource(
                                kdsSource, WatermarkStrategy.noWatermarks(), "Kinesis source");

                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return false;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new KinesisDynamicSource(
                physicalDataType,
                stream,
                sourceConfig,
                decodingFormat,
                producedDataType,
                requestedMetadataFields);
    }

    @Override
    public String asSummaryString() {
        return "Kinesis table source";
    }

    // --------------------------------------------------------------------------------------------
    // SupportsReadingMetadata
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return READABLE_METADATA;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.requestedMetadataFields =
                metadataKeys.stream().map(Metadata::of).collect(Collectors.toList());
        this.producedDataType = producedDataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KinesisDynamicSource that = (KinesisDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(requestedMetadataFields, that.requestedMetadataFields)
                && Objects.equals(stream, that.stream)
                && Objects.equals(sourceConfig, that.sourceConfig)
                && Objects.equals(decodingFormat, that.decodingFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                requestedMetadataFields, producedDataType, stream, sourceConfig, decodingFormat);
    }

    /** Builder class for {@link KinesisDynamicSource}. */
    @Internal
    public static class KinesisDynamicTableSourceBuilder {
        private DataType consumedDataType = null;
        private String stream = null;
        private DecodingFormat<DeserializationSchema<RowData>> decodingFormat = null;

        private Configuration sourceConfig = null;

        public KinesisDynamicSource.KinesisDynamicTableSourceBuilder setConsumedDataType(
                DataType consumedDataType) {
            this.consumedDataType = consumedDataType;
            return this;
        }

        public KinesisDynamicSource.KinesisDynamicTableSourceBuilder setStream(String stream) {
            this.stream = stream;
            return this;
        }

        public KinesisDynamicSource.KinesisDynamicTableSourceBuilder setDecodingFormat(
                DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
            this.decodingFormat = decodingFormat;
            return this;
        }

        public KinesisDynamicSource.KinesisDynamicTableSourceBuilder setSourceConfig(
                Configuration sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        public KinesisDynamicSource build() {
            return new KinesisDynamicSource(consumedDataType, stream, sourceConfig, decodingFormat);
        }
    }
}
