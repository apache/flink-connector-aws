/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.glue.schema.registry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.glue.schema.registry.GlueFormatConfigBuilder;
import org.apache.flink.formats.avro.glue.schema.registry.GlueFormatOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Table format factory for providing configured instances of AWS Glue Schema Registry Protobuf to
 * RowData {@link SerializationSchema} and {@link DeserializationSchema}.
 *
 * <p>This factory supports:
 *
 * <ul>
 *   <li>SPI discovery via identifier {@code protobuf-glue}
 *   <li>Protobuf serialization with GSR header prepending
 *   <li>Protobuf deserialization with GSR header stripping
 *   <li>Protobuf schema registration with GSR derived from Flink RowType
 * </ul>
 */
@Internal
public class GlueSchemaRegistryProtobufFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "protobuf-glue";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                final String schemaName = formatOptions.get(GlueFormatOptions.SCHEMA_NAME);
                final Map<String, Object> configMap =
                        GlueFormatConfigBuilder.buildConfigMap(formatOptions);

                return new GsrProtobufRowDataDeserializationSchema(
                        rowType, rowDataTypeInfo, schemaName, configMap);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                final Map<String, Object> configMap =
                        GlueFormatConfigBuilder.buildConfigMap(formatOptions);
                final String schemaName = formatOptions.get(GlueFormatOptions.SCHEMA_NAME);

                return new GsrProtobufRowDataSerializationSchema(
                        rowType, schemaName, schemaName, configMap);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GlueFormatOptions.AWS_REGION);
        options.add(GlueFormatOptions.REGISTRY_NAME);
        options.add(GlueFormatOptions.SCHEMA_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GlueFormatOptions.AWS_ENDPOINT);
        options.add(GlueFormatOptions.CACHE_SIZE);
        options.add(GlueFormatOptions.CACHE_TTL_MS);
        options.add(GlueFormatOptions.SCHEMA_AUTO_REGISTRATION);
        options.add(GlueFormatOptions.SCHEMA_COMPATIBILITY);
        options.add(GlueFormatOptions.SCHEMA_COMPRESSION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        GlueFormatOptions.AWS_REGION,
                        GlueFormatOptions.AWS_ENDPOINT,
                        GlueFormatOptions.REGISTRY_NAME,
                        GlueFormatOptions.SCHEMA_NAME,
                        GlueFormatOptions.CACHE_SIZE,
                        GlueFormatOptions.CACHE_TTL_MS,
                        GlueFormatOptions.SCHEMA_AUTO_REGISTRATION,
                        GlueFormatOptions.SCHEMA_COMPATIBILITY,
                        GlueFormatOptions.SCHEMA_COMPRESSION)
                .collect(Collectors.toSet());
    }
}
