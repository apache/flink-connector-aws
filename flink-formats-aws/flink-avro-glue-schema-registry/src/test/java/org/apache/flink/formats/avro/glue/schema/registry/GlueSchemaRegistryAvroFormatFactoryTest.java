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

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link GlueSchemaRegistryAvroFormatFactory}. */
class GlueSchemaRegistryAvroFormatFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String SCHEMA_NAME = "test-subject";
    private static final String REGISTRY_NAME = "test-registry-name";
    private static final String REGION = "us-west-2";

    @Test
    void testDeserializationSchema() {
        final DynamicTableSource actualSource = createTableSource(SCHEMA, getDefaultOptions());
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);

        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isInstanceOf(AvroRowDataDeserializationSchema.class);
    }

    @Test
    void testSerializationSchema() {
        final DynamicTableSink actualSink = createTableSink(SCHEMA, getDefaultOptions());
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);

        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        assertThat(actualSer).isInstanceOf(AvroRowDataSerializationSchema.class);
    }

    @Test
    void testMissingSchemaNameForSink() {
        final Map<String, String> options =
                getModifiedOptions(opts -> opts.remove("avro-glue.schema.name"));

        assertThatThrownBy(() -> createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testMissingRegionForSource() {
        final Map<String, String> options =
                getModifiedOptions(opts -> opts.remove("avro-glue.aws.region"));

        assertThatThrownBy(() -> createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testMissingRegistryNameForSource() {
        final Map<String, String> options =
                getModifiedOptions(opts -> opts.remove("avro-glue.registry.name"));

        assertThatThrownBy(() -> createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testDeserializationSchemaWithNamespaceOverride() {
        final String customNamespace = "com.example.custom";
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> opts.put("avro-glue.avro.namespace", customNamespace));

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);

        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        // Verify the schema is created successfully with namespace override
        assertThat(actualDeser).isInstanceOf(AvroRowDataDeserializationSchema.class);
    }

    @Test
    void testSerializationSchemaWithRecordNameOverride() {
        final String customRecordName = "MyCustomRecord";
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> opts.put("avro-glue.avro.record-name", customRecordName));

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);

        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        // Verify the schema is created successfully with record name override
        assertThat(actualSer).isInstanceOf(AvroRowDataSerializationSchema.class);
    }

    @Test
    void testSpiDiscovery() {
        final DynamicTableSource source = createTableSource(SCHEMA, getDefaultOptions());
        assertThat(source).isNotNull();

        final DynamicTableSink sink = createTableSink(SCHEMA, getDefaultOptions());
        assertThat(sink).isNotNull();
    }

    @Test
    void testDeserializationSchemaWithFetchFromRegistry() {
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> opts.put("avro-glue.schema.fetchFromRegistry", "true"));

        // Should not throw - the option should be accepted
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);

        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isInstanceOf(AvroRowDataDeserializationSchema.class);
    }

    @Test
    void testSerializationSchemaWithFetchFromRegistry() {
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> opts.put("avro-glue.schema.fetchFromRegistry", "true"));

        // Should not throw - the option should be accepted
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);

        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        assertThat(actualSer).isInstanceOf(AvroRowDataSerializationSchema.class);
    }

    @Test
    void testDeserializationSchemaWithAllOverrideOptions() {
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> {
                            opts.put("avro-glue.avro.namespace", "com.example.custom");
                            opts.put("avro-glue.avro.record-name", "CustomRecord");
                            opts.put("avro-glue.schema.fetchFromRegistry", "false");
                        });

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);

        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isInstanceOf(AvroRowDataDeserializationSchema.class);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getDefaultOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getDefaultOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", GlueSchemaRegistryAvroFormatFactory.IDENTIFIER);
        options.put("avro-glue.schema.name", SCHEMA_NAME);
        options.put("avro-glue.registry.name", REGISTRY_NAME);
        options.put("avro-glue.aws.region", REGION);
        return options;
    }
}
