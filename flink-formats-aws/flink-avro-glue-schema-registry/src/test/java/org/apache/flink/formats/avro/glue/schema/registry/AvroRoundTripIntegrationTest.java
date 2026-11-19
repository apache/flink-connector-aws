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

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Avro round-trip serialization/deserialization with mock GSR facades.
 *
 * <p>Validates Requirements 8.1, 8.3.
 */
class AvroRoundTripIntegrationTest {

    private MockGlueSchemaRegistryFacades mockFacades;
    private Map<String, Object> configs;

    @BeforeEach
    void setUp() {
        mockFacades = new MockGlueSchemaRegistryFacades();
        configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "test-schema");
    }

    /**
     * Tests basic Avro round-trip: RowData → serialize → deserialize → RowData. Requirement 8.1.
     */
    @Test
    void testBasicAvroRoundTrip() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType())));

        Schema avroSchema = AvroSchemaConverter.convertToSchema(rowType);

        // Build mock SchemaCoder using mock facades
        GlueSchemaRegistryOutputStreamSerializer mockSerializer =
                mockFacades.createMockOutputStreamSerializer("test-topic", configs);
        GlueSchemaRegistryInputStreamDeserializer mockDeserializer =
                mockFacades.createMockInputStreamDeserializer();

        SchemaCoder serCoder = new GlueSchemaRegistryAvroSchemaCoder(mockSerializer);
        SchemaCoder deserCoder = new GlueSchemaRegistryAvroSchemaCoder(mockDeserializer);

        // Create ser/deser schemas
        GlueSchemaRegistryAvroSerializationSchema<org.apache.avro.generic.GenericRecord>
                gsrAvroSer =
                        new GlueSchemaRegistryAvroSerializationSchema<>(
                                org.apache.avro.generic.GenericRecord.class, avroSchema, serCoder);

        AvroRowDataSerializationSchema serSchema =
                new AvroRowDataSerializationSchema(
                        rowType, gsrAvroSer, RowDataToAvroConverters.createConverter(rowType));

        AvroRowDataDeserializationSchema deserSchema =
                new AvroRowDataDeserializationSchema(
                        createDeserSchemaWithMockCoder(avroSchema, deserCoder),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        InternalTypeInfo.of(rowType));

        // Open schemas
        serSchema.open(null);
        deserSchema.open(null);

        // Create test RowData
        GenericRowData original = new GenericRowData(2);
        original.setField(0, StringData.fromString("Alice"));
        original.setField(1, 30);

        // Serialize
        byte[] serialized = serSchema.serialize(original);
        assertThat(serialized).isNotNull();
        assertThat(serialized.length).isGreaterThan(MockGlueSchemaRegistryFacades.GSR_HEADER_SIZE);

        // Deserialize
        RowData deserialized = deserSchema.deserialize(serialized);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized.getString(0).toString()).isEqualTo("Alice");
        assertThat(deserialized.getInt(1)).isEqualTo(30);
    }

    /**
     * Tests namespace bug scenario: serialize with avro.namespace override. Pre-register schema
     * with custom namespace, then serialize with patched schema. Requirement 8.3.
     */
    @Test
    void testNamespaceBugScenario() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType())));

        // Auto-generated schema has namespace "org.apache.flink.avro.generated"
        Schema autoGenerated = AvroSchemaConverter.convertToSchema(rowType);
        assertThat(autoGenerated.getNamespace()).isEqualTo("org.apache.flink.avro.generated");

        // Patch schema with custom namespace (simulating avro.namespace option)
        String customNamespace = "com.example.myapp";
        Schema patchedSchema = AvroSchemaPatcher.patchSchema(autoGenerated, customNamespace, null);
        assertThat(patchedSchema.getNamespace()).isEqualTo(customNamespace);
        assertThat(patchedSchema.getFields()).hasSameSizeAs(autoGenerated.getFields());

        // Build mock SchemaCoder with patched schema
        GlueSchemaRegistryOutputStreamSerializer mockSerializer =
                mockFacades.createMockOutputStreamSerializer("test-topic", configs);
        GlueSchemaRegistryInputStreamDeserializer mockDeserializer =
                mockFacades.createMockInputStreamDeserializer();

        SchemaCoder serCoder = new GlueSchemaRegistryAvroSchemaCoder(mockSerializer);
        SchemaCoder deserCoder = new GlueSchemaRegistryAvroSchemaCoder(mockDeserializer);

        // Create ser schema with patched schema
        GlueSchemaRegistryAvroSerializationSchema<org.apache.avro.generic.GenericRecord>
                gsrAvroSer =
                        new GlueSchemaRegistryAvroSerializationSchema<>(
                                org.apache.avro.generic.GenericRecord.class,
                                patchedSchema,
                                serCoder);

        AvroRowDataSerializationSchema serSchema =
                new AvroRowDataSerializationSchema(
                        rowType, gsrAvroSer, RowDataToAvroConverters.createConverter(rowType));

        // Create deser schema with patched schema
        AvroRowDataDeserializationSchema deserSchema =
                new AvroRowDataDeserializationSchema(
                        createDeserSchemaWithMockCoder(patchedSchema, deserCoder),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        InternalTypeInfo.of(rowType));

        serSchema.open(null);
        deserSchema.open(null);

        // Create test RowData
        GenericRowData original = new GenericRowData(2);
        original.setField(0, StringData.fromString("Bob"));
        original.setField(1, 25);

        // Serialize with patched schema
        byte[] serialized = serSchema.serialize(original);
        assertThat(serialized).isNotNull();

        // Deserialize — should succeed despite different namespace
        RowData deserialized = deserSchema.deserialize(serialized);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized.getString(0).toString()).isEqualTo("Bob");
        assertThat(deserialized.getInt(1)).isEqualTo(25);
    }

    /**
     * Creates a GlueSchemaRegistryAvroDeserializationSchema with a mock SchemaCoder injected via
     * reflection (the schemaCoder field is private in the parent class).
     */
    private static GlueSchemaRegistryAvroDeserializationSchema<
                    org.apache.avro.generic.GenericRecord>
            createDeserSchemaWithMockCoder(Schema schema, SchemaCoder mockCoder) {
        // Create a real deser schema (configs won't be used since we override schemaCoder)
        Map<String, Object> dummyConfigs = new HashMap<>();
        dummyConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        GlueSchemaRegistryAvroDeserializationSchema<org.apache.avro.generic.GenericRecord>
                deserSchema =
                        GlueSchemaRegistryAvroDeserializationSchema.forGeneric(
                                schema, dummyConfigs);

        // Use reflection to inject the mock SchemaCoder
        try {
            Class<?> clazz = deserSchema.getClass();
            while (clazz != null) {
                try {
                    Field field = clazz.getDeclaredField("schemaCoder");
                    field.setAccessible(true);
                    field.set(deserSchema, mockCoder);
                    return deserSchema;
                } catch (NoSuchFieldException e) {
                    clazz = clazz.getSuperclass();
                }
            }
            throw new RuntimeException("Could not find schemaCoder field");
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to inject mock SchemaCoder", e);
        }
    }
}
