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
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.apache.avro.Schema;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for Avro serialization round-trip with mock GSR facades.
 *
 * <p><b>Property 3: Avro serialization round-trip</b>
 *
 * <p><b>Validates: Requirements 1.4, 1.5, 8.1</b>
 */
@Tag("Feature: gsr-flink-sql-formats, Property 3: Avro serialization round-trip")
class AvroRoundTripPropertyTest {

    /**
     * For any valid RowData matching a given RowType, serializing via the Avro encoding format
     * (with mock GSR facades) and then deserializing should produce equivalent RowData.
     */
    @Property(tries = 100)
    void avroRoundTripPreservesData(@ForAll("rowDataWithType") RowDataWithType input)
            throws Exception {
        RowType rowType = input.rowType;
        RowData original = input.rowData;

        Schema avroSchema = AvroSchemaConverter.convertToSchema(rowType);

        // Create mock facades
        MockGlueSchemaRegistryFacades mockFacades = new MockGlueSchemaRegistryFacades();
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "test-schema");

        GlueSchemaRegistryOutputStreamSerializer mockSerializer =
                mockFacades.createMockOutputStreamSerializer("test-topic", configs);
        GlueSchemaRegistryInputStreamDeserializer mockDeserializer =
                mockFacades.createMockInputStreamDeserializer();

        SchemaCoder serCoder = new GlueSchemaRegistryAvroSchemaCoder(mockSerializer);
        SchemaCoder deserCoder = new GlueSchemaRegistryAvroSchemaCoder(mockDeserializer);

        // Create serialization schema
        GlueSchemaRegistryAvroSerializationSchema<org.apache.avro.generic.GenericRecord>
                gsrAvroSer =
                        new GlueSchemaRegistryAvroSerializationSchema<>(
                                org.apache.avro.generic.GenericRecord.class,
                                avroSchema,
                                serCoder);

        AvroRowDataSerializationSchema serSchema =
                new AvroRowDataSerializationSchema(
                        rowType,
                        gsrAvroSer,
                        RowDataToAvroConverters.createConverter(rowType));

        // Create deserialization schema with mock coder via reflection
        GlueSchemaRegistryAvroDeserializationSchema<org.apache.avro.generic.GenericRecord>
                gsrAvroDe =
                        GlueSchemaRegistryAvroDeserializationSchema.forGeneric(
                                avroSchema, configs);
        injectSchemaCoder(gsrAvroDe, deserCoder);

        AvroRowDataDeserializationSchema deserSchema =
                new AvroRowDataDeserializationSchema(
                        gsrAvroDe,
                        AvroToRowDataConverters.createRowConverter(rowType),
                        InternalTypeInfo.of(rowType));

        serSchema.open(null);
        deserSchema.open(null);

        // Serialize
        byte[] serialized = serSchema.serialize(original);
        assertThat(serialized).isNotNull();

        // Deserialize
        RowData deserialized = deserSchema.deserialize(serialized);
        assertThat(deserialized).isNotNull();

        // Verify equivalence field by field
        assertRowDataEquals(original, deserialized, rowType);
    }

    /** Injects a mock SchemaCoder into a deser schema via reflection. */
    private static void injectSchemaCoder(Object target, SchemaCoder coder) {
        try {
            Class<?> clazz = target.getClass();
            while (clazz != null) {
                try {
                    Field field = clazz.getDeclaredField("schemaCoder");
                    field.setAccessible(true);
                    field.set(target, coder);
                    return;
                } catch (NoSuchFieldException e) {
                    clazz = clazz.getSuperclass();
                }
            }
            throw new RuntimeException("Could not find schemaCoder field");
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to inject mock SchemaCoder", e);
        }
    }

    /** Compares two RowData instances field by field based on the RowType. */
    private void assertRowDataEquals(RowData expected, RowData actual, RowType rowType) {
        assertThat(actual.getArity()).isEqualTo(expected.getArity());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            LogicalType fieldType = rowType.getTypeAt(i);
            // Avro treats null strings as null, null ints/bools/doubles as null
            if (expected.isNullAt(i)) {
                assertThat(actual.isNullAt(i)).isTrue();
                continue;
            }
            if (fieldType instanceof VarCharType) {
                assertThat(actual.getString(i).toString())
                        .isEqualTo(expected.getString(i).toString());
            } else if (fieldType instanceof IntType) {
                assertThat(actual.getInt(i)).isEqualTo(expected.getInt(i));
            } else if (fieldType instanceof BooleanType) {
                assertThat(actual.getBoolean(i)).isEqualTo(expected.getBoolean(i));
            } else if (fieldType instanceof DoubleType) {
                assertThat(actual.getDouble(i)).isEqualTo(expected.getDouble(i));
            }
        }
    }

    // --- Generators ---

    @Provide
    Arbitrary<RowDataWithType> rowDataWithType() {
        // Fixed schema with STRING, INT, BOOLEAN, DOUBLE fields
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType()),
                                new RowType.RowField("active", new BooleanType()),
                                new RowType.RowField("score", new DoubleType())));

        return Arbitraries.of(rowType)
                .flatMap(rt -> generateRowData(rt).map(rd -> new RowDataWithType(rt, rd)));
    }

    private Arbitrary<RowData> generateRowData(RowType rowType) {
        Arbitrary<String> strings =
                Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(50);
        Arbitrary<Integer> ints = Arbitraries.integers().between(-10000, 10000);
        Arbitrary<Boolean> bools = Arbitraries.of(true, false);
        Arbitrary<Double> doubles =
                Arbitraries.doubles().between(-1e6, 1e6).ofScale(4);

        return strings.flatMap(
                name ->
                        ints.flatMap(
                                age ->
                                        bools.flatMap(
                                                active ->
                                                        doubles.map(
                                                                score -> {
                                                                    GenericRowData row =
                                                                            new GenericRowData(4);
                                                                    row.setField(
                                                                            0,
                                                                            StringData.fromString(
                                                                                    name));
                                                                    row.setField(1, age);
                                                                    row.setField(2, active);
                                                                    row.setField(3, score);
                                                                    return (RowData) row;
                                                                }))));
    }

    /** Holder for a RowData and its corresponding RowType. */
    static class RowDataWithType {
        final RowType rowType;
        final RowData rowData;

        RowDataWithType(RowType rowType, RowData rowData) {
            this.rowType = rowType;
            this.rowData = rowData;
        }
    }
}
