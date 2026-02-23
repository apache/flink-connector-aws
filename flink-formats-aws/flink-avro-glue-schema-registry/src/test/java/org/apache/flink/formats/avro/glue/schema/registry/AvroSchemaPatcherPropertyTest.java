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

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.Tuple;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link AvroSchemaPatcher}.
 *
 * <p><b>Property 2: Avro schema patching preserves fields with overridden namespace and name</b>
 *
 * <p><b>Validates: Requirements 2.3, 2.4, 2.9</b>
 */
@Tag("Feature: gsr-flink-sql-formats, Property 2: Avro schema patching preserves fields with"
        + " overridden namespace and name")
class AvroSchemaPatcherPropertyTest {

    /**
     * For any valid Flink RowType and for any non-empty namespace and record name strings, patching
     * the auto-generated Avro schema should produce a schema where the namespace and record name
     * match the provided values, and all fields are preserved (with nested records also patched).
     */
    @Property(tries = 100)
    void patchSchemaPreservesFieldsWithOverriddenNamespaceAndName(
            @ForAll("rowTypes") RowType rowType,
            @ForAll("namespaces") String namespace,
            @ForAll("recordNames") String recordName) {

        Schema original = AvroSchemaConverter.convertToSchema(rowType);
        Schema patched = AvroSchemaPatcher.patchSchema(original, namespace, recordName);

        assertThat(patched.getNamespace()).isEqualTo(namespace);
        assertThat(patched.getName()).isEqualTo(recordName);
        assertThat(patched.getFields()).hasSameSizeAs(original.getFields());

        for (int i = 0; i < original.getFields().size(); i++) {
            Schema.Field originalField = original.getFields().get(i);
            Schema.Field patchedField = patched.getFields().get(i);
            assertThat(patchedField.name()).isEqualTo(originalField.name());
            // For nested records, the schema will be patched too, so we verify structure
            assertSchemaStructureMatches(originalField.schema(), patchedField.schema(), namespace);
        }
    }

    /**
     * Verifies that the patched schema has the same structure as the original,
     * with nested records having the new namespace.
     */
    private void assertSchemaStructureMatches(Schema original, Schema patched, String expectedNamespace) {
        assertThat(patched.getType()).isEqualTo(original.getType());

        switch (original.getType()) {
            case RECORD:
                assertThat(patched.getNamespace()).isEqualTo(expectedNamespace);
                assertThat(patched.getFields()).hasSameSizeAs(original.getFields());
                for (int i = 0; i < original.getFields().size(); i++) {
                    assertSchemaStructureMatches(
                            original.getFields().get(i).schema(),
                            patched.getFields().get(i).schema(),
                            expectedNamespace);
                }
                break;
            case ARRAY:
                assertSchemaStructureMatches(original.getElementType(), patched.getElementType(), expectedNamespace);
                break;
            case MAP:
                assertSchemaStructureMatches(original.getValueType(), patched.getValueType(), expectedNamespace);
                break;
            case UNION:
                assertThat(patched.getTypes()).hasSameSizeAs(original.getTypes());
                for (int i = 0; i < original.getTypes().size(); i++) {
                    assertSchemaStructureMatches(
                            original.getTypes().get(i),
                            patched.getTypes().get(i),
                            expectedNamespace);
                }
                break;
            default:
                // Primitive types should be equal
                assertThat(patched).isEqualTo(original);
        }
    }

    /**
     * When only namespace is provided (recordName is null), the record name should remain
     * unchanged.
     */
    @Property(tries = 100)
    void patchSchemaWithOnlyNamespacePreservesRecordName(
            @ForAll("rowTypes") RowType rowType,
            @ForAll("namespaces") String namespace) {

        Schema original = AvroSchemaConverter.convertToSchema(rowType);
        Schema patched = AvroSchemaPatcher.patchSchema(original, namespace, null);

        assertThat(patched.getNamespace()).isEqualTo(namespace);
        assertThat(patched.getName()).isEqualTo(original.getName());
        assertThat(patched.getFields()).hasSameSizeAs(original.getFields());
    }

    /**
     * When only recordName is provided (namespace is null), the namespace should remain unchanged.
     */
    @Property(tries = 100)
    void patchSchemaWithOnlyRecordNamePreservesNamespace(
            @ForAll("rowTypes") RowType rowType,
            @ForAll("recordNames") String recordName) {

        Schema original = AvroSchemaConverter.convertToSchema(rowType);
        Schema patched = AvroSchemaPatcher.patchSchema(original, null, recordName);

        assertThat(patched.getNamespace()).isEqualTo(original.getNamespace());
        assertThat(patched.getName()).isEqualTo(recordName);
        assertThat(patched.getFields()).hasSameSizeAs(original.getFields());
    }

    /**
     * When both namespace and recordName are null, the original schema should be returned
     * unchanged.
     */
    @Property(tries = 100)
    void patchSchemaWithNullOverridesReturnsOriginal(@ForAll("rowTypes") RowType rowType) {

        Schema original = AvroSchemaConverter.convertToSchema(rowType);
        Schema patched = AvroSchemaPatcher.patchSchema(original, null, null);

        assertThat(patched).isSameAs(original);
    }

    @Provide
    Arbitrary<RowType> rowTypes() {
        return rowTypesWithDepth(0);
    }

    /**
     * Generates RowTypes with nested complex types up to a maximum depth.
     */
    private Arbitrary<RowType> rowTypesWithDepth(int depth) {
        Arbitrary<Integer> fieldCount = Arbitraries.integers().between(1, 5);
        return fieldCount.flatMap(
                count -> {
                    Arbitrary<List<LogicalType>> types = logicalTypesWithDepth(depth).list().ofSize(count);
                    return types.map(
                            typeList -> {
                                List<RowType.RowField> fields =
                                        IntStream.range(0, typeList.size())
                                                .mapToObj(
                                                        i ->
                                                                new RowType.RowField(
                                                                        "f" + i, typeList.get(i)))
                                                .collect(Collectors.toList());
                                return new RowType(false, fields);
                            });
                });
    }

    /**
     * Generates LogicalTypes including primitives and complex types (ARRAY, MAP, ROW) with depth control.
     */
    private Arbitrary<LogicalType> logicalTypesWithDepth(int depth) {
        // Primitive types - always available
        Arbitrary<LogicalType> primitives = Arbitraries.of(
                (LogicalType) new VarCharType(VarCharType.MAX_LENGTH),
                new IntType(),
                new BigIntType(),
                new BooleanType(),
                new FloatType(),
                new DoubleType());

        // At max depth, only return primitives
        if (depth >= 2) {
            return primitives;
        }

        // Complex types with nested structures
        Arbitrary<LogicalType> arrayType = logicalTypesWithDepth(depth + 1)
                .map(ArrayType::new);

        Arbitrary<LogicalType> mapType = logicalTypesWithDepth(depth + 1)
                .map(valueType -> new MapType(new VarCharType(VarCharType.MAX_LENGTH), valueType));

        Arbitrary<LogicalType> nestedRowType = rowTypesWithDepth(depth + 1)
                .map(rt -> (LogicalType) rt);

        // Mix primitives and complex types with higher weight on primitives
        return Arbitraries.frequencyOf(
                Tuple.of(6, primitives),
                Tuple.of(1, arrayType),
                Tuple.of(1, mapType),
                Tuple.of(2, nestedRowType));
    }

    @Provide
    Arbitrary<String> namespaces() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .ofMinLength(1)
                .ofMaxLength(20)
                .flatMap(
                        first ->
                                Arbitraries.strings()
                                        .withCharRange('a', 'z')
                                        .ofMinLength(1)
                                        .ofMaxLength(20)
                                        .map(second -> first + "." + second));
    }

    @Provide
    Arbitrary<String> recordNames() {
        // Avro record names must start with a letter and contain only alphanumeric + underscore
        return Arbitraries.strings()
                .withCharRange('A', 'Z')
                .ofLength(1)
                .flatMap(
                        first ->
                                Arbitraries.strings()
                                        .withCharRange('a', 'z')
                                        .ofMinLength(1)
                                        .ofMaxLength(15)
                                        .map(rest -> first + rest));
    }

    /**
     * Test that nested record types are also patched with the same namespace.
     */
    @Test
    void patchSchemaRecursivelyPatchesNestedRecords() {
        // Create a RowType with nested ROW (record within record)
        RowType nestedRowType = new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
                        new RowType.RowField("age", new IntType())));

        RowType outerRowType = new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("id", new VarCharType(VarCharType.MAX_LENGTH)),
                        new RowType.RowField("customer", nestedRowType)));

        Schema original = AvroSchemaConverter.convertToSchema(outerRowType);
        Schema patched = AvroSchemaPatcher.patchSchema(original, "com.example.orders", "OrderEvent");

        // Verify root record is patched
        assertThat(patched.getNamespace()).isEqualTo("com.example.orders");
        assertThat(patched.getName()).isEqualTo("OrderEvent");

        // Verify nested record is also patched with the same namespace
        Schema customerField = patched.getField("customer").schema();
        // Handle union type (nullable)
        if (customerField.getType() == Schema.Type.UNION) {
            customerField = customerField.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
        }
        assertThat(customerField.getNamespace()).isEqualTo("com.example.orders");
    }

    /**
     * Test that ARRAY of records has nested records patched.
     */
    @Test
    void patchSchemaRecursivelyPatchesArrayOfRecords() {
        // Create a RowType with ARRAY<ROW<...>>
        RowType itemRowType = new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("product_name", new VarCharType(VarCharType.MAX_LENGTH)),
                        new RowType.RowField("quantity", new IntType())));

        RowType outerRowType = new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("order_id", new VarCharType(VarCharType.MAX_LENGTH)),
                        new RowType.RowField("items", new ArrayType(itemRowType))));

        Schema original = AvroSchemaConverter.convertToSchema(outerRowType);
        Schema patched = AvroSchemaPatcher.patchSchema(original, "com.example.orders", "OrderEvent");

        // Verify root record is patched
        assertThat(patched.getNamespace()).isEqualTo("com.example.orders");
        assertThat(patched.getName()).isEqualTo("OrderEvent");

        // Verify array element record is also patched
        Schema itemsField = patched.getField("items").schema();
        // Handle union type (nullable)
        if (itemsField.getType() == Schema.Type.UNION) {
            itemsField = itemsField.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.ARRAY)
                    .findFirst()
                    .orElseThrow();
        }
        Schema elementSchema = itemsField.getElementType();
        // Handle union type for element
        if (elementSchema.getType() == Schema.Type.UNION) {
            elementSchema = elementSchema.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
        }
        assertThat(elementSchema.getNamespace()).isEqualTo("com.example.orders");
    }

    /**
     * Test that MAP values with record types are patched.
     */
    @Test
    void patchSchemaRecursivelyPatchesMapValues() {
        // Create a RowType with MAP<STRING, ROW<...>>
        RowType valueRowType = new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("key", new VarCharType(VarCharType.MAX_LENGTH)),
                        new RowType.RowField("value", new IntType())));

        RowType outerRowType = new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("id", new VarCharType(VarCharType.MAX_LENGTH)),
                        new RowType.RowField("metadata", new MapType(
                                new VarCharType(VarCharType.MAX_LENGTH), valueRowType))));

        Schema original = AvroSchemaConverter.convertToSchema(outerRowType);
        Schema patched = AvroSchemaPatcher.patchSchema(original, "com.example.data", "DataRecord");

        // Verify root record is patched
        assertThat(patched.getNamespace()).isEqualTo("com.example.data");
        assertThat(patched.getName()).isEqualTo("DataRecord");

        // Verify map value record is also patched
        Schema metadataField = patched.getField("metadata").schema();
        // Handle union type (nullable)
        if (metadataField.getType() == Schema.Type.UNION) {
            metadataField = metadataField.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.MAP)
                    .findFirst()
                    .orElseThrow();
        }
        Schema valueSchema = metadataField.getValueType();
        // Handle union type for value
        if (valueSchema.getType() == Schema.Type.UNION) {
            valueSchema = valueSchema.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
        }
        assertThat(valueSchema.getNamespace()).isEqualTo("com.example.data");
    }
}
