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

package org.apache.flink.formats.json.glue.schema.registry;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link JsonSchemaConverter}, exercising the review fixes over the initial candidate.
 *
 * <p>Covered fixes:
 *
 * <ul>
 *   <li>JSON-escaping of field names (top-level and nested)
 *   <li>fail-fast on genuinely unsupported types instead of silent {@code "string"}
 *   <li>{@code "format"} keywords for temporal types and {@code "contentEncoding"} for binary
 *   <li>{@code "required"} array for {@code NOT NULL} fields
 *   <li>nullable scalars expressed as a {@code ["type","null"]} union without brittle string
 *       surgery
 *   <li>MAP keys constrained to character types
 * </ul>
 */
class JsonSchemaConverterTest {

    private static RowType row(RowType.RowField... fields) {
        return new RowType(false, Arrays.asList(fields));
    }

    private static RowType.RowField f(String name, LogicalType type) {
        return new RowType.RowField(name, type);
    }

    @Test
    void testScalarTypesAndFormats() {
        RowType rowType =
                row(
                        f("s", new VarCharType(false, VarCharType.MAX_LENGTH)),
                        f("i", new IntType(false)),
                        f("d", new DoubleType(false)),
                        f("dec", new DecimalType(false, 10, 2)),
                        f("b", new BooleanType(false)),
                        f("dt", new DateType(false)),
                        f("tm", new TimeType(false, 3)),
                        f("ts", new TimestampType(false, 3)),
                        f("tsltz", new LocalZonedTimestampType(false, 3)),
                        f("bin", new BinaryType(false, 16)));

        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);

        assertThat(schema).contains("\"$schema\":\"http://json-schema.org/draft-07/schema#\"");
        // Closed content model: without additionalProperties:false, GSR's BACKWARD
        // compatibility check rejects any added property, blocking schema evolution.
        assertThat(schema)
                .contains("\"type\":\"object\",\"additionalProperties\":false,\"properties\":{");
        assertThat(schema).contains("\"s\":{\"type\":\"string\"}");
        assertThat(schema).contains("\"i\":{\"type\":\"integer\"}");
        assertThat(schema).contains("\"d\":{\"type\":\"number\"}");
        assertThat(schema).contains("\"dec\":{\"type\":\"number\"}");
        assertThat(schema).contains("\"b\":{\"type\":\"boolean\"}");
        assertThat(schema).contains("\"dt\":{\"type\":\"string\",\"format\":\"date\"}");
        assertThat(schema).contains("\"tm\":{\"type\":\"string\",\"format\":\"time\"}");
        assertThat(schema).contains("\"ts\":{\"type\":\"string\",\"format\":\"date-time\"}");
        assertThat(schema).contains("\"tsltz\":{\"type\":\"string\",\"format\":\"date-time\"}");
        assertThat(schema).contains("\"bin\":{\"type\":\"string\",\"contentEncoding\":\"base64\"}");
    }

    @Test
    void testRequiredArrayForNotNullFields() {
        RowType rowType =
                row(
                        f("id", new IntType(false)),
                        f("opt", new VarCharType(true, VarCharType.MAX_LENGTH)));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema).contains("\"required\":[\"id\"]");
    }

    @Test
    void testNullableScalarBecomesTypeUnion() {
        RowType rowType = row(f("opt", new VarCharType(true, VarCharType.MAX_LENGTH)));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema).contains("\"opt\":{\"type\":[\"string\",\"null\"]}");
        // No required array when every field is nullable.
        assertThat(schema).doesNotContain("\"required\"");
    }

    @Test
    void testNullableTemporalKeepsFormat() {
        RowType rowType = row(f("ts", new TimestampType(true, 3)));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema)
                .contains("\"ts\":{\"type\":[\"string\",\"null\"],\"format\":\"date-time\"}");
    }

    @Test
    void testFieldNamesAreJsonEscaped() {
        // Column name contains a double-quote and a backslash.
        RowType rowType = row(f("a\"b\\c", new IntType(false)));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        // The property key must be escaped: a\"b\\c
        assertThat(schema).contains("\"a\\\"b\\\\c\":{\"type\":\"integer\"}");
    }

    @Test
    void testArrayAndStringKeyedMap() {
        RowType rowType =
                row(
                        f("tags", new ArrayType(false, new IntType(false))),
                        f(
                                "attrs",
                                new MapType(
                                        false,
                                        new VarCharType(false, VarCharType.MAX_LENGTH),
                                        new VarCharType(true, VarCharType.MAX_LENGTH))));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema).contains("\"type\":\"array\",\"items\":{\"type\":\"integer\"}");
        assertThat(schema).contains("\"type\":\"object\",\"additionalProperties\":");
    }

    @Test
    void testNestedRowFieldNamesEscapedAndStructured() {
        RowType nested = row(f("street", new VarCharType(false, VarCharType.MAX_LENGTH)));
        RowType rowType = row(f("addr", nested));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema)
                .contains(
                        "\"addr\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"street\":{\"type\":\"string\"}}");
    }

    // ---- Composition: parameterized / nullable / structured types inside containers ----------

    /** Element nullability and formats must survive inside an array, not only at top level. */
    @Test
    void testArrayOfNullableTimestampsKeepsUnionAndFormat() {
        RowType rowType = row(f("times", new ArrayType(false, new TimestampType(true, 3))));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema)
                .contains(
                        "\"times\":{\"type\":\"array\",\"items\":{\"type\":[\"string\",\"null\"],\"format\":\"date-time\"}}");
    }

    @Test
    void testArrayOfRowsCarriesNestedRequiredList() {
        RowType item =
                row(
                        f("sku", new VarCharType(false, VarCharType.MAX_LENGTH)),
                        f("qty", new IntType(true)));
        RowType rowType = row(f("items", new ArrayType(false, item)));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema)
                .contains("\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\"")
                .contains("\"sku\":{\"type\":\"string\"}")
                .contains("\"qty\":{\"type\":[\"integer\",\"null\"]}")
                .contains("\"required\":[\"sku\"]");
        // Only the top-level 'items' and the nested 'sku' are required: exactly two lists.
        assertThat(schema.split("\"required\"", -1)).hasSize(3);
    }

    @Test
    void testArrayOfArraysAndMapOfArrays() {
        RowType rowType =
                row(
                        f(
                                "matrix",
                                new ArrayType(false, new ArrayType(false, new DoubleType(false)))),
                        f(
                                "byKey",
                                new MapType(
                                        false,
                                        new VarCharType(false, VarCharType.MAX_LENGTH),
                                        new ArrayType(false, new DecimalType(false, 10, 2)))));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema)
                .contains(
                        "\"matrix\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}")
                .contains(
                        "\"byKey\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}");
    }

    @Test
    void testRowInRowInRowKeepsInnermostRequired() {
        RowType inner = row(f("c", new IntType(false)));
        RowType middle = row(f("b", inner));
        RowType rowType = row(f("a", middle));
        String schema = JsonSchemaConverter.convertToJsonSchema(rowType);
        assertThat(schema)
                .contains("\"c\":{\"type\":\"integer\"}")
                .contains("\"required\":[\"c\"]")
                .contains("\"required\":[\"b\"]")
                .contains("\"required\":[\"a\"]");
    }

    /** Unsupported types nested inside a container must fail as clearly as at top level. */
    @Test
    void testUnsupportedTypeInsideContainerFailsFast() {
        RowType rowType =
                row(
                        f(
                                "bad",
                                new ArrayType(
                                        false,
                                        new MapType(
                                                false,
                                                new IntType(false),
                                                new VarCharType(false, VarCharType.MAX_LENGTH)))));
        assertThatThrownBy(() -> JsonSchemaConverter.convertToJsonSchema(rowType))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testUnsupportedTypeFailsFast() {
        RowType rowType = row(f("m", new MultisetType(new IntType())));
        assertThatThrownBy(() -> JsonSchemaConverter.convertToJsonSchema(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support");
    }

    @Test
    void testMapWithNonStringKeyFailsFast() {
        RowType rowType =
                row(f("m", new MapType(new IntType(), new VarCharType(VarCharType.MAX_LENGTH))));
        assertThatThrownBy(() -> JsonSchemaConverter.convertToJsonSchema(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("MAP keys must be");
    }

    @Test
    void testTimestampWithTimeZoneFailsFast() {
        // TIMESTAMP WITH TIME ZONE is not supported by Flink's JSON ser/de.
        List<RowType.RowField> fields =
                Collections.singletonList(
                        f(
                                "zts",
                                new org.apache.flink.table.types.logical.ZonedTimestampType(
                                        false, 3)));
        RowType rowType = new RowType(false, fields);
        assertThatThrownBy(() -> JsonSchemaConverter.convertToJsonSchema(rowType))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
