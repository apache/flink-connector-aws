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

package org.apache.flink.formats.json.glue.schema.registry;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for JSON serialization round-trip with GSR header handling.
 *
 * <p><b>Property 4: JSON serialization round-trip</b>
 *
 * <p><b>Validates: Requirements 3.4, 3.5, 8.1</b>
 */
@Tag("Feature: gsr-flink-sql-formats, Property 4: JSON serialization round-trip")
class GsrJsonRoundTripPropertyTest {

    private static final int GSR_HEADER_SIZE = 18;

    /**
     * For any valid RowData matching a given RowType, serializing to JSON bytes, prepending a mock
     * GSR header, then stripping the header and deserializing should produce equivalent RowData.
     */
    @Property(tries = 100)
    void jsonRoundTripPreservesData(@ForAll("rowDataWithType") RowDataWithType input)
            throws Exception {
        RowType rowType = input.rowType;
        RowData original = input.rowData;

        // Serialize RowData to JSON bytes using Flink's JSON serializer
        JsonRowDataSerializationSchema jsonSer =
                new JsonRowDataSerializationSchema(
                        rowType,
                        TimestampFormat.SQL,
                        JsonFormatOptions.MapNullKeyMode.LITERAL,
                        "null",
                        false,
                        false);
        jsonSer.open(null);
        byte[] jsonBytes = jsonSer.serialize(original);

        // Simulate GSR encoding: prepend 18-byte mock header
        byte[] gsrEncoded = prependMockGsrHeader(jsonBytes);

        // Simulate GSR decoding: strip 18-byte header
        byte[] stripped = Arrays.copyOfRange(gsrEncoded, GSR_HEADER_SIZE, gsrEncoded.length);

        // Deserialize JSON bytes back to RowData
        JsonRowDataDeserializationSchema jsonDeser =
                new JsonRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), false, false, TimestampFormat.SQL);
        jsonDeser.open(null);
        RowData deserialized = jsonDeser.deserialize(stripped);

        // Verify equivalence field by field
        assertThat(deserialized).isNotNull();
        assertRowDataEquals(original, deserialized, rowType);
    }

    /** Creates a mock 18-byte GSR header and prepends it to the payload. */
    private byte[] prependMockGsrHeader(byte[] payload) {
        UUID schemaId = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(GSR_HEADER_SIZE + payload.length);
        buffer.put((byte) 0x03); // header version
        buffer.put((byte) 0x00); // no compression
        buffer.putLong(schemaId.getMostSignificantBits());
        buffer.putLong(schemaId.getLeastSignificantBits());
        buffer.put(payload);
        return buffer.array();
    }

    /** Compares two RowData instances field by field based on the RowType. */
    private void assertRowDataEquals(RowData expected, RowData actual, RowType rowType) {
        assertThat(actual.getArity()).isEqualTo(expected.getArity());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            LogicalType fieldType = rowType.getTypeAt(i);
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
        Arbitrary<String> strings = Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(50);
        Arbitrary<Integer> ints = Arbitraries.integers().between(-10000, 10000);
        Arbitrary<Boolean> bools = Arbitraries.of(true, false);
        Arbitrary<Double> doubles = Arbitraries.doubles().between(-1e6, 1e6).ofScale(4);

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
