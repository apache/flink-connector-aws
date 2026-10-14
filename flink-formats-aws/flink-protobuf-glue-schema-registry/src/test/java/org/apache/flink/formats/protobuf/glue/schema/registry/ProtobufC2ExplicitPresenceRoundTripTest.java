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

package org.apache.flink.formats.protobuf.glue.schema.registry;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip regression tests for review finding <b>C2</b>: proto3 <b>explicit presence</b> for
 * NULLABLE columns.
 *
 * <p>Before the fix the generated schema used implicit-presence (plain proto3) scalars for every
 * column, so the reader could not tell an unset field from a field set to its type default. A
 * nullable column holding {@code 0}, {@code ""} or {@code false} was therefore indistinguishable
 * from a null column on the wire, and vice-versa. {@link ProtobufSchemaConverter} now emits
 * nullable scalars as proto3 {@code optional} (synthetic-oneof) fields and {@link
 * ProtobufToRowDataConverter} consults {@code hasField()}, so:
 *
 * <ul>
 *   <li>a {@code null} value round-trips as {@code null} (field left unset), and
 *   <li>a set type-default value ({@code 0} / {@code ""} / {@code false}) round-trips as that
 *       <b>value</b> — it is NOT collapsed to {@code null}.
 * </ul>
 *
 * <p>The round-trip runs RowData → {@link RowDataToProtobufConverter} (write) → {@link
 * GsrProtobufRowDataDeserializationSchema} (read, via a fake GSR reader that resolves the writer
 * schema and returns the raw payload) → RowData, so it exercises the same schema/converter code the
 * production decode path uses without a live registry.
 */
class ProtobufC2ExplicitPresenceRoundTripTest {

    private static final String SCHEMA_NAME = "TestMessage";

    /** All three columns are NULLABLE: an int, a string and a boolean. */
    private static RowType nullableRowType() {
        return new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("i", new IntType(true)),
                        new RowType.RowField("s", new VarCharType(true, VarCharType.MAX_LENGTH)),
                        new RowType.RowField("b", new BooleanType(true))));
    }

    /**
     * C2 null side: a NULLABLE column that is {@code null} at write time is emitted as an unset
     * proto3 explicit-presence field and decodes back to {@code null} — not to the type default.
     */
    @Test
    void testNullableColumnsNullRoundTripAsNull() throws Exception {
        RowType rowType = nullableRowType();

        GenericRowData original = new GenericRowData(3);
        original.setField(0, null);
        original.setField(1, null);
        original.setField(2, null);

        RowData decoded = roundTrip(original, rowType);

        assertThat(decoded).isNotNull();
        assertThat(decoded.isNullAt(0)).isTrue();
        assertThat(decoded.isNullAt(1)).isTrue();
        assertThat(decoded.isNullAt(2)).isTrue();
    }

    /**
     * C2 value side (the crux of the finding): a NULLABLE column set to its <b>type default</b> —
     * {@code 0} for int32, {@code ""} for string, {@code false} for bool — round-trips as that
     * value, NOT collapsed to {@code null}. This is only possible with explicit presence.
     */
    @Test
    void testNullableColumnsTypeDefaultValuesRoundTripAsValues() throws Exception {
        RowType rowType = nullableRowType();

        GenericRowData original = new GenericRowData(3);
        original.setField(0, 0);
        original.setField(1, StringData.fromString(""));
        original.setField(2, false);

        RowData decoded = roundTrip(original, rowType);

        assertThat(decoded).isNotNull();
        // The distinguishing assertions: values are present, not null.
        assertThat(decoded.isNullAt(0)).isFalse();
        assertThat(decoded.getInt(0)).isEqualTo(0);
        assertThat(decoded.isNullAt(1)).isFalse();
        assertThat(decoded.getString(1).toString()).isEqualTo("");
        assertThat(decoded.isNullAt(2)).isFalse();
        assertThat(decoded.getBoolean(2)).isFalse();
    }

    /**
     * C2 mixed row: null and set-default values coexist in the same record and each retains its own
     * presence — proving presence is tracked per field, not row-wide.
     */
    @Test
    void testMixedNullAndTypeDefaultValuesRoundTrip() throws Exception {
        RowType rowType = nullableRowType();

        GenericRowData original = new GenericRowData(3);
        original.setField(0, 0); // set default -> stays 0
        original.setField(1, null); // null -> stays null
        original.setField(2, false); // set default -> stays false

        RowData decoded = roundTrip(original, rowType);

        assertThat(decoded).isNotNull();
        assertThat(decoded.isNullAt(0)).isFalse();
        assertThat(decoded.getInt(0)).isEqualTo(0);
        assertThat(decoded.isNullAt(1)).isTrue();
        assertThat(decoded.isNullAt(2)).isFalse();
        assertThat(decoded.getBoolean(2)).isFalse();
    }

    /**
     * Sanity check that non-default set values also survive, so the value-side assertions above are
     * not vacuously passing on a decode path that ignores the payload.
     */
    @Test
    void testNullableColumnsNonDefaultValuesRoundTrip() throws Exception {
        RowType rowType = nullableRowType();

        GenericRowData original = new GenericRowData(3);
        original.setField(0, 42);
        original.setField(1, StringData.fromString("hello"));
        original.setField(2, true);

        RowData decoded = roundTrip(original, rowType);

        assertThat(decoded).isNotNull();
        assertThat(decoded.getInt(0)).isEqualTo(42);
        assertThat(decoded.getString(1).toString()).isEqualTo("hello");
        assertThat(decoded.getBoolean(2)).isTrue();
    }

    /**
     * RowData → Protobuf bytes (descriptor built from {@code rowType}) → {@link
     * GsrProtobufRowDataDeserializationSchema} (fed the writer schema derived from {@code rowType}
     * and the raw payload via a fake reader) → RowData.
     */
    private static RowData roundTrip(RowData original, RowType rowType) throws Exception {
        DynamicMessage message =
                RowDataToProtobufConverter.convertRowData(
                        original, rowType, buildDescriptor(rowType));
        byte[] payload = message.toByteArray();
        String writerSchema = ProtobufSchemaConverter.convertToProtobufSchema(rowType, SCHEMA_NAME);

        GsrProtobufRowDataDeserializationSchema deser =
                new GsrProtobufRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), SCHEMA_NAME, new HashMap<>());
        deser.setReader(new StaticGsrReader(writerSchema, payload));
        deser.open(null);
        return deser.deserialize(payload);
    }

    private static Descriptors.Descriptor buildDescriptor(RowType rowType) {
        try {
            DescriptorProtos.FileDescriptorProto fileProto =
                    ProtobufSchemaConverter.buildFileDescriptorProto(rowType, SCHEMA_NAME);
            Descriptors.FileDescriptor fileDescriptor =
                    Descriptors.FileDescriptor.buildFrom(
                            fileProto, new Descriptors.FileDescriptor[] {});
            return fileDescriptor.findMessageTypeByName(SCHEMA_NAME);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fake {@link GsrProtobufReader} standing in for the GSR facade: returns a fixed writer schema
     * definition and the raw Protobuf payload (no header/compression), so decode is driven by the
     * writer schema exactly as in production.
     */
    private static final class StaticGsrReader implements GsrProtobufReader {
        private static final long serialVersionUID = 1L;
        private final String writerSchemaDefinition;
        private final byte[] payload;

        StaticGsrReader(String writerSchemaDefinition, byte[] payload) {
            this.writerSchemaDefinition = writerSchemaDefinition;
            this.payload = payload;
        }

        @Override
        public String writerSchemaDefinition(byte[] gsrEncoded) {
            return writerSchemaDefinition;
        }

        @Override
        public byte[] actualData(byte[] gsrEncoded) {
            return payload;
        }
    }
}
