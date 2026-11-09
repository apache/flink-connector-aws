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
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Protobuf round-trip serialization/deserialization with mock GSR header
 * handling.
 *
 * <p>Validates Requirement 8.1.
 */
class ProtobufRoundTripIntegrationTest {

    private static final int GSR_HEADER_SIZE = 18;
    private static final String SCHEMA_NAME = "TestMessage";

    /**
     * Tests full Protobuf round-trip: RowData → Protobuf serialize → prepend GSR header →
     * GsrProtobufRowDataDeserializationSchema (strips header + deser) → RowData.
     */
    @Test
    void testProtobufRoundTrip() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType())));

        Descriptors.Descriptor descriptor = buildDescriptor(rowType);

        // Serialize: RowData → DynamicMessage → Protobuf bytes → prepend GSR header
        GenericRowData original = new GenericRowData(2);
        original.setField(0, StringData.fromString("Alice"));
        original.setField(1, 30);

        DynamicMessage message =
                RowDataToProtobufConverter.convertRowData(original, rowType, descriptor);
        byte[] protobufBytes = message.toByteArray();
        byte[] gsrEncoded = prependMockGsrHeader(protobufBytes);

        // Deserialize using GsrProtobufRowDataDeserializationSchema, routed through a fake GSR
        // reader that resolves the writer schema and strips the header (mirrors the facade).
        GsrProtobufRowDataDeserializationSchema deser =
                new GsrProtobufRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), SCHEMA_NAME, new HashMap<>());
        deser.setReader(new FakeGsrReader(rowType, SCHEMA_NAME));
        deser.open(null);

        RowData deserialized = deser.deserialize(gsrEncoded);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized.getString(0).toString()).isEqualTo("Alice");
        assertThat(deserialized.getInt(1)).isEqualTo(30);
    }

    /** Tests round-trip with multiple records. */
    @Test
    void testProtobufRoundTripMultipleRecords() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("value", new IntType())));

        Descriptors.Descriptor descriptor = buildDescriptor(rowType);

        GsrProtobufRowDataDeserializationSchema deser =
                new GsrProtobufRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), SCHEMA_NAME, new HashMap<>());
        deser.setReader(new FakeGsrReader(rowType, SCHEMA_NAME));
        deser.open(null);

        String[] names = {"Alice", "Bob", "Charlie"};
        int[] values = {10, 20, 30};

        for (int i = 0; i < names.length; i++) {
            GenericRowData row = new GenericRowData(2);
            row.setField(0, StringData.fromString(names[i]));
            row.setField(1, values[i]);

            DynamicMessage message =
                    RowDataToProtobufConverter.convertRowData(row, rowType, descriptor);
            byte[] protobufBytes = message.toByteArray();
            byte[] gsrEncoded = prependMockGsrHeader(protobufBytes);

            RowData result = deser.deserialize(gsrEncoded);
            assertThat(result).isNotNull();
            assertThat(result.getString(0).toString()).isEqualTo(names[i]);
            assertThat(result.getInt(1)).isEqualTo(values[i]);
        }
    }

    private Descriptors.Descriptor buildDescriptor(RowType rowType) {
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

    /**
     * Fake {@link GsrProtobufReader} that stands in for the GSR facade in tests: it returns the
     * writer schema definition derived from the given RowType and strips the mock 18-byte header
     * (no compression) to yield the raw Protobuf payload.
     */
    private static final class FakeGsrReader implements GsrProtobufReader {
        private static final long serialVersionUID = 1L;
        private final String writerSchemaDefinition;

        FakeGsrReader(RowType rowType, String schemaName) {
            this.writerSchemaDefinition =
                    ProtobufSchemaConverter.convertToProtobufSchema(rowType, schemaName);
        }

        @Override
        public String writerSchemaDefinition(byte[] gsrEncoded) {
            return writerSchemaDefinition;
        }

        @Override
        public byte[] actualData(byte[] gsrEncoded) {
            return Arrays.copyOfRange(gsrEncoded, GSR_HEADER_SIZE, gsrEncoded.length);
        }
    }
}
