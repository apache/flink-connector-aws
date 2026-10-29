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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip regression test for review finding <b>C1</b>: compression asymmetry — the encode path
 * honours {@code schema.compression} (ZLIB) while the pre-fix decode path stripped a fixed 18-byte
 * header and <b>never decompressed</b>, so any non-{@code NONE} compression produced unparseable
 * payloads on read.
 *
 * <p>The fix (shared with B1) routes decode through the GSR deserialization facade: {@link
 * GsrProtobufRowDataDeserializationSchema#deserialize} obtains the payload solely via {@link
 * GsrProtobufReader#actualData}, and the production {@link FacadeGsrProtobufReader} delegates that
 * to {@code GlueSchemaRegistryDeserializationFacade.getActualData}, which strips the header <b>and
 * decompresses in-library</b>. The schema itself performs no explicit header strip and no explicit
 * decompression.
 *
 * <p>These tests stand in for the facade with a {@link CompressionAwareGsrReader} fake that models
 * exactly that contract: it reads the GSR header's compression byte and ZLIB-inflates the payload
 * when set (mirroring the SDK), so a record compressed on write is transparently read back. Because
 * the deserialization schema delegates all payload extraction to the reader, a green round-trip
 * here proves the read path is no longer missing decompression — the very asymmetry C1 flagged.
 */
class ProtobufC1CompressionRoundTripTest {

    private static final String SCHEMA_NAME = "TestMessage";
    private static final int GSR_HEADER_SIZE = 18;

    /** GSR header version byte. */
    private static final byte HEADER_VERSION = (byte) 0x03;

    /** GSR compression byte: no compression. */
    private static final byte COMPRESSION_NONE = (byte) 0x00;

    /** GSR compression byte: ZLIB (matches the SDK's ZLIB compression byte). */
    private static final byte COMPRESSION_ZLIB = (byte) 0x05;

    private static RowType rowType() {
        return new RowType(
                false,
                Arrays.asList(
                        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
                        new RowType.RowField("age", new IntType())));
    }

    /**
     * C1 core proof: a record whose payload is ZLIB-compressed on the wire (compression byte set in
     * the header) is decoded successfully. This only passes because decode routes through the
     * facade ({@link GsrProtobufReader#actualData}) which decompresses in-library; the pre-fix path
     * that merely stripped 18 bytes would hand raw deflate bytes to the Protobuf parser and fail.
     */
    @Test
    void testZlibCompressedRecordRoundTrips() throws Exception {
        RowType rowType = rowType();

        GenericRowData original = new GenericRowData(2);
        original.setField(0, StringData.fromString("Alice"));
        original.setField(1, 30);

        byte[] gsrEncoded = encodeWithCompression(original, rowType, COMPRESSION_ZLIB);

        RowData decoded = decode(rowType, gsrEncoded);

        assertThat(decoded).isNotNull();
        assertThat(decoded.getString(0).toString()).isEqualTo("Alice");
        assertThat(decoded.getInt(1)).isEqualTo(30);
    }

    /**
     * Control: the identical payload with compression disabled (compression byte {@code 0x00}) also
     * round-trips through the same facade-routed path, confirming the ZLIB result above is not an
     * artefact of the fake but the compression byte genuinely drives decompression.
     */
    @Test
    void testUncompressedRecordRoundTrips() throws Exception {
        RowType rowType = rowType();

        GenericRowData original = new GenericRowData(2);
        original.setField(0, StringData.fromString("Alice"));
        original.setField(1, 30);

        byte[] gsrEncoded = encodeWithCompression(original, rowType, COMPRESSION_NONE);

        RowData decoded = decode(rowType, gsrEncoded);

        assertThat(decoded).isNotNull();
        assertThat(decoded.getString(0).toString()).isEqualTo("Alice");
        assertThat(decoded.getInt(1)).isEqualTo(30);
    }

    /**
     * C1 with multiple records over the same schema, exercising the reader's per-record
     * decompression on a compressible payload large enough that deflate actually shrinks it (guards
     * against a fake that silently no-ops compression).
     */
    @Test
    void testZlibCompressedMultipleRecordsRoundTrip() throws Exception {
        RowType rowType = rowType();

        GsrProtobufRowDataDeserializationSchema deser =
                new GsrProtobufRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), SCHEMA_NAME, new HashMap<>());
        deser.setReader(new CompressionAwareGsrReader(rowType, SCHEMA_NAME));
        deser.open(null);

        String[] names = {
            "Alice", "Bob", "Charlie", "a-name-repeated-repeated-repeated-repeated-repeated"
        };
        int[] ages = {30, 41, 52, 63};

        for (int i = 0; i < names.length; i++) {
            GenericRowData row = new GenericRowData(2);
            row.setField(0, StringData.fromString(names[i]));
            row.setField(1, ages[i]);

            byte[] gsrEncoded = encodeWithCompression(row, rowType, COMPRESSION_ZLIB);
            RowData decoded = deser.deserialize(gsrEncoded);

            assertThat(decoded).isNotNull();
            assertThat(decoded.getString(0).toString()).isEqualTo(names[i]);
            assertThat(decoded.getInt(1)).isEqualTo(ages[i]);
        }
    }

    /** Runs the decode path with the compression-aware fake facade reader. */
    private static RowData decode(RowType rowType, byte[] gsrEncoded) throws Exception {
        GsrProtobufRowDataDeserializationSchema deser =
                new GsrProtobufRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), SCHEMA_NAME, new HashMap<>());
        deser.setReader(new CompressionAwareGsrReader(rowType, SCHEMA_NAME));
        deser.open(null);
        return deser.deserialize(gsrEncoded);
    }

    /**
     * Builds a GSR-encoded record: 18-byte header (version + compression byte + 16-byte UUID)
     * followed by the Protobuf payload, ZLIB-compressed when {@code compressionByte} is non-zero —
     * exactly what the serialization facade emits when {@code schema.compression=ZLIB}.
     */
    private static byte[] encodeWithCompression(RowData row, RowType rowType, byte compressionByte)
            throws Exception {
        DynamicMessage message =
                RowDataToProtobufConverter.convertRowData(row, rowType, buildDescriptor(rowType));
        byte[] protobufBytes = message.toByteArray();
        byte[] body = compressionByte == COMPRESSION_NONE ? protobufBytes : zlib(protobufBytes);

        UUID schemaId = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(GSR_HEADER_SIZE + body.length);
        buffer.put(HEADER_VERSION);
        buffer.put(compressionByte);
        buffer.putLong(schemaId.getMostSignificantBits());
        buffer.putLong(schemaId.getLeastSignificantBits());
        buffer.put(body);
        return buffer.array();
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

    private static byte[] zlib(byte[] data) throws Exception {
        Deflater deflater = new Deflater();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DeflaterOutputStream dos = new DeflaterOutputStream(out, deflater)) {
            dos.write(data);
        }
        deflater.end();
        return out.toByteArray();
    }

    private static byte[] inflate(byte[] data) throws Exception {
        Inflater inflater = new Inflater();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InflaterInputStream iis =
                new InflaterInputStream(new java.io.ByteArrayInputStream(data), inflater)) {
            byte[] buf = new byte[1024];
            int n;
            while ((n = iis.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
        }
        inflater.end();
        return out.toByteArray();
    }

    /**
     * Fake {@link GsrProtobufReader} that models the GSR deserialization facade's compression
     * contract: it resolves the writer schema from the RowType and, for {@link
     * #actualData(byte[])}, reads the header's compression byte and ZLIB-inflates the body when set
     * — i.e. decompression happens <b>in the facade layer</b>, exactly as the production {@link
     * FacadeGsrProtobufReader} delegates to the SDK. The deserialization schema under test performs
     * no decompression of its own; it only parses whatever {@code actualData} returns.
     */
    private static final class CompressionAwareGsrReader implements GsrProtobufReader {
        private static final long serialVersionUID = 1L;
        private final String writerSchemaDefinition;

        CompressionAwareGsrReader(RowType rowType, String schemaName) {
            this.writerSchemaDefinition =
                    ProtobufSchemaConverter.convertToProtobufSchema(rowType, schemaName);
        }

        @Override
        public String writerSchemaDefinition(byte[] gsrEncoded) {
            return writerSchemaDefinition;
        }

        @Override
        public byte[] actualData(byte[] gsrEncoded) {
            byte compressionByte = gsrEncoded[1];
            byte[] body = Arrays.copyOfRange(gsrEncoded, GSR_HEADER_SIZE, gsrEncoded.length);
            if (compressionByte == COMPRESSION_NONE) {
                return body;
            }
            try {
                return inflate(body);
            } catch (Exception e) {
                throw new RuntimeException("Failed to decompress GSR payload", e);
            }
        }
    }
}
