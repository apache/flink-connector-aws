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

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JSON round-trip serialization/deserialization with mock GSR header
 * handling.
 *
 * <p>Validates Requirement 8.1.
 *
 * <p>Decoding is routed through the {@link GsrJsonReader} seam; these tests stand in for the GSR
 * deserialization facade with a header-stripping fake, mirroring the production {@link
 * FacadeGsrJsonReader} which delegates to {@code
 * GlueSchemaRegistryDeserializationFacade.getActualData}.
 */
class JsonRoundTripIntegrationTest {

    private static final int GSR_HEADER_SIZE = 18;

    /**
     * Tests full JSON round-trip: RowData → JSON serialize → prepend GSR header → strip GSR header
     * via facade → JSON deserialize → RowData.
     */
    @Test
    void testJsonRoundTrip() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType())));

        // Create Flink JSON serializer
        JsonRowDataSerializationSchema jsonSer =
                new JsonRowDataSerializationSchema(
                        rowType,
                        TimestampFormat.SQL,
                        JsonFormatOptions.MapNullKeyMode.LITERAL,
                        "null",
                        false,
                        false);
        jsonSer.open(null);

        // Create GSR JSON deserializer (routes payload extraction through the facade seam)
        JsonRowDataDeserializationSchema jsonDeser =
                new JsonRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), false, false, TimestampFormat.SQL);
        GsrJsonRowDataDeserializationSchema gsrDeser =
                new GsrJsonRowDataDeserializationSchema(
                        rowType, jsonDeser, InternalTypeInfo.of(rowType), new HashMap<>());
        gsrDeser.setReader(new HeaderStrippingGsrJsonReader());
        gsrDeser.open(null);

        // Create test RowData
        GenericRowData original = new GenericRowData(2);
        original.setField(0, StringData.fromString("Alice"));
        original.setField(1, 30);

        // Serialize to JSON bytes
        byte[] jsonBytes = jsonSer.serialize(original);
        assertThat(jsonBytes).isNotNull();

        // Simulate GSR encoding: prepend mock 18-byte header
        byte[] gsrEncoded = prependMockGsrHeader(jsonBytes);
        assertThat(gsrEncoded.length).isEqualTo(GSR_HEADER_SIZE + jsonBytes.length);

        // Deserialize using GSR JSON deser (facade strips header, then JSON deser)
        RowData deserialized = gsrDeser.deserialize(gsrEncoded);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized.getString(0).toString()).isEqualTo("Alice");
        assertThat(deserialized.getInt(1)).isEqualTo(30);
    }

    /** Tests round-trip with multiple records to ensure consistency. */
    @Test
    void testJsonRoundTripMultipleRecords() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("value", new IntType())));

        JsonRowDataSerializationSchema jsonSer =
                new JsonRowDataSerializationSchema(
                        rowType,
                        TimestampFormat.SQL,
                        JsonFormatOptions.MapNullKeyMode.LITERAL,
                        "null",
                        false,
                        false);
        jsonSer.open(null);

        JsonRowDataDeserializationSchema jsonDeser =
                new JsonRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), false, false, TimestampFormat.SQL);
        GsrJsonRowDataDeserializationSchema gsrDeser =
                new GsrJsonRowDataDeserializationSchema(
                        rowType, jsonDeser, InternalTypeInfo.of(rowType), new HashMap<>());
        gsrDeser.setReader(new HeaderStrippingGsrJsonReader());
        gsrDeser.open(null);

        String[] names = {"Alice", "Bob", "Charlie"};
        int[] values = {10, 20, 30};

        for (int i = 0; i < names.length; i++) {
            GenericRowData row = new GenericRowData(2);
            row.setField(0, StringData.fromString(names[i]));
            row.setField(1, values[i]);

            byte[] jsonBytes = jsonSer.serialize(row);
            byte[] gsrEncoded = prependMockGsrHeader(jsonBytes);
            RowData result = gsrDeser.deserialize(gsrEncoded);

            assertThat(result).isNotNull();
            assertThat(result.getString(0).toString()).isEqualTo(names[i]);
            assertThat(result.getInt(1)).isEqualTo(values[i]);
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
     * Fake {@link GsrJsonReader} standing in for the facade: strips the fixed 18-byte header (no
     * compression in this test) and returns the JSON payload.
     */
    private static final class HeaderStrippingGsrJsonReader implements GsrJsonReader {
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] actualData(byte[] gsrEncoded) {
            return Arrays.copyOfRange(gsrEncoded, GSR_HEADER_SIZE, gsrEncoded.length);
        }
    }
}
