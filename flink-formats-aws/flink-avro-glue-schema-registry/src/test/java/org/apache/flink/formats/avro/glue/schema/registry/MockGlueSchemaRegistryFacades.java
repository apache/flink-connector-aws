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

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * In-memory mock implementations of GSR facade classes for integration testing without AWS
 * credentials.
 *
 * <p>The mock serialization facade stores schemas in-memory and prepends a mock 18-byte GSR header.
 * The mock deserialization facade strips the header and returns the schema definition from the
 * in-memory store.
 */
public class MockGlueSchemaRegistryFacades {

    /** GSR header size: 1 (version) + 1 (compression) + 16 (UUID) = 18 bytes. */
    public static final int GSR_HEADER_SIZE = 18;

    /** In-memory schema store shared between serialization and deserialization mocks. */
    private final Map<UUID, String> schemaStore = new HashMap<>();

    /**
     * Encodes payload bytes by prepending a mock GSR header and registering the schema.
     *
     * @param schemaDefinition the schema definition string
     * @param payload the raw serialized bytes
     * @return bytes with mock GSR header prepended
     */
    public byte[] encode(String schemaDefinition, byte[] payload) {
        UUID schemaVersionId = getOrRegister(schemaDefinition);
        ByteBuffer buffer = ByteBuffer.allocate(GSR_HEADER_SIZE + payload.length);
        buffer.put(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE);
        buffer.put((byte) 0x00); // no compression
        buffer.putLong(schemaVersionId.getMostSignificantBits());
        buffer.putLong(schemaVersionId.getLeastSignificantBits());
        buffer.put(payload);
        return buffer.array();
    }

    /**
     * Strips the mock GSR header and returns the raw payload bytes.
     *
     * @param gsrEncodedBytes the GSR-encoded bytes (header + payload)
     * @return the raw payload bytes
     * @throws IOException if the input is too short
     */
    public byte[] getActualData(byte[] gsrEncodedBytes) throws IOException {
        if (gsrEncodedBytes.length < GSR_HEADER_SIZE) {
            throw new IOException(
                    "Invalid GSR-encoded data: expected at least "
                            + GSR_HEADER_SIZE
                            + " header bytes, got "
                            + gsrEncodedBytes.length);
        }
        byte[] payload = new byte[gsrEncodedBytes.length - GSR_HEADER_SIZE];
        System.arraycopy(gsrEncodedBytes, GSR_HEADER_SIZE, payload, 0, payload.length);
        return payload;
    }

    /**
     * Extracts the schema definition from the GSR header UUID.
     *
     * @param gsrEncodedBytes the GSR-encoded bytes
     * @return the schema definition string
     * @throws IOException if the schema is not found
     */
    public String getSchemaDefinition(byte[] gsrEncodedBytes) throws IOException {
        if (gsrEncodedBytes.length < GSR_HEADER_SIZE) {
            throw new IOException("Invalid GSR-encoded data: too short");
        }
        ByteBuffer buffer = ByteBuffer.wrap(gsrEncodedBytes, 2, 16);
        UUID schemaVersionId = new UUID(buffer.getLong(), buffer.getLong());
        String definition = schemaStore.get(schemaVersionId);
        if (definition == null) {
            throw new IOException("Schema not found for UUID: " + schemaVersionId);
        }
        return definition;
    }

    /** Registers a schema definition and returns its UUID. */
    private UUID getOrRegister(String schemaDefinition) {
        // Check if already registered
        for (Map.Entry<UUID, String> entry : schemaStore.entrySet()) {
            if (entry.getValue().equals(schemaDefinition)) {
                return entry.getKey();
            }
        }
        UUID id = UUID.randomUUID();
        schemaStore.put(id, schemaDefinition);
        return id;
    }

    /**
     * Creates a mock {@link GlueSchemaRegistryOutputStreamSerializer} that uses this facade's
     * in-memory store.
     */
    public GlueSchemaRegistryOutputStreamSerializer createMockOutputStreamSerializer(
            String transportName, Map<String, Object> configs) {
        return new MockOutputStreamSerializer(transportName, configs);
    }

    /**
     * Creates a mock {@link GlueSchemaRegistryInputStreamDeserializer} that uses this facade's
     * in-memory store.
     */
    public GlueSchemaRegistryInputStreamDeserializer createMockInputStreamDeserializer() {
        return new MockInputStreamDeserializer();
    }

    // ---- Inner mock classes ----

    /** Mock output stream serializer that stores schemas in-memory. */
    private class MockOutputStreamSerializer extends GlueSchemaRegistryOutputStreamSerializer {

        public MockOutputStreamSerializer(String transportName, Map<String, Object> configs) {
            super(transportName, configs, null);
        }

        @Override
        public void registerSchemaAndSerializeStream(Schema schema, OutputStream out, byte[] data)
                throws IOException {
            byte[] encoded = encode(schema.toString(), data);
            out.write(encoded);
        }
    }

    /** Mock input stream deserializer that reads from the in-memory store. */
    private class MockInputStreamDeserializer extends GlueSchemaRegistryInputStreamDeserializer {

        public MockInputStreamDeserializer() {
            super(
                    (com.amazonaws.services.schemaregistry.deserializers
                                    .GlueSchemaRegistryDeserializationFacade)
                            null);
        }

        @Override
        public Schema getSchemaAndDeserializedStream(InputStream in) throws IOException {
            byte[] inputBytes = new byte[in.available()];
            in.read(inputBytes);
            in.reset();

            // Extract schema definition from the header
            String schemaDefinition =
                    MockGlueSchemaRegistryFacades.this.getSchemaDefinition(inputBytes);

            // Strip header and update the stream
            byte[] actualData = MockGlueSchemaRegistryFacades.this.getActualData(inputBytes);
            org.apache.flink.formats.avro.utils.MutableByteArrayInputStream mutableStream =
                    (org.apache.flink.formats.avro.utils.MutableByteArrayInputStream) in;
            mutableStream.setBuffer(actualData);

            return new Schema.Parser().parse(schemaDefinition);
        }
    }
}
