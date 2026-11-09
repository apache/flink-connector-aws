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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Deserialization schema that decodes AWS Glue Schema Registry encoded JSON records into Flink
 * {@link RowData}.
 *
 * <p>Decoding is routed through the GSR deserialization facade (see {@link GsrJsonReader}) so that
 * the GSR header and any compression are handled centrally in-library, rather than by a hand-rolled
 * 18-byte strip that never decompressed (review finding C1). Any non-{@code NONE} {@code
 * schema.compression} previously produced unparseable payloads on read because the encode path
 * compressed the body but the decode path only stripped a fixed-size header.
 *
 * <p>The GSR encoded record layout is:
 *
 * <pre>
 * [1 byte: header version (0x03)] [1 byte: compression] [16 bytes: schema version UUID] [N bytes: (possibly compressed) JSON payload]
 * </pre>
 *
 * <p>The facade returns the header-stripped, decompressed JSON payload, which is then parsed by
 * Flink's {@link JsonRowDataDeserializationSchema} against the local {@code RowType}.
 */
@Internal
public class GsrJsonRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final JsonRowDataDeserializationSchema jsonDeserializer;
    private final TypeInformation<RowData> producedType;
    private final DecimalRangeValidator decimalValidator;
    private final Map<String, Object> configs;

    private transient GsrJsonReader reader;

    /**
     * Creates a new GSR JSON deserialization schema.
     *
     * @param rowType the Flink RowType the payload is parsed against
     * @param jsonDeserializer the inner Flink JSON deserializer
     * @param producedType the type information for the produced RowData
     * @param configs the GSR SDK configuration map (region, registry, credentials, cache,
     *     compression, ...) used to build the deserialization facade
     */
    public GsrJsonRowDataDeserializationSchema(
            RowType rowType,
            JsonRowDataDeserializationSchema jsonDeserializer,
            TypeInformation<RowData> producedType,
            Map<String, Object> configs) {
        this.jsonDeserializer = jsonDeserializer;
        this.producedType = producedType;
        this.decimalValidator = new DecimalRangeValidator(rowType);
        this.configs = configs;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonDeserializer.open(context);
        if (reader == null) {
            reader = new FacadeGsrJsonReader(configs);
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        // Route through the facade so the GSR header is stripped and any compression is
        // decompressed in-library (review finding C1); then parse the JSON payload.
        byte[] jsonPayload = reader.actualData(message);
        final JsonNode root;
        try {
            root = jsonDeserializer.deserializeToJsonNode(jsonPayload);
        } catch (Throwable t) {
            throw deserializationFailure(jsonPayload, t);
        }
        if (root == null || root.isArray()) {
            // Multi-record payloads are flink-json's business (it rejects them on this entry
            // point); keep its behaviour.
            return jsonDeserializer.deserialize(jsonPayload);
        }
        try {
            // flink-json turns a DECIMAL that overflows the reader's precision into null without
            // an error; check the parsed values against the declared types first.
            decimalValidator.validate(root);
            return jsonDeserializer.convertToRowData(root);
        } catch (Throwable t) {
            // Same wrapping flink-json applies on its own deserialize(byte[]) path.
            throw deserializationFailure(jsonPayload, t);
        }
    }

    private static IOException deserializationFailure(byte[] payload, Throwable cause) {
        return new IOException(
                String.format(
                        "Failed to deserialize JSON '%s'.",
                        new String(payload, StandardCharsets.UTF_8)),
                cause);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    @VisibleForTesting
    void setReader(GsrJsonReader reader) {
        this.reader = reader;
    }
}
