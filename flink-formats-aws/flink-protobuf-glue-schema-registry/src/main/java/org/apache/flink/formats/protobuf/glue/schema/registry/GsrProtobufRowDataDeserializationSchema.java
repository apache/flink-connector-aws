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

package org.apache.flink.formats.protobuf.glue.schema.registry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserialization schema that decodes AWS Glue Schema Registry encoded Protobuf records into Flink
 * {@link RowData}.
 *
 * <p>Decoding is routed through the GSR deserialization facade (see {@link GsrProtobufReader}) so
 * that:
 *
 * <ul>
 *   <li>the <b>writer</b> schema/version is resolved from Glue via the schema-version UUID in the
 *       record — the reader descriptor is built from that writer schema, not from the local {@code
 *       RowType} (review finding B1);
 *   <li>the GSR header and any compression are handled centrally in-library, rather than by a
 *       hand-rolled 18-byte strip that never decompresses (review finding C1).
 * </ul>
 *
 * <p>The decoded {@link DynamicMessage} is mapped to {@link RowData} by field <b>name</b>; see
 * {@link ProtobufToRowDataConverter}.
 */
@Internal
public class GsrProtobufRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final TypeInformation<RowData> producedType;
    private final String schemaName;
    private final Map<String, Object> configs;

    private transient GsrProtobufReader reader;

    /** Reader descriptors cached by writer schema definition to avoid re-parsing per record. */
    private transient Map<String, Descriptors.Descriptor> descriptorCache;

    /**
     * Creates a new GSR Protobuf deserialization schema.
     *
     * @param rowType the Flink RowType describing the expected schema
     * @param producedType the type information for the produced RowData
     * @param schemaName the schema name (retained for diagnostics)
     * @param configs the GSR SDK configuration map (region, registry, credentials, cache,
     *     compression, ...) used to build the deserialization facade
     */
    public GsrProtobufRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> producedType,
            String schemaName,
            Map<String, Object> configs) {
        this.rowType = rowType;
        this.producedType = producedType;
        this.schemaName = schemaName;
        this.configs = configs;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        if (reader == null) {
            reader = new FacadeGsrProtobufReader(configs);
        }
        if (descriptorCache == null) {
            descriptorCache = new HashMap<>();
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }

        // Resolve the writer schema from GSR and let the facade strip the header + decompress.
        final String writerSchemaDefinition = reader.writerSchemaDefinition(message);
        final byte[] protobufPayload = reader.actualData(message);

        final Descriptors.Descriptor writerDescriptor =
                descriptorCache.computeIfAbsent(
                        writerSchemaDefinition,
                        ProtobufSchemaConverter::buildDescriptorFromProtoSchema);

        final DynamicMessage dynamicMessage =
                DynamicMessage.parseFrom(writerDescriptor, protobufPayload);
        return ProtobufToRowDataConverter.convertToRowData(dynamicMessage, rowType);
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
    void setReader(GsrProtobufReader reader) {
        this.reader = reader;
    }

    @VisibleForTesting
    String getSchemaName() {
        return schemaName;
    }
}
