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

import java.io.Serializable;

/**
 * Read-side seam that resolves the writer schema and the actual Protobuf payload from a GSR-encoded
 * record.
 *
 * <p>This mirrors the Avro format's {@code SchemaCoder} abstraction (see {@link
 * org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroSchemaCoder}): decoding
 * is routed through the AWS Glue Schema Registry deserialization facade so that the <b>writer</b>
 * schema/version registered in Glue is resolved from the record, and the GSR header, schema-version
 * UUID and compression are handled centrally in-library — instead of the on-wire contract being
 * defined by the local table DDL (review finding B1) and instead of a hand-rolled 18-byte header
 * strip that never decompresses (review finding C1).
 *
 * <p>The seam is {@link Serializable} so it ships with the {@link
 * GsrProtobufRowDataDeserializationSchema}; the concrete facade is built lazily on the task
 * managers. A hand-written fake implementation is used in unit tests, avoiding any dependency on a
 * live registry.
 */
@Internal
interface GsrProtobufReader extends Serializable {

    /**
     * Returns the proto3 writer schema definition registered in GSR for this record. The reader
     * descriptor is built from this definition, so the on-wire field numbers/types come from the
     * registry — not from the local {@code RowType}.
     *
     * @param gsrEncoded the full GSR-encoded record (header + payload)
     * @return the writer schema definition (proto3 text)
     */
    String writerSchemaDefinition(byte[] gsrEncoded);

    /**
     * Returns the header-stripped and (if the writer compressed it) decompressed Protobuf payload.
     * Decompression is handled centrally by the GSR facade (review finding C1).
     *
     * @param gsrEncoded the full GSR-encoded record (header + payload)
     * @return the raw Protobuf message bytes
     */
    byte[] actualData(byte[] gsrEncoded);
}
