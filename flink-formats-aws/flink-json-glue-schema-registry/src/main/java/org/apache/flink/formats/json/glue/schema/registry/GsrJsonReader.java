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

import java.io.Serializable;

/**
 * Read-side seam that resolves the actual JSON payload from a GSR-encoded record.
 *
 * <p>This mirrors the Protobuf format's {@code GsrProtobufReader} abstraction: decoding is routed
 * through the AWS Glue Schema Registry deserialization facade so that the GSR header,
 * schema-version UUID and compression are handled centrally in-library — instead of a hand-rolled
 * 18-byte header strip that never decompresses (review finding C1). Any non-{@code NONE} {@code
 * schema.compression} produced unparseable payloads on the pre-fix read path because the encode
 * side compressed but the decode side never inflated.
 *
 * <p>The seam is {@link Serializable} so it ships with the {@link
 * GsrJsonRowDataDeserializationSchema}; the concrete facade is built lazily on the task managers. A
 * hand-written fake implementation is used in unit tests, avoiding any dependency on a live
 * registry.
 */
@Internal
interface GsrJsonReader extends Serializable {

    /**
     * Returns the header-stripped and (if the writer compressed it) decompressed JSON payload.
     * Decompression is handled centrally by the GSR facade (review finding C1).
     *
     * @param gsrEncoded the full GSR-encoded record (header + payload)
     * @return the raw JSON payload bytes
     */
    byte[] actualData(byte[] gsrEncoded);
}
