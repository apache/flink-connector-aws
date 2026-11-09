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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.util.StringJoiner;

/**
 * Converts a Flink {@link RowType} to a JSON Schema (draft-07) definition string for registration
 * with AWS Glue Schema Registry.
 *
 * <p>Type mapping:
 *
 * <ul>
 *   <li>{@code BOOLEAN} → {@code "boolean"}
 *   <li>{@code TINYINT}/{@code SMALLINT}/{@code INTEGER}/{@code BIGINT} → {@code "integer"}
 *   <li>{@code FLOAT}/{@code DOUBLE}/{@code DECIMAL} → {@code "number"} (matches how Flink's JSON
 *       serializer encodes these on the wire)
 *   <li>{@code CHAR}/{@code VARCHAR} → {@code "string"}
 *   <li>{@code DATE} → {@code "string"} with {@code "format":"date"}; {@code TIME} → {@code
 *       "string"} with {@code "format":"time"}; {@code TIMESTAMP}/{@code TIMESTAMP_LTZ} → {@code
 *       "string"} with {@code "format":"date-time"}
 *   <li>{@code BINARY}/{@code VARBINARY} → {@code "string"} with {@code "contentEncoding":"base64"}
 *       (matches Flink's base64 wire encoding for binary)
 *   <li>{@code ARRAY} → {@code {"type":"array","items":...}}
 *   <li>{@code MAP} → {@code {"type":"object","additionalProperties":...}} (keys must be
 *       character-typed, since JSON object keys are strings)
 *   <li>{@code ROW} → nested {@code {"type":"object","properties":{...}}}
 * </ul>
 *
 * <p>Review fixes applied over the initial candidate implementation:
 *
 * <ul>
 *   <li>Field names are JSON-escaped (top-level and nested {@code ROW}s) so a column containing a
 *       {@code "} or {@code \} no longer produces malformed schema JSON.
 *   <li>Genuinely unsupported types (MULTISET, RAW, INTERVAL, STRUCTURED, DISTINCT,
 *       TIMESTAMP_WITH_TIME_ZONE, …) now <b>fail fast</b> with an {@link
 *       UnsupportedOperationException} instead of being silently coerced to {@code "string"}.
 *   <li>Temporal types carry the JSON Schema {@code "format"} keyword and binary types carry {@code
 *       "contentEncoding":"base64"} rather than an untyped {@code "string"}.
 *   <li>A {@code "required"} array is emitted for {@code NOT NULL} fields so nullability is
 *       expressed rather than universally widened.
 *   <li>Object nodes (top-level and nested ROWs) emit {@code "additionalProperties":false}. With an
 *       open content model, GSR's server-side BACKWARD compatibility check rejects <em>any</em>
 *       added property (old data could already carry that property name with an arbitrary type),
 *       making schema evolution impossible. The closed model makes add-nullable-field evolution
 *       pass the check, verified against real GSR.
 *   <li>The brittle {@code indexOf}/{@code substring} re-parse of just-built type JSON is gone;
 *       nullability is composed directly.
 * </ul>
 *
 * <p><b>Deferred</b> (documented, consistent with the Protobuf PR this stacks on): the SQL read
 * path still does not resolve the writer schema from GSR (no registry query / version-UUID
 * resolution) and compression is not symmetric on read. Those require the larger GSR-facade
 * integration rework flagged in {@code reviews/glue-schema-registry-review.md} and are left as
 * follow-ups.
 */
@Internal
public class JsonSchemaConverter {

    /**
     * Converts a Flink RowType to a draft-07 JSON Schema string.
     *
     * @param rowType the Flink RowType
     * @return a JSON Schema string
     */
    public static String convertToJsonSchema(RowType rowType) {
        return objectSchema(rowType, true);
    }

    /** Builds a JSON-Schema {@code object} node for a {@link RowType}. */
    private static String objectSchema(RowType rowType, boolean topLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (topLevel) {
            sb.append("\"$schema\":\"http://json-schema.org/draft-07/schema#\",");
        }
        sb.append("\"type\":\"object\",\"additionalProperties\":false,\"properties\":{");
        StringJoiner props = new StringJoiner(",");
        StringJoiner required = new StringJoiner(",");
        for (RowType.RowField field : rowType.getFields()) {
            String escaped = escapeJson(field.getName());
            props.add("\"" + escaped + "\":" + convertType(field.getType()));
            if (!field.getType().isNullable()) {
                required.add("\"" + escaped + "\"");
            }
        }
        sb.append(props).append("}");
        if (required.length() > 0) {
            sb.append(",\"required\":[").append(required).append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    private static String convertType(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        boolean nullable = type.isNullable();
        switch (root) {
            case BOOLEAN:
                return scalar("boolean", "", nullable);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return scalar("integer", "", nullable);
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return scalar("number", "", nullable);
            case CHAR:
            case VARCHAR:
                return scalar("string", "", nullable);
            case DATE:
                return scalar("string", ",\"format\":\"date\"", nullable);
            case TIME_WITHOUT_TIME_ZONE:
                return scalar("string", ",\"format\":\"time\"", nullable);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return scalar("string", ",\"format\":\"date-time\"", nullable);
            case BINARY:
            case VARBINARY:
                return scalar("string", ",\"contentEncoding\":\"base64\"", nullable);
            case ARRAY:
                LogicalType elementType = ((ArrayType) type).getElementType();
                String array = "{\"type\":\"array\",\"items\":" + convertType(elementType) + "}";
                return nullable ? anyOfNull(array) : array;
            case MAP:
                MapType mapType = (MapType) type;
                LogicalTypeRoot keyRoot = mapType.getKeyType().getTypeRoot();
                if (keyRoot != LogicalTypeRoot.CHAR && keyRoot != LogicalTypeRoot.VARCHAR) {
                    throw unsupported(
                            type,
                            "MAP keys must be CHAR/VARCHAR because JSON object keys are strings; "
                                    + "found key type '"
                                    + mapType.getKeyType().asSummaryString()
                                    + "'");
                }
                String map =
                        "{\"type\":\"object\",\"additionalProperties\":"
                                + convertType(mapType.getValueType())
                                + "}";
                return nullable ? anyOfNull(map) : map;
            case ROW:
                String object = objectSchema((RowType) type, false);
                return nullable ? anyOfNull(object) : object;
            default:
                throw unsupported(type, null);
        }
    }

    /**
     * Builds a scalar node. For a nullable scalar the JSON type becomes a {@code ["type","null"]}
     * array while any extra keywords (e.g. {@code "format"}) are preserved.
     */
    private static String scalar(String jsonType, String extraKeywords, boolean nullable) {
        if (nullable) {
            return "{\"type\":[\"" + jsonType + "\",\"null\"]" + extraKeywords + "}";
        }
        return "{\"type\":\"" + jsonType + "\"" + extraKeywords + "}";
    }

    /** Wraps a complex (object/array) node so it also accepts {@code null}. */
    private static String anyOfNull(String node) {
        return "{\"anyOf\":[" + node + ",{\"type\":\"null\"}]}";
    }

    /** Escapes a string for safe inclusion inside a JSON string literal. */
    static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    private static UnsupportedOperationException unsupported(LogicalType type, String detail) {
        String base =
                "The 'json-glue' format does not support the Flink type '"
                        + type.asSummaryString()
                        + "'. Supported types are the scalar types (BOOLEAN, INT family, "
                        + "FLOAT/DOUBLE, DECIMAL, CHAR/VARCHAR, BINARY/VARBINARY, DATE, TIME, "
                        + "TIMESTAMP, TIMESTAMP_LTZ) and the nested container types "
                        + "(ARRAY, MAP with string keys, ROW).";
        return new UnsupportedOperationException(detail == null ? base : base + " " + detail);
    }

    private JsonSchemaConverter() {}
}
