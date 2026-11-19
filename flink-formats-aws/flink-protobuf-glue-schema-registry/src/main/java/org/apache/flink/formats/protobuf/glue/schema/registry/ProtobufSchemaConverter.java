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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts a Flink {@link RowType} to a Protobuf schema definition string and to a {@link
 * DescriptorProtos.FileDescriptorProto} for runtime use.
 *
 * <p>Type mapping (proto3, scalar fields only):
 *
 * <ul>
 *   <li>{@code BOOLEAN} → {@code bool}
 *   <li>{@code TINYINT}/{@code SMALLINT}/{@code INTEGER} → {@code int32}
 *   <li>{@code DATE} → {@code int32} (epoch day); {@code TIME} → {@code int32} (millis of day)
 *   <li>{@code BIGINT} → {@code int64}
 *   <li>{@code TIMESTAMP}/{@code TIMESTAMP_LTZ} → {@code int64} (epoch millis)
 *   <li>{@code FLOAT} → {@code float}; {@code DOUBLE} → {@code double}
 *   <li>{@code CHAR}/{@code VARCHAR} → {@code string}; {@code DECIMAL} → {@code string} (lossless
 *       {@code BigDecimal} text form)
 *   <li>{@code BINARY}/{@code VARBINARY} → {@code bytes}
 * </ul>
 *
 * <p>Complex/unsupported types (ARRAY, MAP, MULTISET, ROW, RAW, STRUCTURED, ...) are rejected
 * fail-fast rather than silently coerced to {@code string} — see {@code unsupported()}.
 *
 * <p>Field identifiers are sanitized to valid proto names (see {@link #sanitizeFieldName}); the
 * original SQL column name is preserved as the field's {@code json_name} so it survives a
 * round-trip regardless of sanitization.
 */
@Internal
public class ProtobufSchemaConverter {

    private static final String PROTO_SYNTAX = "proto3";

    /** Matches the {@code message <Name> {}} declaration in a proto3 writer schema. */
    private static final Pattern MESSAGE_PATTERN =
            Pattern.compile("message\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*\\{");

    /**
     * Matches a single proto3 scalar field line, e.g. {@code string name = 1;} or {@code int64 ts =
     * 3 [json_name = "orig name"];}. Group 1 = optional label ({@code optional}/{@code repeated}),
     * group 2 = proto type keyword, group 3 = field name, group 4 = field number, group 5 = {@code
     * json_name} (the original SQL column name, when the identifier was sanitized).
     */
    private static final Pattern FIELD_PATTERN =
            Pattern.compile(
                    "(?:(optional|repeated)\\s+)?"
                            + "([A-Za-z_][A-Za-z0-9_.]*)\\s+"
                            + "([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*(\\d+)\\s*"
                            + "(?:\\[\\s*json_name\\s*=\\s*\"([^\"]*)\"\\s*\\])?\\s*;");

    /**
     * Converts a Flink RowType to a Protobuf schema definition string (proto3 syntax).
     *
     * @param rowType the Flink RowType
     * @param messageName the name for the Protobuf message
     * @return a Protobuf schema definition string
     */
    public static String convertToProtobufSchema(RowType rowType, String messageName) {
        requireUniqueSanitizedFieldNames(rowType);
        String sanitized = sanitizeMessageName(messageName);
        StringBuilder sb = new StringBuilder();
        sb.append("syntax = \"proto3\";\n\n");
        sb.append("message ").append(sanitized).append(" {\n");
        int fieldNumber = 1;
        for (RowType.RowField field : rowType.getFields()) {
            String protoType = toProtoType(field.getType());
            String fieldName = sanitizeFieldName(field.getName());
            sb.append("  ");
            // proto3 explicit presence: a NULLABLE column is emitted with the `optional`
            // keyword so the reader can distinguish an unset field (null) from a set type
            // default (0/""/false); a NOT NULL column stays a plain implicit-presence field
            // (review finding C2).
            if (field.getType().isNullable()) {
                sb.append("optional ");
            }
            sb.append(protoType).append(" ").append(fieldName);
            sb.append(" = ").append(fieldNumber++);
            if (!fieldName.equals(field.getName())) {
                sb.append(" [json_name = \"").append(field.getName()).append("\"]");
            }
            sb.append(";\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    /**
     * Builds a {@link DescriptorProtos.FileDescriptorProto} from a Flink RowType for runtime
     * Protobuf serialization/deserialization.
     *
     * @param rowType the Flink RowType
     * @param messageName the name for the Protobuf message
     * @return a FileDescriptorProto
     */
    public static DescriptorProtos.FileDescriptorProto buildFileDescriptorProto(
            RowType rowType, String messageName) {
        requireUniqueSanitizedFieldNames(rowType);
        String sanitized = sanitizeMessageName(messageName);
        DescriptorProtos.DescriptorProto.Builder messageBuilder =
                DescriptorProtos.DescriptorProto.newBuilder().setName(sanitized);

        int fieldNumber = 1;
        int syntheticOneofIndex = 0;
        for (RowType.RowField field : rowType.getFields()) {
            String fieldName = sanitizeFieldName(field.getName());
            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder =
                    DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName(fieldName)
                            .setNumber(fieldNumber++)
                            .setType(toProtoFieldType(field.getType()))
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            if (!fieldName.equals(field.getName())) {
                fieldBuilder.setJsonName(field.getName());
            }
            if (field.getType().isNullable()) {
                // proto3 explicit presence: wrap the nullable scalar in a synthetic oneof
                // (proto3 `optional`) so hasField() distinguishes an unset column (null) from
                // a set type default at decode time (review finding C2). NOT NULL columns keep
                // implicit presence. Synthetic oneofs are declared in field order, so each
                // oneof_index matches the position at which the oneof decl is appended.
                fieldBuilder.setProto3Optional(true);
                fieldBuilder.setOneofIndex(syntheticOneofIndex++);
                messageBuilder.addOneofDecl(
                        DescriptorProtos.OneofDescriptorProto.newBuilder()
                                .setName("_" + fieldName));
            }
            messageBuilder.addField(fieldBuilder);
        }

        return DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax(PROTO_SYNTAX)
                .addMessageType(messageBuilder)
                .build();
    }

    /**
     * Reconstructs a runtime {@link Descriptors.Descriptor} from a proto3 writer schema definition
     * resolved from AWS Glue Schema Registry (review finding B1). The on-wire field numbers, names
     * and {@code json_name}s therefore come from the <b>writer</b> schema registered in Glue,
     * rather than being synthesized from the local {@code RowType}.
     *
     * <p>The grammar handled is exactly the one emitted by {@link #convertToProtobufSchema}: a
     * single proto3 message of scalar fields, each optionally carrying a {@code json_name} option.
     *
     * @param protoSchemaDefinition the proto3 writer schema text
     * @return the message descriptor described by that schema
     */
    public static Descriptors.Descriptor buildDescriptorFromProtoSchema(
            String protoSchemaDefinition) {
        if (protoSchemaDefinition == null) {
            throw new IllegalArgumentException(
                    "GSR returned a null Protobuf writer schema definition on the decode path.");
        }
        Matcher messageMatcher = MESSAGE_PATTERN.matcher(protoSchemaDefinition);
        if (!messageMatcher.find()) {
            throw new IllegalArgumentException(
                    "Could not locate a proto3 'message' declaration in the GSR writer schema:\n"
                            + protoSchemaDefinition);
        }
        String messageName = messageMatcher.group(1);
        String body = protoSchemaDefinition.substring(messageMatcher.end());

        DescriptorProtos.DescriptorProto.Builder messageBuilder =
                DescriptorProtos.DescriptorProto.newBuilder().setName(messageName);

        Matcher fieldMatcher = FIELD_PATTERN.matcher(body);
        int syntheticOneofIndex = 0;
        while (fieldMatcher.find()) {
            String label = fieldMatcher.group(1);
            String protoTypeKeyword = fieldMatcher.group(2);
            String fieldName = fieldMatcher.group(3);
            int fieldNumber = Integer.parseInt(fieldMatcher.group(4));
            String jsonName = fieldMatcher.group(5);

            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder =
                    DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName(fieldName)
                            .setNumber(fieldNumber)
                            .setType(protoKeywordToFieldType(protoTypeKeyword))
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            if (jsonName != null) {
                fieldBuilder.setJsonName(jsonName);
            }
            if ("optional".equals(label)) {
                // Rebuild proto3 explicit presence for the field the writer marked `optional`
                // so hasField() works on the decode path (review finding C2). Synthetic oneofs
                // are declared in field order to keep each oneof_index aligned.
                fieldBuilder.setProto3Optional(true);
                fieldBuilder.setOneofIndex(syntheticOneofIndex++);
                messageBuilder.addOneofDecl(
                        DescriptorProtos.OneofDescriptorProto.newBuilder()
                                .setName("_" + fieldName));
            }
            messageBuilder.addField(fieldBuilder);
        }

        DescriptorProtos.FileDescriptorProto fileProto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setSyntax(PROTO_SYNTAX)
                        .addMessageType(messageBuilder)
                        .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(
                            fileProto, new Descriptors.FileDescriptor[] {})
                    .findMessageTypeByName(messageName);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(
                    "Failed to build a Protobuf descriptor from the GSR writer schema", e);
        }
    }

    private static DescriptorProtos.FieldDescriptorProto.Type protoKeywordToFieldType(
            String keyword) {
        switch (keyword) {
            case "bool":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
            case "int32":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
            case "sint32":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32;
            case "uint32":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32;
            case "fixed32":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32;
            case "sfixed32":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32;
            case "int64":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            case "sint64":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64;
            case "uint64":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64;
            case "fixed64":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64;
            case "sfixed64":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64;
            case "float":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
            case "double":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
            case "string":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
            case "bytes":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Protobuf field type '"
                                + keyword
                                + "' in the GSR writer schema.");
        }
    }

    /**
     * Sanitizes a schema name to be a valid Protobuf message name. Replaces non-alphanumeric
     * characters with underscores and ensures it starts with a letter.
     */
    static String sanitizeMessageName(String name) {
        String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
        if (!sanitized.isEmpty() && Character.isDigit(sanitized.charAt(0))) {
            sanitized = "M_" + sanitized;
        }
        if (sanitized.isEmpty()) {
            sanitized = "Message";
        }
        return sanitized;
    }

    /**
     * Sanitizes a Flink column name into a valid Protobuf field identifier. Protobuf field names
     * must match {@code [a-zA-Z_][a-zA-Z0-9_]*}; a column with a space, hyphen or leading digit
     * would otherwise trigger a {@code DescriptorValidationException} when the descriptor is built
     * at {@code open()} time.
     */
    static String sanitizeFieldName(String name) {
        String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
        if (sanitized.isEmpty()) {
            return "_field";
        }
        char first = sanitized.charAt(0);
        if (!Character.isLetter(first) && first != '_') {
            sanitized = "_" + sanitized;
        }
        return sanitized;
    }

    /**
     * Two distinct SQL columns can sanitize to the same Protobuf identifier ({@code `a b`} and
     * {@code `a-b`} both become {@code a_b}). The generated schema would then declare the field
     * twice, which fails descriptor validation at {@code open()} on the write side and resolves
     * both columns to the first field on the read side. Reject it up front with both column names.
     */
    static void requireUniqueSanitizedFieldNames(RowType rowType) {
        java.util.Map<String, String> seen = new java.util.HashMap<>();
        for (RowType.RowField field : rowType.getFields()) {
            String sanitized = sanitizeFieldName(field.getName());
            String previous = seen.putIfAbsent(sanitized, field.getName());
            if (previous != null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Columns '%s' and '%s' both map to the Protobuf field name '%s'. "
                                        + "Protobuf field names may only contain letters, digits "
                                        + "and underscores; rename one of the columns.",
                                previous, field.getName(), sanitized));
            }
        }
    }

    /**
     * Timestamps and times travel as {@code int64}/{@code int32} milliseconds, so a precision above
     * 3 would be silently truncated on write and could not be restored on read. Reject it at schema
     * time instead of losing digits at runtime.
     */
    private static void requireMillisecondPrecision(LogicalType type) {
        int precision;
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                precision =
                        ((org.apache.flink.table.types.logical.TimestampType) type).getPrecision();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                precision =
                        ((org.apache.flink.table.types.logical.LocalZonedTimestampType) type)
                                .getPrecision();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                precision = ((org.apache.flink.table.types.logical.TimeType) type).getPrecision();
                break;
            default:
                return;
        }
        if (precision > 3) {
            throw new IllegalArgumentException(
                    String.format(
                            "Type %s is not supported by the protobuf-glue format: timestamps and "
                                    + "times are encoded with millisecond precision, so a precision "
                                    + "above 3 would lose data. Declare the column with precision "
                                    + "3 or lower.",
                            type.asSummaryString()));
        }
    }

    private static String toProtoType(LogicalType type) {
        requireMillisecondPrecision(type);
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "bool";
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return "int32";
            case BIGINT:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "int64";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case CHAR:
            case VARCHAR:
            case DECIMAL:
                return "string";
            case BINARY:
            case VARBINARY:
                return "bytes";
            default:
                throw unsupported(type);
        }
    }

    private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(LogicalType type) {
        requireMillisecondPrecision(type);
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
            case BIGINT:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            case FLOAT:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
            case DOUBLE:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
            case CHAR:
            case VARCHAR:
            case DECIMAL:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
            case BINARY:
            case VARBINARY:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
            default:
                throw unsupported(type);
        }
    }

    private static UnsupportedOperationException unsupported(LogicalType type) {
        return new UnsupportedOperationException(
                "The 'protobuf-glue' format does not support the Flink type '"
                        + type.asSummaryString()
                        + "'. Supported types are the scalar types (BOOLEAN, INT family, "
                        + "FLOAT/DOUBLE, DECIMAL, CHAR/VARCHAR, BINARY/VARBINARY, DATE, TIME, "
                        + "TIMESTAMP, TIMESTAMP_LTZ). Complex types (ARRAY, MAP, MULTISET, ROW, "
                        + "RAW) are not yet supported.");
    }

    private ProtobufSchemaConverter() {}
}
