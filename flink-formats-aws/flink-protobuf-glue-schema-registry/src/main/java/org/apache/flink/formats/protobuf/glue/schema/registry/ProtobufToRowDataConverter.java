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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.math.BigDecimal;
import java.util.List;

/**
 * Converts Protobuf {@link DynamicMessage} to Flink {@link RowData} based on a Flink {@link
 * RowType}.
 *
 * <p>Temporal and decimal columns are decoded from their wire encoding (epoch-millis {@code int64}
 * for TIMESTAMP/TIMESTAMP_LTZ, {@code int32} for DATE/TIME, {@code string} for DECIMAL) — mirroring
 * {@link RowDataToProtobufConverter} so the round-trip is symmetric (review finding B4).
 */
@Internal
public class ProtobufToRowDataConverter {

    /**
     * Converts a Protobuf DynamicMessage to a Flink RowData.
     *
     * <p>Fields are mapped by <b>name</b> (not position): for each {@link RowType} column the
     * matching field is resolved in the writer descriptor by its sanitized Protobuf name, falling
     * back to the original SQL name preserved as {@code json_name}. This means a column reorder or
     * a writer schema that carries extra/fewer fields no longer silently reinterprets on-wire tags
     * (review finding B1). A column with no counterpart in the GSR-registered writer schema is
     * treated as missing: {@code null} for a nullable column, a hard error for a {@code NOT NULL}
     * column.
     *
     * @param message the Protobuf DynamicMessage decoded with the writer descriptor
     * @param rowType the Flink RowType describing the expected schema
     * @return a GenericRowData
     */
    public static RowData convertToRowData(DynamicMessage message, RowType rowType) {
        GenericRowData row = new GenericRowData(rowType.getFieldCount());
        Descriptors.Descriptor descriptor = message.getDescriptorForType();

        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField rowField = fields.get(i);
            LogicalType fieldType = rowField.getType();

            Descriptors.FieldDescriptor fd = findFieldByName(descriptor, rowField.getName());
            if (fd == null) {
                // The GSR-registered writer schema has no field for this column.
                if (!fieldType.isNullable()) {
                    throw new IllegalStateException(
                            "Column '"
                                    + rowField.getName()
                                    + "' is declared NOT NULL but is absent from the "
                                    + "GSR-registered writer schema '"
                                    + descriptor.getName()
                                    + "'. Cannot decode a required column that the writer never "
                                    + "produced.");
                }
                row.setField(i, null);
                continue;
            }

            Object protoValue = readFieldValue(message, fd);
            row.setField(i, convertProtoValue(protoValue, fieldType));
        }
        return row;
    }

    /**
     * Reads a field value honoring proto3 explicit presence (review finding C2). For a
     * presence-tracking field (a proto3 {@code optional} scalar, wrapped in a synthetic oneof by
     * {@link ProtobufSchemaConverter}) that the writer left unset, this returns {@code null} so a
     * nullable column round-trips as null rather than the type default (0/""/false). For an
     * implicit-presence field (a NOT NULL column) it returns the value, which is the type default
     * when unset.
     */
    private static Object readFieldValue(DynamicMessage message, Descriptors.FieldDescriptor fd) {
        if (fd.hasPresence() && !message.hasField(fd)) {
            return null;
        }
        return message.getField(fd);
    }

    /**
     * Resolves the writer-descriptor field for a Flink column name. Tries the sanitized Protobuf
     * field name first (the writer sanitizes identically), then the original SQL name preserved as
     * {@code json_name}, then a verbatim name match.
     */
    private static Descriptors.FieldDescriptor findFieldByName(
            Descriptors.Descriptor descriptor, String columnName) {
        Descriptors.FieldDescriptor fd =
                descriptor.findFieldByName(ProtobufSchemaConverter.sanitizeFieldName(columnName));
        if (fd != null) {
            return fd;
        }
        for (Descriptors.FieldDescriptor candidate : descriptor.getFields()) {
            if (columnName.equals(candidate.getJsonName())) {
                return candidate;
            }
        }
        return descriptor.findFieldByName(columnName);
    }

    private static Object convertProtoValue(Object protoValue, LogicalType type) {
        // A null here means either the writer's schema had no field for this column, or the field
        // is a proto3 explicit-presence (`optional`) field the writer left unset (finding C2);
        // either way the column decodes to null. For NOT NULL columns (implicit presence) an unset
        // field surfaces its type default here, which is the intended proto3 semantic.
        if (protoValue == null) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return protoValue;
            case TINYINT:
                return ((Integer) protoValue).byteValue();
            case SMALLINT:
                return ((Integer) protoValue).shortValue();
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                // int32 wire form; DATE = epoch day, TIME = millis-of-day (both int in RowData).
                return protoValue;
            case BIGINT:
                return protoValue;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromEpochMillis((Long) protoValue);
            case FLOAT:
                return protoValue;
            case DOUBLE:
                return protoValue;
            case DECIMAL:
                final DecimalType dt = (DecimalType) type;
                final BigDecimal decimal = new BigDecimal(protoValue.toString());
                // fromBigDecimal rescales to the reader's scale (rounding HALF_UP, as CAST does)
                // and returns null when the integer part does not fit the reader's precision.
                // A null here would surface as a wrong value on a nullable column and as a
                // NOT NULL violation on a non-nullable one, so fail with the actual numbers.
                DecimalData decimalData =
                        DecimalData.fromBigDecimal(decimal, dt.getPrecision(), dt.getScale());
                if (decimalData == null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Value %s written by the producer does not fit the declared "
                                            + "type %s (%d digit(s) before the decimal point, "
                                            + "%d allowed). The writer's schema is wider than the "
                                            + "reader's; widen the column or fix the producer.",
                                    decimal.toPlainString(),
                                    dt.asSummaryString(),
                                    decimal.precision() - decimal.scale(),
                                    dt.getPrecision() - dt.getScale()));
                }
                return decimalData;
            case CHAR:
            case VARCHAR:
                return StringData.fromString(protoValue.toString());
            case BINARY:
            case VARBINARY:
                if (protoValue instanceof ByteString) {
                    return ((ByteString) protoValue).toByteArray();
                }
                return protoValue;
            default:
                throw new UnsupportedOperationException(
                        "The 'protobuf-glue' format cannot decode the Flink type '"
                                + type.asSummaryString()
                                + "'.");
        }
    }

    private ProtobufToRowDataConverter() {}
}
