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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.List;

/**
 * Converts Flink {@link RowData} to Protobuf {@link DynamicMessage} based on a Protobuf {@link
 * Descriptors.Descriptor}.
 *
 * <p>Temporal and decimal columns are encoded via their proper {@link RowData} accessors — never
 * via {@code getString} — to avoid the {@code ClassCastException} that a naive {@code default:}
 * branch produced (see review finding B4). TIMESTAMP/TIMESTAMP_LTZ → epoch-millis {@code int64},
 * DATE/TIME → {@code int32}, DECIMAL → its lossless {@code BigDecimal} text form.
 */
@Internal
public class RowDataToProtobufConverter {

    /**
     * Converts a Flink RowData to a Protobuf DynamicMessage.
     *
     * @param rowData the Flink RowData
     * @param rowType the Flink RowType describing the schema
     * @param descriptor the Protobuf message descriptor
     * @return a DynamicMessage
     */
    public static DynamicMessage convertRowData(
            RowData rowData, RowType rowType, Descriptors.Descriptor descriptor) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            LogicalType fieldType = rowType.getTypeAt(i);
            if (rowData.isNullAt(i)) {
                if (!fieldType.isNullable()) {
                    // A NOT NULL column is an implicit-presence proto3 field: leaving it unset
                    // would decode as the type default (0 / "" / false), turning a contract
                    // violation into silently wrong data. SQL sinks enforce NOT NULL before the
                    // serializer; DataStream users may not.
                    throw new IllegalStateException(
                            String.format(
                                    "Column '%s' is declared NOT NULL but the row carries null",
                                    rowType.getFieldNames().get(i)));
                }
                // Leave the field unset. For a NULLABLE column this is a proto3 explicit-presence
                // (`optional`) field, so an unset field is observably absent (hasField()==false)
                // and decodes back to null. Explicit presence is emitted by
                // ProtobufSchemaConverter (review finding C2).
                continue;
            }
            Descriptors.FieldDescriptor fd = fields.get(i);
            Object value = extractFieldValue(rowData, i, fieldType);
            if (value != null) {
                builder.setField(fd, value);
            }
        }
        return builder.build();
    }

    private static Object extractFieldValue(RowData rowData, int index, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return rowData.getBoolean(index);
            case TINYINT:
                return (int) rowData.getByte(index);
            case SMALLINT:
                return (int) rowData.getShort(index);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                // DATE = epoch day, TIME = millis-of-day, both stored as int in RowData.
                return rowData.getInt(index);
            case BIGINT:
                return rowData.getLong(index);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return rowData.getTimestamp(index, ((TimestampType) type).getPrecision())
                        .getMillisecond();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return rowData.getTimestamp(index, ((LocalZonedTimestampType) type).getPrecision())
                        .getMillisecond();
            case FLOAT:
                return rowData.getFloat(index);
            case DOUBLE:
                return rowData.getDouble(index);
            case DECIMAL:
                final DecimalType dt = (DecimalType) type;
                return rowData.getDecimal(index, dt.getPrecision(), dt.getScale())
                        .toBigDecimal()
                        .toString();
            case CHAR:
            case VARCHAR:
                return rowData.getString(index).toString();
            case BINARY:
            case VARBINARY:
                return ByteString.copyFrom(rowData.getBinary(index));
            default:
                throw new UnsupportedOperationException(
                        "The 'protobuf-glue' format cannot encode the Flink type '"
                                + type.asSummaryString()
                                + "'.");
        }
    }

    private RowDataToProtobufConverter() {}
}
