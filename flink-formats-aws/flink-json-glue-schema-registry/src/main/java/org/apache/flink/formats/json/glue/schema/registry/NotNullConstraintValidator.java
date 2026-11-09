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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

/**
 * Rejects rows that violate a {@code NOT NULL} declaration before they are serialized.
 *
 * <p>{@link JsonSchemaConverter} registers every {@code NOT NULL} column as a {@code required}
 * property with a non-nullable type. Flink's {@code JsonRowDataSerializationSchema} on the other
 * hand wraps every converter in a null-safe one and writes {@code null} for a null field, and the
 * GSR encode path deliberately skips payload validation (see {@link
 * GsrJsonRowDataSerializationSchema#serialize}). Without this check a null in a {@code NOT NULL}
 * column would therefore be published as a record that contradicts the very schema registered for
 * it, and consumers validating against that schema would reject it long after the producer moved
 * on. SQL sinks enforce {@code NOT NULL} before the serializer; DataStream users may not.
 *
 * <p>The walk mirrors the schema converter: top-level columns, nested {@code ROW} fields, {@code
 * ARRAY} elements and {@code MAP} values, each only when its own type is declared non-nullable.
 */
@Internal
final class NotNullConstraintValidator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;

    NotNullConstraintValidator(RowType rowType) {
        this.rowType = rowType;
    }

    /**
     * Validates one row.
     *
     * @throws IllegalStateException naming the offending column path when a {@code NOT NULL} field
     *     carries null
     */
    void validate(RowData row) {
        validateRow(row, rowType, "");
    }

    private static void validateRow(RowData row, RowType type, String prefix) {
        for (int i = 0; i < type.getFieldCount(); i++) {
            LogicalType fieldType = type.getTypeAt(i);
            String path = prefix + type.getFieldNames().get(i);
            if (row.isNullAt(i)) {
                if (!fieldType.isNullable()) {
                    throw notNullViolation(path);
                }
                continue;
            }
            validateValue(
                    RowData.createFieldGetter(fieldType, i).getFieldOrNull(row), fieldType, path);
        }
    }

    private static void validateValue(Object value, LogicalType type, String path) {
        switch (type.getTypeRoot()) {
            case ROW:
                validateRow((RowData) value, (RowType) type, path + ".");
                return;
            case ARRAY:
                LogicalType elementType = ((ArrayType) type).getElementType();
                ArrayData array = (ArrayData) value;
                ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
                for (int i = 0; i < array.size(); i++) {
                    String elementPath = path + "[" + i + "]";
                    if (array.isNullAt(i)) {
                        if (!elementType.isNullable()) {
                            throw notNullViolation(elementPath);
                        }
                        continue;
                    }
                    validateValue(
                            elementGetter.getElementOrNull(array, i), elementType, elementPath);
                }
                return;
            case MAP:
                LogicalType valueType = ((MapType) type).getValueType();
                MapData map = (MapData) value;
                ArrayData values = map.valueArray();
                ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
                for (int i = 0; i < values.size(); i++) {
                    String valuePath = path + "[value " + i + "]";
                    if (values.isNullAt(i)) {
                        if (!valueType.isNullable()) {
                            throw notNullViolation(valuePath);
                        }
                        continue;
                    }
                    validateValue(valueGetter.getElementOrNull(values, i), valueType, valuePath);
                }
                return;
            default:
                // scalar: nullness already checked by the caller
        }
    }

    private static IllegalStateException notNullViolation(String path) {
        return new IllegalStateException(
                String.format(
                        "Column '%s' is declared NOT NULL but the row carries null. The JSON "
                                + "Schema registered for this table marks it required, so the "
                                + "record is rejected instead of being published in violation "
                                + "of its own schema.",
                        path));
    }
}
