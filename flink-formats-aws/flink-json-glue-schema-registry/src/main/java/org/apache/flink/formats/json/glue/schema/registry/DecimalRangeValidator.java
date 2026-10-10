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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

/**
 * Rejects JSON payloads whose {@code DECIMAL} values do not fit the declared reader type.
 *
 * <p>Flink's {@code JsonToRowDataConverters} ends its decimal conversion in {@link
 * DecimalData#fromBigDecimal(BigDecimal, int, int)}, which rescales (rounding {@code HALF_UP}, as
 * {@code CAST} does) but returns {@code null} when the integer part does not fit the reader's
 * precision. That null is then collected as the field value: a wrong value on a nullable column, a
 * {@code NOT NULL} violation on a non-nullable one, silently either way. A producer whose
 * registered schema is wider than the reader's table (say {@code DECIMAL(12,2)} written, {@code
 * DECIMAL(5,2)} read) therefore loses data without an error. This validator walks the parsed tree
 * first and fails with the offending value, path and type instead.
 *
 * <p>Only {@code DECIMAL} is checked: every other JSON-to-Flink conversion in flink-json either
 * succeeds or throws.
 */
@Internal
final class DecimalRangeValidator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final boolean hasDecimal;

    DecimalRangeValidator(RowType rowType) {
        this.rowType = rowType;
        this.hasDecimal = containsDecimal(rowType);
    }

    /** Validates one parsed record (an object node) against the row type. */
    void validate(JsonNode root) {
        if (!hasDecimal || root == null || root.isNull()) {
            return;
        }
        validateRow(root, rowType, "");
    }

    private static boolean containsDecimal(LogicalType type) {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                return true;
            case ROW:
                return ((RowType) type)
                        .getFields().stream().anyMatch(f -> containsDecimal(f.getType()));
            case ARRAY:
                return containsDecimal(((ArrayType) type).getElementType());
            case MAP:
                return containsDecimal(((MapType) type).getValueType());
            default:
                return false;
        }
    }

    private static void validateRow(JsonNode node, RowType type, String prefix) {
        if (!node.isObject()) {
            return; // flink-json reports the shape mismatch itself
        }
        for (RowType.RowField field : type.getFields()) {
            JsonNode child = node.get(field.getName());
            if (child == null || child.isNull()) {
                continue;
            }
            validateValue(child, field.getType(), prefix + field.getName());
        }
    }

    private static void validateValue(JsonNode node, LogicalType type, String path) {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                validateDecimal(node, (DecimalType) type, path);
                return;
            case ROW:
                validateRow(node, (RowType) type, path + ".");
                return;
            case ARRAY:
                if (node.isArray()) {
                    LogicalType elementType = ((ArrayType) type).getElementType();
                    for (int i = 0; i < node.size(); i++) {
                        JsonNode element = node.get(i);
                        if (!element.isNull()) {
                            validateValue(element, elementType, path + "[" + i + "]");
                        }
                    }
                }
                return;
            case MAP:
                if (node.isObject()) {
                    LogicalType valueType = ((MapType) type).getValueType();
                    Iterator<Map.Entry<String, JsonNode>> it = node.fields();
                    while (it.hasNext()) {
                        Map.Entry<String, JsonNode> e = it.next();
                        if (!e.getValue().isNull()) {
                            validateValue(e.getValue(), valueType, path + "['" + e.getKey() + "']");
                        }
                    }
                }
                return;
            default:
                // no range to check
        }
    }

    private static void validateDecimal(JsonNode node, DecimalType type, String path) {
        BigDecimal value;
        try {
            value = node.isBigDecimal() ? node.decimalValue() : new BigDecimal(node.asText());
        } catch (NumberFormatException e) {
            return; // flink-json reports the parse failure itself
        }
        if (DecimalData.fromBigDecimal(value, type.getPrecision(), type.getScale()) == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Value %s at '%s' written by the producer does not fit the declared "
                                    + "type %s (%d digit(s) before the decimal point, %d allowed). "
                                    + "The writer's schema is wider than the reader's; widen the "
                                    + "column or fix the producer.",
                            value.toPlainString(),
                            path,
                            type.asSummaryString(),
                            value.precision() - value.scale(),
                            type.getPrecision() - type.getScale()));
        }
    }
}
