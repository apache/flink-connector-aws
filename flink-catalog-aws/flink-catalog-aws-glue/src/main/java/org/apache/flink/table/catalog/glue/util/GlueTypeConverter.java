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

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.glue.exception.UnsupportedDataTypeMappingException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for converting Flink types to Glue types and vice versa.
 * Supports the conversion of common primitive, array, map, and struct types.
 */
public class GlueTypeConverter {

    /** Logger for tracking Glue type conversions. */
    private static final Logger LOG = LoggerFactory.getLogger(GlueTypeConverter.class);

    /** Regular expressions for handling specific Glue types. */
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");
    private static final Pattern ARRAY_PATTERN = Pattern.compile("array<(.+)>");
    private static final Pattern MAP_PATTERN = Pattern.compile("map<(.+),(.+)>");
    private static final Pattern STRUCT_PATTERN = Pattern.compile("struct<(.+)>");

    /**
     * Converts a Flink DataType to its corresponding Glue type as a string.
     *
     * @param flinkType The Flink DataType to be converted.
     * @return The Glue type as a string.
     */
    public String toGlueDataType(DataType flinkType) {
        LogicalType logicalType = flinkType.getLogicalType();
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();

        // Handle various Flink types and map them to corresponding Glue types
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return "string";
            case BOOLEAN:
                return "boolean";
            case BINARY:
            case VARBINARY:
                return "binary";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return String.format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return "tinyint";
            case SMALLINT:
                return "smallint";
            case INTEGER:
                return "int";
            case BIGINT:
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DATE:
                return "date";
            case TIME_WITHOUT_TIME_ZONE:
                return "string"; // Glue doesn't have a direct time type, use string
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "timestamp";
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                return "array<" + toGlueDataType(DataTypes.of(arrayType.getElementType())) + ">";
            case MAP:
                MapType mapType = (MapType) logicalType;
                return String.format("map<%s,%s>",
                        toGlueDataType(DataTypes.of(mapType.getKeyType())),
                        toGlueDataType(DataTypes.of(mapType.getValueType())));
            case ROW:
                RowType rowType = (RowType) logicalType;
                StringBuilder structBuilder = new StringBuilder("struct<");
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    if (i > 0) {
                        structBuilder.append(",");
                    }
                    // Keep original field name for nested structs
                    structBuilder.append(rowType.getFieldNames().get(i))
                            .append(":")
                            .append(toGlueDataType(DataTypes.of(rowType.getChildren().get(i))));
                }
                structBuilder.append(">");
                return structBuilder.toString();
            default:
                throw new UnsupportedDataTypeMappingException("Flink type not supported by Glue Catalog: " + logicalType.getTypeRoot());

        }
    }

    /**
     * Converts a Glue type (as a string) to the corresponding Flink DataType.
     *
     * @param glueType The Glue type as a string.
     * @return The corresponding Flink DataType.
     * @throws IllegalArgumentException if the Glue type is invalid or unknown.
     */
    public DataType toFlinkDataType(String glueType) {
        if (glueType == null || glueType.trim().isEmpty()) {
            throw new IllegalArgumentException("Glue type cannot be null or empty");
        }

        // Trim but don't lowercase - we'll handle case-insensitivity per type
        String trimmedGlueType = glueType.trim();

        // Handle DECIMAL type - using lowercase for pattern matching
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(trimmedGlueType.toLowerCase());
        if (decimalMatcher.matches()) {
            int precision = Integer.parseInt(decimalMatcher.group(1));
            int scale = Integer.parseInt(decimalMatcher.group(2));
            return DataTypes.DECIMAL(precision, scale);
        }

        // Handle ARRAY type - using lowercase for pattern matching but preserving content
        Matcher arrayMatcher = ARRAY_PATTERN.matcher(trimmedGlueType);
        if (arrayMatcher.matches()) {
            // Extract from original string to preserve case in content
            int contentStart = trimmedGlueType.indexOf('<') + 1;
            int contentEnd = trimmedGlueType.lastIndexOf('>');
            String elementType = trimmedGlueType.substring(contentStart, contentEnd);
            return DataTypes.ARRAY(toFlinkDataType(elementType));
        }

        // Handle MAP type - using lowercase for pattern matching but preserving content
        Matcher mapMatcher = MAP_PATTERN.matcher(trimmedGlueType);
        if (mapMatcher.matches()) {
            // Extract from original string to preserve case in content
            int contentStart = trimmedGlueType.indexOf('<') + 1;
            int contentEnd = trimmedGlueType.lastIndexOf('>');
            String mapContent = trimmedGlueType.substring(contentStart, contentEnd);

            // Split key and value types
            int commaPos = findMapTypeSeparator(mapContent);
            if (commaPos < 0) {
                throw new IllegalArgumentException("Invalid map type format: " + glueType);
            }

            String keyType = mapContent.substring(0, commaPos).trim();
            String valueType = mapContent.substring(commaPos + 1).trim();

            return DataTypes.MAP(
                    toFlinkDataType(keyType),
                    toFlinkDataType(valueType)
            );
        }

        // Handle STRUCT type - using lowercase for pattern matching but preserving content
        Matcher structMatcher = STRUCT_PATTERN.matcher(trimmedGlueType);
        if (structMatcher.matches()) {
            // Extract from original string to preserve case in field names
            int contentStart = trimmedGlueType.indexOf('<') + 1;
            int contentEnd = trimmedGlueType.lastIndexOf('>');
            String structContent = trimmedGlueType.substring(contentStart, contentEnd);

            return parseStructType(structContent);
        }

        // Handle primitive types (case insensitive)
        switch (trimmedGlueType.toLowerCase()) {
            case "string":
            case "char":
            case "varchar":
                return DataTypes.STRING();
            case "boolean":
                return DataTypes.BOOLEAN();
            case "binary":
                return DataTypes.BYTES();
            case "tinyint":
                return DataTypes.TINYINT();
            case "smallint":
                return DataTypes.SMALLINT();
            case "int":
                return DataTypes.INT();
            case "bigint":
                return DataTypes.BIGINT();
            case "float":
                return DataTypes.FLOAT();
            case "double":
                return DataTypes.DOUBLE();
            case "date":
                return DataTypes.DATE();
            case "timestamp":
                return DataTypes.TIMESTAMP();
            default:
                throw new UnsupportedDataTypeMappingException("Unsupported Glue type: " + glueType);
        }
    }

    /**
     * Helper method to find the comma that separates key and value types in a map.
     * Handles nested types correctly by tracking angle brackets.
     *
     * @param mapContent The content of the map type definition.
     * @return The position of the separator comma, or -1 if not found.
     */
    private int findMapTypeSeparator(String mapContent) {
        int nestedLevel = 0;
        for (int i = 0; i < mapContent.length(); i++) {
            char c = mapContent.charAt(i);
            if (c == '<') {
                nestedLevel++;
            } else if (c == '>') {
                nestedLevel--;
            } else if (c == ',' && nestedLevel == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Parses a struct type definition and returns the corresponding Flink DataType.
     *
     * @param structDefinition The struct definition string to parse.
     * @return The corresponding Flink ROW DataType.
     */
    public DataType parseStructType(String structDefinition) {
        String[] fields = splitStructFields(structDefinition);
        List<DataTypes.Field> flinkFields = new ArrayList<>();

        for (String field : fields) {
            // Important: We need to find the colon separator properly,
            // as field names might contain characters like '<' for nested structs
            int colonPos = field.indexOf(':');
            if (colonPos < 0) {
                LOG.warn("Invalid struct field definition (no colon found): {}", field);
                continue;
            }

            // Extract field name and type, preserving the original case of the field name
            // This is crucial because Glue preserves case for struct fields
            String fieldName = field.substring(0, colonPos).trim();
            String fieldType = field.substring(colonPos + 1).trim();

            // Add field with its original case preserved from Glue
            flinkFields.add(DataTypes.FIELD(fieldName, toFlinkDataType(fieldType)));
        }

        return DataTypes.ROW(flinkFields.toArray(new DataTypes.Field[0]));
    }

    /**
     * Splits the struct definition string into individual field definitions.
     *
     * @param structDefinition The struct definition string to split.
     * @return An array of field definitions.
     */
    public String[] splitStructFields(String structDefinition) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        int nestedLevel = 0;
        int parenLevel = 0;

        // Parse the struct fields while handling nested angle brackets and parentheses.
        for (char c : structDefinition.toCharArray()) {
            if (c == '<') {
                nestedLevel++;
            } else if (c == '>') {
                nestedLevel--;
            } else if (c == '(') {
                parenLevel++;
            } else if (c == ')') {
                parenLevel--;
            }

            // Only split on comma if we're not inside any nested structure (angles or parens)
            if (c == ',' && nestedLevel == 0 && parenLevel == 0) {
                fields.add(currentField.toString().trim());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }

        if (currentField.length() > 0) {
            fields.add(currentField.toString().trim());
        }

        return fields.toArray(new String[0]);
    }
}
