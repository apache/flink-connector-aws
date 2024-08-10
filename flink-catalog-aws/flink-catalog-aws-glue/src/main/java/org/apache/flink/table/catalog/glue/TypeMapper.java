/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * The {@code TypeMapper} class provides utility methods to map Flink's {@link LogicalType} to AWS.
 * Glue data types and vice versa.
 *
 * <p>This class supports conversion between Flink's logical types and Glue data types, handling
 * both primitive types and complex types such as arrays, maps, and rows. The mapping allows for
 * seamless integration between Flink and AWS Glue, enabling Flink to read from and write to Glue
 * tables with the appropriate data types.
 *
 * <p>For complex types like arrays, maps, and rows, the conversion is handled recursively, ensuring
 * that nested types are also converted accurately.
 *
 * <p>This class currently supports the following mappings:
 *
 * <ul>
 *   <li>Flink {@code IntType} -> Glue {@code int}
 *   <li>Flink {@code BigIntType} -> Glue {@code bigint}
 *   <li>Flink {@code VarCharType} -> Glue {@code string}
 *   <li>Flink {@code BooleanType} -> Glue {@code boolean}
 *   <li>Flink {@code DecimalType} -> Glue {@code decimal}
 *   <li>Flink {@code FloatType} -> Glue {@code float}
 *   <li>Flink {@code DoubleType} -> Glue {@code double}
 *   <li>Flink {@code DateType} -> Glue {@code date}
 *   <li>Flink {@code TimestampType} -> Glue {@code timestamp}
 *   <li>Flink {@code ArrayType} -> Glue {@code array<elementType>}
 *   <li>Flink {@code MapType} -> Glue {@code map<keyType,valueType>}
 *   <li>Flink {@code RowType} -> Glue {@code struct<fieldName:fieldType, ...>}
 * </ul>
 *
 * <p>Note: Struct type handling in {@code glueTypeToFlinkType} is currently not supported and will
 * throw an {@link UnsupportedOperationException}.
 *
 * @see org.apache.flink.table.types.logical.LogicalType
 * @see org.apache.flink.table.api.DataTypes
 * @see org.apache.flink.table.catalog.CatalogTable
 * @see org.apache.flink.table.catalog.ResolvedCatalogTable
 */
public class TypeMapper {

    /**
     * Maps a given Flink {@link LogicalType} to its corresponding AWS Glue data type as a string.
     *
     * @param logicalType the Flink logical type to be mapped
     * @return the corresponding AWS Glue data type as a string
     * @throws UnsupportedOperationException if the Flink type is not supported
     */
    public static String mapFlinkTypeToGlueType(LogicalType logicalType) {
        if (logicalType instanceof IntType) {
            return "int";
        } else if (logicalType instanceof BigIntType) {
            return "bigint";
        } else if (logicalType instanceof VarCharType) {
            return "string";
        } else if (logicalType instanceof BooleanType) {
            return "boolean";
        } else if (logicalType instanceof DecimalType) {
            return "decimal";
        } else if (logicalType instanceof FloatType) {
            return "float";
        } else if (logicalType instanceof DoubleType) {
            return "double";
        } else if (logicalType instanceof DateType) {
            return "date";
        } else if (logicalType instanceof TimestampType) {
            return "timestamp";
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            String elementType = mapFlinkTypeToGlueType(arrayType.getElementType());
            return "array<" + elementType + ">";
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            String keyType = mapFlinkTypeToGlueType(mapType.getKeyType());
            String valueType = mapFlinkTypeToGlueType(mapType.getValueType());
            return "map<" + keyType + "," + valueType + ">";
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            StringBuilder structType = new StringBuilder("struct<");
            for (RowType.RowField field : rowType.getFields()) {
                structType
                        .append(field.getName())
                        .append(":")
                        .append(mapFlinkTypeToGlueType(field.getType()))
                        .append(",");
            }
            // Remove the trailing comma and close the struct definition
            structType.setLength(structType.length() - 1);
            structType.append(">");
            return structType.toString();
        } else {
            throw new UnsupportedOperationException("Unsupported Flink type: " + logicalType);
        }
    }

    /**
     * Maps a given AWS Glue data type as a string to its corresponding Flink {@link
     * AbstractDataType}.
     *
     * @param glueType the AWS Glue data type as a string
     * @return the corresponding Flink data type
     * @throws UnsupportedOperationException if the Glue type is not supported
     */
    public static AbstractDataType<?> glueTypeToFlinkType(String glueType) {
        if (glueType.equals("int")) {
            return DataTypes.INT();
        } else if (glueType.equals("bigint")) {
            return DataTypes.BIGINT();
        } else if (glueType.equals("string")) {
            return DataTypes.STRING();
        } else if (glueType.equals("boolean")) {
            return DataTypes.BOOLEAN();
        } else if (glueType.equals("decimal")) {
            return DataTypes.DECIMAL(10, 0);
        } else if (glueType.equals("float")) {
            return DataTypes.FLOAT();
        } else if (glueType.equals("double")) {
            return DataTypes.DOUBLE();
        } else if (glueType.equals("date")) {
            return DataTypes.DATE();
        } else if (glueType.equals("timestamp")) {
            return DataTypes.TIMESTAMP(5);
        } else if (glueType.startsWith("array")) {
            String elementType = glueType.substring(6, glueType.length() - 1);
            return DataTypes.ARRAY(glueTypeToFlinkType(elementType));
        } else if (glueType.startsWith("map")) {
            // Example: map<string, string> -> DataTypes.MAP(DataTypes.STRING(),
            // DataTypes.STRING())
            int commaIndex = glueType.indexOf(",");
            String keyType = glueType.substring(4, commaIndex);
            String valueType = glueType.substring(commaIndex + 1, glueType.length() - 1);
            return DataTypes.MAP(glueTypeToFlinkType(keyType), glueTypeToFlinkType(valueType));
        } else {
            // Handle struct type if necessary
            // For this case, custom parsing might be required based on struct definition
            throw new UnsupportedOperationException("Struct type not yet supported");
        }
    }
}
