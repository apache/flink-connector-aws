package com.amazonaws.services.msf.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
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
    public String toGlueType(DataType flinkType) {
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
                return "array<" + toGlueType(DataTypes.of(arrayType.getElementType())) + ">";
            case MAP:
                MapType mapType = (MapType) logicalType;
                return String.format("map<%s,%s>",
                        toGlueType(DataTypes.of(mapType.getKeyType())),
                        toGlueType(DataTypes.of(mapType.getValueType())));
            case ROW:
                RowType rowType = (RowType) logicalType;
                StringBuilder structBuilder = new StringBuilder("struct<");
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    if (i > 0) structBuilder.append(",");
                    structBuilder.append(rowType.getFieldNames().get(i))
                            .append(":")
                            .append(toGlueType(DataTypes.of(rowType.getChildren().get(i))));
                }
                structBuilder.append(">");
                return structBuilder.toString();
            default:
                throw new UnsupportedOperationException("Unsupported Flink type: " + logicalType.getTypeRoot());
        }
    }

    /**
     * Converts a Glue type (as a string) to the corresponding Flink DataType.
     *
     * @param glueType The Glue type as a string.
     * @return The corresponding Flink DataType.
     * @throws IllegalArgumentException if the Glue type is invalid or unknown.
     */
    public DataType toFlinkType(String glueType) {
        if (glueType == null || glueType.trim().isEmpty()) {
            throw new IllegalArgumentException("Glue type cannot be null or empty");
        }

        glueType = glueType.toLowerCase().trim();

        // Handle DECIMAL type
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(glueType);
        if (decimalMatcher.matches()) {
            int precision = Integer.parseInt(decimalMatcher.group(1));
            int scale = Integer.parseInt(decimalMatcher.group(2));
            return DataTypes.DECIMAL(precision, scale);
        }

        // Handle ARRAY type
        Matcher arrayMatcher = ARRAY_PATTERN.matcher(glueType);
        if (arrayMatcher.matches()) {
            String elementType = arrayMatcher.group(1);
            return DataTypes.ARRAY(toFlinkType(elementType));
        }

        // Handle MAP type
        Matcher mapMatcher = MAP_PATTERN.matcher(glueType);
        if (mapMatcher.matches()) {
            String keyType = mapMatcher.group(1);
            String valueType = mapMatcher.group(2);
            return DataTypes.MAP(
                    toFlinkType(keyType),
                    toFlinkType(valueType)
            );
        }

        // Handle STRUCT type
        Matcher structMatcher = STRUCT_PATTERN.matcher(glueType);
        if (structMatcher.matches()) {
            return parseStructType(structMatcher.group(1));
        }

        // Handle primitive types
        switch (glueType) {
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
                throw new UnsupportedOperationException("Unsupported Glue type: " + glueType);
        }
    }

    /**
     * Parses a struct type definition and returns the corresponding Flink DataType.
     *
     * @param structDefinition The struct definition string to parse.
     * @return The corresponding Flink ROW DataType.
     */
    private DataType parseStructType(String structDefinition) {
        String[] fields = splitStructFields(structDefinition);
        List<DataTypes.Field> flinkFields = new ArrayList<>();

        // Log the struct parsing action
        LOG.debug("Parsing struct definition: {}", structDefinition);

        for (String field : fields) {
            String[] fieldParts = field.split(":");
            String fieldName = fieldParts[0].trim();
            String fieldType = fieldParts[1].trim();
            flinkFields.add(DataTypes.FIELD(fieldName, toFlinkType(fieldType)));
        }

        return DataTypes.ROW(flinkFields.toArray(new DataTypes.Field[0]));
    }

    /**
     * Splits the struct definition string into individual field definitions.
     *
     * @param structDefinition The struct definition string to split.
     * @return An array of field definitions.
     */
    private String[] splitStructFields(String structDefinition) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        int nestedLevel = 0;

        // Parse the struct fields while handling nested angle brackets.
        for (char c : structDefinition.toCharArray()) {
            if (c == '<') nestedLevel++;
            else if (c == '>') nestedLevel--;

            if (c == ',' && nestedLevel == 0) {
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
