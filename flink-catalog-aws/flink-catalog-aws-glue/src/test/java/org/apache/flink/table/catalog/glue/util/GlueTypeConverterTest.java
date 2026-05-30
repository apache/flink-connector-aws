package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.glue.exception.UnsupportedDataTypeMappingException;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GlueTypeConverterTest {

    private final GlueTypeConverter converter = new GlueTypeConverter();

    @Test
    void testToGlueDataTypeForString() {
        DataType flinkType = DataTypes.STRING();
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("string", glueType);
    }

    @Test
    void testToGlueDataTypeForBoolean() {
        DataType flinkType = DataTypes.BOOLEAN();
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("boolean", glueType);
    }

    @Test
    void testToGlueDataTypeForDecimal() {
        DataType flinkType = DataTypes.DECIMAL(10, 2);
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("decimal(10,2)", glueType);
    }

    @Test
    void testToGlueDataTypeForArray() {
        DataType flinkType = DataTypes.ARRAY(DataTypes.STRING());
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("array<string>", glueType);
    }

    @Test
    void testToGlueDataTypeForMap() {
        DataType flinkType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("map<string,int>", glueType);
    }

    @Test
    void testToGlueDataTypeForStruct() {
        DataType flinkType =
                DataTypes.ROW(
                        DataTypes.FIELD("field1", DataTypes.STRING()),
                        DataTypes.FIELD("field2", DataTypes.INT()));
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("struct<field1:string,field2:int>", glueType);
    }

    @Test
    void testToFlinkDataTypeForString() {
        DataType flinkType = converter.toFlinkDataType("string");
        Assertions.assertEquals(DataTypes.STRING(), flinkType);
    }

    @Test
    void testToFlinkDataTypeForBoolean() {
        DataType flinkType = converter.toFlinkDataType("boolean");
        Assertions.assertEquals(DataTypes.BOOLEAN(), flinkType);
    }

    @Test
    void testToFlinkDataTypeForDecimal() {
        DataType flinkType = converter.toFlinkDataType("decimal(10,2)");
        Assertions.assertEquals(DataTypes.DECIMAL(10, 2), flinkType);
    }

    @Test
    void testToFlinkDataTypeForArray() {
        DataType flinkType = converter.toFlinkDataType("array<string>");
        Assertions.assertEquals(DataTypes.ARRAY(DataTypes.STRING()), flinkType);
    }

    @Test
    void testToFlinkDataTypeForMap() {
        DataType flinkType = converter.toFlinkDataType("map<string,int>");
        Assertions.assertEquals(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), flinkType);
    }

    @Test
    void testToFlinkDataTypeForStruct() {
        DataType flinkType = converter.toFlinkDataType("struct<field1:string,field2:int>");
        Assertions.assertEquals(
                DataTypes.ROW(
                        DataTypes.FIELD("field1", DataTypes.STRING()),
                        DataTypes.FIELD("field2", DataTypes.INT())),
                flinkType);
    }

    @Test
    void testToFlinkTypeThrowsExceptionForInvalidDataType() {
        Assertions.assertThrows(
                UnsupportedDataTypeMappingException.class, () -> converter.toFlinkDataType("invalidtype"));
    }

    @Test
    void testToGlueTypeThrowsExceptionForEmptyGlueDataType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.toFlinkDataType(""));
    }

    @Test
    void testToGlueTypeThrowsExceptionForUnsupportedDataType() {
        DataType unsupportedType = DataTypes.NULL(); // NULL type isn't supported
        Assertions.assertThrows(
                UnsupportedDataTypeMappingException.class, () -> converter.toGlueDataType(unsupportedType));
    }

    @Test
    void testSplitStructFieldsWithNestedStructs() {
        String input = "field1:int,field2:struct<sub1:string,sub2:int>";
        String[] fields = converter.splitStructFields(input);
        Assertions.assertArrayEquals(
                new String[] {"field1:int", "field2:struct<sub1:string,sub2:int>"}, fields);
    }

    @Test
    void testParseStructType() {
        DataType flinkType = converter.toFlinkDataType("struct<field1:string,field2:int>");
        Assertions.assertEquals(
                DataTypes.ROW(
                        DataTypes.FIELD("field1", DataTypes.STRING()),
                        DataTypes.FIELD("field2", DataTypes.INT())),
                flinkType);
    }

    @Test
    void testToGlueDataTypeForNestedStructs() {
        DataType flinkType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "outerField",
                                DataTypes.ROW(DataTypes.FIELD("innerField", DataTypes.STRING()))));
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("struct<outerField:struct<innerField:string>>", glueType);
    }

    @Test
    void testToGlueDataTypeForNestedMaps() {
        DataType flinkType =
                DataTypes.MAP(
                        DataTypes.STRING(), DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        String glueType = converter.toGlueDataType(flinkType);
        Assertions.assertEquals("map<string,map<string,int>>", glueType);
    }

    @Test
    void testCasePreservationForStructFields() {
        // Test that mixed-case field names in struct are preserved
        // This simulates how Glue actually behaves - preserving case for struct fields
        String glueStructType =
                "struct<FirstName:string,lastName:string,Address:struct<Street:string,zipCode:string>>";

        // Convert to Flink type
        DataType flinkType = converter.toFlinkDataType(glueStructType);

        // The result should be a row type
        Assertions.assertEquals(
                org.apache.flink.table.types.logical.LogicalTypeRoot.ROW,
                flinkType.getLogicalType().getTypeRoot(),
                "Result should be a ROW type");

        // Extract field names from the row type
        org.apache.flink.table.types.logical.RowType rowType =
                (org.apache.flink.table.types.logical.RowType) flinkType.getLogicalType();

        Assertions.assertEquals(3, rowType.getFieldCount(), "Should have 3 top-level fields");

        // Verify exact field name case is preserved
        Assertions.assertEquals(
                "FirstName", rowType.getFieldNames().get(0), "Field name case should be preserved");
        Assertions.assertEquals(
                "lastName", rowType.getFieldNames().get(1), "Field name case should be preserved");
        Assertions.assertEquals(
                "Address", rowType.getFieldNames().get(2), "Field name case should be preserved");

        // Verify nested struct field names case is also preserved
        org.apache.flink.table.types.logical.LogicalType nestedType =
                rowType.getFields().get(2).getType();
        Assertions.assertEquals(
                org.apache.flink.table.types.logical.LogicalTypeRoot.ROW,
                nestedType.getTypeRoot(),
                "Nested field should be a ROW type");

        org.apache.flink.table.types.logical.RowType nestedRowType =
                (org.apache.flink.table.types.logical.RowType) nestedType;

        Assertions.assertEquals(
                "Street",
                nestedRowType.getFieldNames().get(0),
                "Nested field name case should be preserved");
        Assertions.assertEquals(
                "zipCode",
                nestedRowType.getFieldNames().get(1),
                "Nested field name case should be preserved");
    }
}
