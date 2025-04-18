package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GlueTypeConverterTest {

    private final GlueTypeConverter converter = new GlueTypeConverter();

    @Test
    void testToGlueTypeForString() {
        DataType flinkType = DataTypes.STRING();
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("string", glueType);
    }

    @Test
    void testToGlueTypeForBoolean() {
        DataType flinkType = DataTypes.BOOLEAN();
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("boolean", glueType);
    }

    @Test
    void testToGlueTypeForDecimal() {
        DataType flinkType = DataTypes.DECIMAL(10, 2);
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("decimal(10,2)", glueType);
    }

    @Test
    void testToGlueTypeForArray() {
        DataType flinkType = DataTypes.ARRAY(DataTypes.STRING());
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("array<string>", glueType);
    }

    @Test
    void testToGlueTypeForMap() {
        DataType flinkType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("map<string,int>", glueType);
    }

    @Test
    void testToGlueTypeForStruct() {
        DataType flinkType =
                DataTypes.ROW(
                        DataTypes.FIELD("field1", DataTypes.STRING()),
                        DataTypes.FIELD("field2", DataTypes.INT()));
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("struct<field1:string,field2:int>", glueType);
    }

    @Test
    void testToFlinkTypeForString() {
        DataType flinkType = converter.toFlinkType("string");
        Assertions.assertEquals(DataTypes.STRING(), flinkType);
    }

    @Test
    void testToFlinkTypeForBoolean() {
        DataType flinkType = converter.toFlinkType("boolean");
        Assertions.assertEquals(DataTypes.BOOLEAN(), flinkType);
    }

    @Test
    void testToFlinkTypeForDecimal() {
        DataType flinkType = converter.toFlinkType("decimal(10,2)");
        Assertions.assertEquals(DataTypes.DECIMAL(10, 2), flinkType);
    }

    @Test
    void testToFlinkTypeForArray() {
        DataType flinkType = converter.toFlinkType("array<string>");
        Assertions.assertEquals(DataTypes.ARRAY(DataTypes.STRING()), flinkType);
    }

    @Test
    void testToFlinkTypeForMap() {
        DataType flinkType = converter.toFlinkType("map<string,int>");
        Assertions.assertEquals(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), flinkType);
    }

    @Test
    void testToFlinkTypeForStruct() {
        DataType flinkType = converter.toFlinkType("struct<field1:string,field2:int>");
        Assertions.assertEquals(
                DataTypes.ROW(
                        DataTypes.FIELD("field1", DataTypes.STRING()),
                        DataTypes.FIELD("field2", DataTypes.INT())),
                flinkType);
    }

    @Test
    void testToFlinkTypeThrowsExceptionForInvalidType() {
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> converter.toFlinkType("invalidtype"));
    }

    @Test
    void testToGlueTypeThrowsExceptionForEmptyGlueType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.toFlinkType(""));
    }

    @Test
    void testToGlueTypeThrowsExceptionForUnsupportedType() {
        DataType unsupportedType = DataTypes.NULL(); // NULL type isn't supported
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> converter.toGlueType(unsupportedType));
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
        DataType flinkType = converter.toFlinkType("struct<field1:string,field2:int>");
        Assertions.assertEquals(
                DataTypes.ROW(
                        DataTypes.FIELD("field1", DataTypes.STRING()),
                        DataTypes.FIELD("field2", DataTypes.INT())),
                flinkType);
    }

    @Test
    void testToGlueTypeForNestedStructs() {
        DataType flinkType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "outerField",
                                DataTypes.ROW(DataTypes.FIELD("innerField", DataTypes.STRING()))));
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("struct<outerField:struct<innerField:string>>", glueType);
    }

    @Test
    void testToGlueTypeForNestedMaps() {
        DataType flinkType =
                DataTypes.MAP(
                        DataTypes.STRING(), DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        String glueType = converter.toGlueType(flinkType);
        Assertions.assertEquals("map<string,map<string,int>>", glueType);
    }

    @Test
    void testCasePreservationForStructFields() {
        // Test that mixed-case field names in struct are preserved
        // This simulates how Glue actually behaves - preserving case for struct fields
        String glueStructType =
                "struct<FirstName:string,lastName:string,Address:struct<Street:string,zipCode:string>>";

        // Convert to Flink type
        DataType flinkType = converter.toFlinkType(glueStructType);

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
