package com.amazonaws.services.msf.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

class GlueTypeConverterTest {

    private final GlueTypeConverter converter = new GlueTypeConverter();

    @Test
    void testToGlueTypeForString() {
        DataType flinkType = DataTypes.STRING();
        String glueType = converter.toGlueType(flinkType);
        assertEquals("string", glueType);
    }

    @Test
    void testToGlueTypeForBoolean() {
        DataType flinkType = DataTypes.BOOLEAN();
        String glueType = converter.toGlueType(flinkType);
        assertEquals("boolean", glueType);
    }

    @Test
    void testToGlueTypeForDecimal() {
        DataType flinkType = DataTypes.DECIMAL(10, 2);
        String glueType = converter.toGlueType(flinkType);
        assertEquals("decimal(10,2)", glueType);
    }

    @Test
    void testToGlueTypeForArray() {
        DataType flinkType = DataTypes.ARRAY(DataTypes.STRING());
        String glueType = converter.toGlueType(flinkType);
        assertEquals("array<string>", glueType);
    }

    @Test
    void testToGlueTypeForMap() {
        DataType flinkType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        String glueType = converter.toGlueType(flinkType);
        assertEquals("map<string,int>", glueType);
    }

    @Test
    void testToGlueTypeForStruct() {
        DataType flinkType = DataTypes.ROW(
                DataTypes.FIELD("field1", DataTypes.STRING()),
                DataTypes.FIELD("field2", DataTypes.INT())
        );
        String glueType = converter.toGlueType(flinkType);
        assertEquals("struct<field1:string,field2:int>", glueType);
    }

    @Test
    void testToFlinkTypeForString() {
        DataType flinkType = converter.toFlinkType("string");
        assertEquals(DataTypes.STRING(), flinkType);
    }

    @Test
    void testToFlinkTypeForBoolean() {
        DataType flinkType = converter.toFlinkType("boolean");
        assertEquals(DataTypes.BOOLEAN(), flinkType);
    }

    @Test
    void testToFlinkTypeForDecimal() {
        DataType flinkType = converter.toFlinkType("decimal(10,2)");
        assertEquals(DataTypes.DECIMAL(10, 2), flinkType);
    }

    @Test
    void testToFlinkTypeForArray() {
        DataType flinkType = converter.toFlinkType("array<string>");
        assertEquals(DataTypes.ARRAY(DataTypes.STRING()), flinkType);
    }

    @Test
    void testToFlinkTypeForMap() {
        DataType flinkType = converter.toFlinkType("map<string,int>");
        assertEquals(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), flinkType);
    }

    @Test
    void testToFlinkTypeForStruct() {
        DataType flinkType = converter.toFlinkType("struct<field1:string,field2:int>");
        assertEquals(DataTypes.ROW(
                DataTypes.FIELD("field1", DataTypes.STRING()),
                DataTypes.FIELD("field2", DataTypes.INT())
        ), flinkType);
    }

    @Test
    void testToFlinkTypeThrowsExceptionForInvalidType() {
        assertThrows(UnsupportedOperationException.class, () -> converter.toFlinkType("invalidtype"));
    }

    @Test
    void testToGlueTypeThrowsExceptionForEmptyGlueType() {
        assertThrows(IllegalArgumentException.class, () -> converter.toFlinkType(""));
    }

    @Test
    void testToGlueTypeThrowsExceptionForUnsupportedType() {
        DataType unsupportedType = DataTypes.NULL(); // NULL type isn't supported
        assertThrows(UnsupportedOperationException.class, () -> converter.toGlueType(unsupportedType));
    }

    @Test
    void testSplitStructFieldsWithNestedStructs() {
        String input = "field1:int,field2:struct<sub1:string,sub2:int>";
        String[] fields = converter.splitStructFields(input);
        assertArrayEquals(new String[]{"field1:int", "field2:struct<sub1:string,sub2:int>"}, fields);
    }

    @Test
    void testParseStructType() {
        DataType flinkType = converter.toFlinkType("struct<field1:string,field2:int>");
        assertEquals(DataTypes.ROW(
                DataTypes.FIELD("field1", DataTypes.STRING()),
                DataTypes.FIELD("field2", DataTypes.INT())
        ), flinkType);
    }
    @Test
    void testToGlueTypeForNestedStructs() {
        DataType flinkType = DataTypes.ROW(
                DataTypes.FIELD("outerField", DataTypes.ROW(
                        DataTypes.FIELD("innerField", DataTypes.STRING())
                ))
        );
        String glueType = converter.toGlueType(flinkType);
        assertEquals("struct<outerField:struct<innerField:string>>", glueType);
    }

    @Test
    void testToGlueTypeForNestedMaps() {
        DataType flinkType = DataTypes.MAP(DataTypes.STRING(), DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        String glueType = converter.toGlueType(flinkType);
        assertEquals("map<string,map<string,int>>", glueType);
    }


}
