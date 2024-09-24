package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.DataTypes;
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for the {@link TypeMapper} class. */
public class TypeMapperTest {

    @Test
    public void testMapFlinkTypeToGlueType_Primitives() {
        assertEquals("int", TypeMapper.mapFlinkTypeToGlueType(new IntType()));
        assertEquals("bigint", TypeMapper.mapFlinkTypeToGlueType(new BigIntType()));
        assertEquals("string", TypeMapper.mapFlinkTypeToGlueType(new VarCharType(255)));
        assertEquals("boolean", TypeMapper.mapFlinkTypeToGlueType(new BooleanType()));
        assertEquals("decimal", TypeMapper.mapFlinkTypeToGlueType(new DecimalType(10, 0)));
        assertEquals("float", TypeMapper.mapFlinkTypeToGlueType(new FloatType()));
        assertEquals("double", TypeMapper.mapFlinkTypeToGlueType(new DoubleType()));
        assertEquals("date", TypeMapper.mapFlinkTypeToGlueType(new DateType()));
        assertEquals("timestamp", TypeMapper.mapFlinkTypeToGlueType(new TimestampType(5)));
    }

    @Test
    public void testMapFlinkTypeToGlueType_Array() {
        LogicalType arrayType = new ArrayType(new VarCharType(255));
        assertEquals("array<string>", TypeMapper.mapFlinkTypeToGlueType(arrayType));
    }

    @Test
    public void testMapFlinkTypeToGlueType_Map() {
        LogicalType mapType = new MapType(new VarCharType(255), new IntType());
        assertEquals("map<string,int>", TypeMapper.mapFlinkTypeToGlueType(mapType));
    }

    @Test
    public void testMapFlinkTypeToGlueType_Row() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new VarCharType(255), new IntType()},
                        new String[] {"name", "age"});
        assertEquals("struct<name:string,age:int>", TypeMapper.mapFlinkTypeToGlueType(rowType));
    }

    @Test
    public void testGlueTypeToFlinkType_Primitives() {
        assertEquals(DataTypes.INT(), TypeMapper.glueTypeToFlinkType("int"));
        assertEquals(DataTypes.BIGINT(), TypeMapper.glueTypeToFlinkType("bigint"));
        assertEquals(DataTypes.STRING(), TypeMapper.glueTypeToFlinkType("string"));
        assertEquals(DataTypes.BOOLEAN(), TypeMapper.glueTypeToFlinkType("boolean"));
        assertEquals(DataTypes.DECIMAL(10, 0), TypeMapper.glueTypeToFlinkType("decimal"));
        assertEquals(DataTypes.FLOAT(), TypeMapper.glueTypeToFlinkType("float"));
        assertEquals(DataTypes.DOUBLE(), TypeMapper.glueTypeToFlinkType("double"));
        assertEquals(DataTypes.DATE(), TypeMapper.glueTypeToFlinkType("date"));
        assertEquals(DataTypes.TIMESTAMP(5), TypeMapper.glueTypeToFlinkType("timestamp"));
    }

    @Test
    public void testGlueTypeToFlinkType_Array() {
        LogicalType arrayType = new ArrayType(new VarCharType(255));
        assertEquals("array<string>", TypeMapper.mapFlinkTypeToGlueType(arrayType));
    }

    @Test
    public void testGlueTypeToFlinkType_Map() {
        LogicalType mapType = new MapType(new VarCharType(255), new IntType());
        assertEquals("map<string,int>", TypeMapper.mapFlinkTypeToGlueType(mapType));
    }

    @Test
    public void testGlueTypeToFlinkType_Unsupported() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> TypeMapper.glueTypeToFlinkType("struct<name:string,age:int>"));
    }
}
