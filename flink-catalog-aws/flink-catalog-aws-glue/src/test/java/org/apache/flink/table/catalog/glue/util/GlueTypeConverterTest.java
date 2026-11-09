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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

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
                UnsupportedDataTypeMappingException.class,
                () -> converter.toFlinkDataType("invalidtype"));
    }

    /**
     * Parameterized types carry their own commas ({@code decimal(10,2)}), which must not be
     * mistaken for the map key/value separator. Regression test for the map separator scan ignoring
     * parentheses.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("mapsWithParameterizedKeyOrValue")
    void testMapWithParameterizedKeyOrValueType(String glueType, DataType expected) {
        Assertions.assertEquals(expected, converter.toFlinkDataType(glueType));
    }

    static Stream<Arguments> mapsWithParameterizedKeyOrValue() {
        return Stream.of(
                Arguments.of(
                        "map<decimal(10,2),string>",
                        DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.STRING())),
                Arguments.of(
                        "map<string,decimal(10,2)>",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.DECIMAL(10, 2))),
                Arguments.of(
                        "map<decimal(10,2),decimal(5,1)>",
                        DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(5, 1))),
                Arguments.of(
                        "map<varchar(255),char(3)>",
                        DataTypes.MAP(DataTypes.VARCHAR(255), DataTypes.CHAR(3))),
                Arguments.of(
                        "map<decimal(10,2),array<decimal(3,1)>>",
                        DataTypes.MAP(
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.ARRAY(DataTypes.DECIMAL(3, 1)))),
                Arguments.of(
                        "map<string,struct<amount:decimal(10,2),currency:varchar(3)>>",
                        DataTypes.MAP(
                                DataTypes.STRING(),
                                DataTypes.ROW(
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2)),
                                        DataTypes.FIELD("currency", DataTypes.VARCHAR(3))))),
                Arguments.of(
                        "array<map<decimal(10,2),string>>",
                        DataTypes.ARRAY(
                                DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.STRING()))));
    }

    /**
     * A struct field without a {@code name:type} separator is malformed. Dropping it would yield a
     * ROW with fewer fields than the Glue table declares, so the converter must fail instead.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedStructDefinitions")
    void testStructWithMalformedFieldFails(String glueType) {
        UnsupportedDataTypeMappingException e =
                Assertions.assertThrows(
                        UnsupportedDataTypeMappingException.class,
                        () -> converter.toFlinkDataType(glueType));
        Assertions.assertTrue(
                e.getMessage().contains("Invalid struct field definition"),
                "unexpected message: " + e.getMessage());
    }

    static Stream<Arguments> malformedStructDefinitions() {
        return Stream.of(
                Arguments.of("struct<a>"),
                Arguments.of("struct<a:int,b>"),
                Arguments.of("struct<a:int,,b:string>"),
                Arguments.of("array<struct<a:int,b>>"),
                Arguments.of("map<string,struct<x>>"));
    }

    // ------------------------------------------------------------------------------------------
    // Composition: every lossless leaf type inside every container position, to depth 3
    // ------------------------------------------------------------------------------------------

    /** Leaf types whose Glue representation round-trips to the same Flink type. */
    private static final DataType[] LOSSLESS_LEAVES = {
        DataTypes.STRING(),
        DataTypes.BOOLEAN(),
        DataTypes.BYTES(),
        DataTypes.TINYINT(),
        DataTypes.SMALLINT(),
        DataTypes.INT(),
        DataTypes.BIGINT(),
        DataTypes.FLOAT(),
        DataTypes.DOUBLE(),
        DataTypes.DATE(),
        DataTypes.TIMESTAMP(6),
        DataTypes.DECIMAL(10, 2),
        DataTypes.DECIMAL(38, 0),
        DataTypes.DECIMAL(1, 1)
    };

    /**
     * Exhaustively composes every lossless leaf into every container position (array element, map
     * key, map value, struct field) up to depth 3 and asserts Flink -> Glue -> Flink is the
     * identity. A flat sweep of the type table cannot find bugs that only exist when two features
     * nest, such as a parameterized type's own comma being taken for a container separator.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("composedTypes")
    void testComposedTypesRoundTrip(DataType flinkType) {
        String glueType = converter.toGlueDataType(flinkType);
        DataType back = converter.toFlinkDataType(glueType);
        Assertions.assertEquals(
                flinkType.getLogicalType().asSerializableString(),
                back.getLogicalType().asSerializableString(),
                "via Glue type: " + glueType);
    }

    static Stream<Arguments> composedTypes() {
        List<DataType> depth1 = new ArrayList<>();
        for (DataType leaf : LOSSLESS_LEAVES) {
            depth1.addAll(wrapInEveryContainer(leaf, leaf));
        }
        // Depth 2 and 3: wrap each depth-1 type once more, alternating the sibling leaf so that
        // maps and structs mix a parameterized type with a plain one in both positions.
        List<DataType> depth2 = new ArrayList<>();
        for (DataType t : depth1) {
            depth2.addAll(wrapInEveryContainer(t, DataTypes.DECIMAL(5, 3)));
        }
        List<DataType> depth3 = new ArrayList<>();
        for (int i = 0; i < depth2.size(); i += 7) { // sample: full depth-3 is ~10k cases
            depth3.addAll(wrapInEveryContainer(depth2.get(i), DataTypes.STRING()));
        }
        return Stream.of(depth1, depth2, depth3).flatMap(List::stream).map(Arguments::of);
    }

    private static List<DataType> wrapInEveryContainer(DataType inner, DataType sibling) {
        return java.util.Arrays.asList(
                DataTypes.ARRAY(inner),
                DataTypes.MAP(inner, sibling),
                DataTypes.MAP(sibling, inner),
                DataTypes.ROW(DataTypes.FIELD("first", inner), DataTypes.FIELD("second", sibling)),
                DataTypes.ROW(DataTypes.FIELD("only", inner)));
    }

    // ------------------------------------------------------------------------------------------
    // Foreign spellings: how Athena / Hive / Spark print the same types
    // ------------------------------------------------------------------------------------------

    /**
     * Other engines print nested types with whitespace after separators and in mixed case.
     * Whitespace must be ignored everywhere except inside struct field names.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("foreignSpellings")
    void testForeignSpellingsOfNestedTypes(String glueType, DataType expected) {
        Assertions.assertEquals(expected, converter.toFlinkDataType(glueType));
    }

    static Stream<Arguments> foreignSpellings() {
        return Stream.of(
                Arguments.of(
                        "struct<a:int, b:string>",
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING()))),
                Arguments.of(
                        "struct< a : int , b : decimal(10,2) >",
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.DECIMAL(10, 2)))),
                Arguments.of(
                        "map< string , array< int > >",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))),
                Arguments.of(
                        "array< decimal( 10 , 2 ) >", DataTypes.ARRAY(DataTypes.DECIMAL(10, 2))),
                Arguments.of(
                        "ARRAY<STRUCT<Id:BIGINT, Name:VARCHAR(20)>>",
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("Id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("Name", DataTypes.VARCHAR(20))))),
                Arguments.of(
                        "Map<Decimal(10,2), Struct<X:Int>>",
                        DataTypes.MAP(
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.ROW(DataTypes.FIELD("X", DataTypes.INT())))));
    }

    /** Empty containers are malformed and must fail clearly rather than produce an empty type. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("emptyContainers")
    void testEmptyContainersFail(String glueType) {
        Assertions.assertThrows(RuntimeException.class, () -> converter.toFlinkDataType(glueType));
    }

    static Stream<Arguments> emptyContainers() {
        return Stream.of(
                Arguments.of("array<>"),
                Arguments.of("map<>"),
                Arguments.of("struct<>"),
                Arguments.of("map<string>"),
                Arguments.of("array<struct<>>"));
    }

    /**
     * Every primitive type AWS Glue / Hive can put on a column (see the Glue Data Catalog "data
     * types" reference), in the spellings other engines write, and the Flink type each maps to.
     */
    static Stream<Arguments> glueToFlinkPrimitives() {
        return Stream.of(
                Arguments.of("boolean", DataTypes.BOOLEAN()),
                Arguments.of("tinyint", DataTypes.TINYINT()),
                Arguments.of("smallint", DataTypes.SMALLINT()),
                Arguments.of("int", DataTypes.INT()),
                Arguments.of("integer", DataTypes.INT()),
                Arguments.of("bigint", DataTypes.BIGINT()),
                Arguments.of("float", DataTypes.FLOAT()),
                Arguments.of("double", DataTypes.DOUBLE()),
                Arguments.of("decimal", DataTypes.DECIMAL(10, 0)),
                Arguments.of("decimal(38,18)", DataTypes.DECIMAL(38, 18)),
                Arguments.of("DECIMAL(10,2)", DataTypes.DECIMAL(10, 2)),
                Arguments.of("string", DataTypes.STRING()),
                Arguments.of("STRING", DataTypes.STRING()),
                Arguments.of("char", DataTypes.STRING()),
                Arguments.of("varchar", DataTypes.STRING()),
                Arguments.of("char(10)", DataTypes.CHAR(10)),
                Arguments.of("varchar(255)", DataTypes.VARCHAR(255)),
                Arguments.of("VARCHAR(65535)", DataTypes.VARCHAR(65535)),
                Arguments.of("binary", DataTypes.BYTES()),
                Arguments.of("date", DataTypes.DATE()),
                Arguments.of("timestamp", DataTypes.TIMESTAMP()),
                Arguments.of(" bigint ", DataTypes.BIGINT()),
                Arguments.of("array<varchar(10)>", DataTypes.ARRAY(DataTypes.VARCHAR(10))),
                Arguments.of(
                        "map<string,decimal(5,2)>",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.DECIMAL(5, 2))),
                Arguments.of(
                        "struct<a:char(3),b:array<int>>",
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.CHAR(3)),
                                DataTypes.FIELD("b", DataTypes.ARRAY(DataTypes.INT())))));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("glueToFlinkPrimitives")
    void testEveryGlueTypeMapsToFlink(String glueType, DataType expected) {
        Assertions.assertEquals(expected, converter.toFlinkDataType(glueType));
    }

    /**
     * Glue type strings written by other engines (Athena, Spark, Hive, crawlers) are not normalized
     * to lowercase, so every type keyword - primitive, parameterized and complex - must be matched
     * case-insensitively. Field names inside a struct must keep their case.
     */
    @ParameterizedTest(name = "{0} (case variants) -> {1}")
    @MethodSource("glueToFlinkPrimitives")
    void testEveryGlueTypeIsMatchedCaseInsensitively(String glueType, DataType expected) {
        Assertions.assertEquals(
                expected,
                converter.toFlinkDataType(transformKeywords(glueType, String::toUpperCase)));
        Assertions.assertEquals(
                expected,
                converter.toFlinkDataType(
                        transformKeywords(
                                glueType,
                                kw -> Character.toUpperCase(kw.charAt(0)) + kw.substring(1))));
    }

    @Test
    void testStructFieldNamesKeepCaseWhenKeywordsAreUpperCase() {
        DataType type =
                converter.toFlinkDataType("ARRAY<STRUCT<UserId:BIGINT,Tags:MAP<STRING,INT>>>");
        Assertions.assertEquals(
                DataTypes.ARRAY(
                        DataTypes.ROW(
                                DataTypes.FIELD("UserId", DataTypes.BIGINT()),
                                DataTypes.FIELD(
                                        "Tags",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())))),
                type);
    }

    /**
     * Applies {@code fn} to every type keyword, leaving struct field names untouched:
     * array&lt;struct&lt;a:int&gt;&gt; -> ARRAY&lt;STRUCT&lt;a:INT&gt;&gt;.
     */
    private static String transformKeywords(String glueType, Function<String, String> fn) {
        java.util.regex.Matcher m = TYPE_KEYWORD.matcher(glueType);
        StringBuffer out = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(out, fn.apply(m.group()));
        }
        m.appendTail(out);
        return out.toString();
    }

    private static final java.util.regex.Pattern TYPE_KEYWORD =
            java.util.regex.Pattern.compile(
                    "\\b(array|map|struct|decimal|varchar|char|string|boolean|tinyint|smallint|"
                            + "integer|int|bigint|float|double|binary|date|timestamp)\\b");

    @Test
    void testHiveUnionTypeIsRejectedWithExplanation() {
        UnsupportedDataTypeMappingException exception =
                Assertions.assertThrows(
                        UnsupportedDataTypeMappingException.class,
                        () -> converter.toFlinkDataType("uniontype<int,string>"));
        Assertions.assertTrue(exception.getMessage().contains("union types"));
    }

    /** Every Flink type root the converter supports, and the Glue type string it writes. */
    static Stream<Arguments> flinkToGluePrimitives() {
        return Stream.of(
                Arguments.of(DataTypes.CHAR(5), "string"),
                Arguments.of(DataTypes.VARCHAR(100), "string"),
                Arguments.of(DataTypes.STRING(), "string"),
                Arguments.of(DataTypes.BOOLEAN(), "boolean"),
                Arguments.of(DataTypes.BINARY(16), "binary"),
                Arguments.of(DataTypes.VARBINARY(16), "binary"),
                Arguments.of(DataTypes.BYTES(), "binary"),
                Arguments.of(DataTypes.DECIMAL(20, 4), "decimal(20,4)"),
                Arguments.of(DataTypes.TINYINT(), "tinyint"),
                Arguments.of(DataTypes.SMALLINT(), "smallint"),
                Arguments.of(DataTypes.INT(), "int"),
                Arguments.of(DataTypes.BIGINT(), "bigint"),
                Arguments.of(DataTypes.FLOAT(), "float"),
                Arguments.of(DataTypes.DOUBLE(), "double"),
                Arguments.of(DataTypes.DATE(), "date"),
                Arguments.of(DataTypes.TIME(3), "string"),
                Arguments.of(DataTypes.TIMESTAMP(3), "timestamp"),
                Arguments.of(DataTypes.TIMESTAMP_LTZ(3), "timestamp"),
                Arguments.of(DataTypes.INT().notNull(), "int"),
                Arguments.of(DataTypes.ARRAY(DataTypes.TIMESTAMP(3)), "array<timestamp>"),
                Arguments.of(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())),
                        "map<string,array<int>>"),
                Arguments.of(
                        DataTypes.ROW(
                                DataTypes.FIELD("Id", DataTypes.BIGINT()),
                                DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING()))),
                        "struct<Id:bigint,tags:array<string>>"));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("flinkToGluePrimitives")
    void testEveryFlinkTypeMapsToGlue(DataType flinkType, String expected) {
        Assertions.assertEquals(expected, converter.toGlueDataType(flinkType));
    }

    /**
     * Types Glue cannot store lose information (precision, time zone, TIME) on the way there; the
     * catalog persists the declared type separately, so here we only pin what the lossy mapping
     * reads back as.
     */
    @Test
    void testLossyTypesReadBackAsDocumented() {
        Assertions.assertEquals(
                DataTypes.TIMESTAMP(),
                converter.toFlinkDataType(converter.toGlueDataType(DataTypes.TIMESTAMP_LTZ(3))));
        Assertions.assertEquals(
                DataTypes.STRING(),
                converter.toFlinkDataType(converter.toGlueDataType(DataTypes.TIME(3))));
        Assertions.assertEquals(
                DataTypes.STRING(),
                converter.toFlinkDataType(converter.toGlueDataType(DataTypes.VARCHAR(10))));
    }

    @Test
    void testUnsupportedFlinkTypesAreRejected() {
        for (DataType unsupported :
                new DataType[] {
                    DataTypes.INTERVAL(DataTypes.DAY()),
                    DataTypes.MULTISET(DataTypes.STRING()),
                    DataTypes.TIMESTAMP_WITH_TIME_ZONE(3),
                    DataTypes.NULL()
                }) {
            Assertions.assertThrows(
                    UnsupportedDataTypeMappingException.class,
                    () -> converter.toGlueDataType(unsupported),
                    unsupported.toString());
        }
    }

    @Test
    void testToGlueTypeThrowsExceptionForEmptyGlueDataType() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> converter.toFlinkDataType(""));
    }

    @Test
    void testToGlueTypeThrowsExceptionForUnsupportedDataType() {
        DataType unsupportedType = DataTypes.NULL(); // NULL type isn't supported
        Assertions.assertThrows(
                UnsupportedDataTypeMappingException.class,
                () -> converter.toGlueDataType(unsupportedType));
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
