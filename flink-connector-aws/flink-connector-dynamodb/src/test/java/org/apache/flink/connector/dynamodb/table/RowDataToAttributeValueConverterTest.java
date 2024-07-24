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

package org.apache.flink.connector.dynamodb.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowDataToAttributeValueConverter}. */
public class RowDataToAttributeValueConverterTest {

    @Test
    void testChar() {
        String key = "key";
        String value = "some_char";

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.CHAR(9)));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(StringData.fromString(value)));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().s(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testVarChar() {
        String key = "key";
        String value = "some_var_char";

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.VARCHAR(13)));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(StringData.fromString(value)));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().s(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testString() {
        String key = "key";
        String value = "some_string";

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.STRING()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(StringData.fromString(value)));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().s(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testBoolean() {
        String key = "key";
        boolean value = true;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.BOOLEAN()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().bool(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testDecimal() {
        String key = "key";
        BigDecimal value = BigDecimal.valueOf(1.001);

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.DECIMAL(5, 4)));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(DecimalData.fromBigDecimal(value, 5, 4)));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().n("1.0010").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testTinyInt() {
        String key = "key";
        byte value = 5;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.TINYINT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().n("5").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testSmallInt() {
        String key = "key";
        short value = 256;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.SMALLINT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().n("256").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testInt() {
        String key = "key";
        int value = 65536;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.INT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().n("65536").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testBigInt() {
        String key = "key";
        long value = 4294967295L;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.BIGINT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().n("4294967295").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testFloat() {
        String key = "key";
        float value = 123456789123456789.000001f;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.FLOAT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().n("1.23456791E17").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testDouble() {
        String key = "key";
        double value = 1.23456789123456789e19;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.DOUBLE()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().n("1.234567891234568E19").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testTimestamp() {
        String key = "key";
        LocalDateTime value = LocalDateTime.of(2022, 11, 10, 0, 0);

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.TIMESTAMP()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(TimestampData.fromLocalDateTime(value)));
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().s("2022-11-10T00:00").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testStringArray() {
        String key = "key";
        String[] value = {"Some", "String", "Array"};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.STRING())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, StringData::fromString));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(s -> AttributeValue.builder().s(s).build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testBooleanArray() {
        String key = "key";
        Boolean[] value = {true, false, true};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.BOOLEAN())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        bool ->
                                                                AttributeValue.builder()
                                                                        .bool(bool)
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testBigDecimalArray() {
        String key = "key";
        BigDecimal[] value = {BigDecimal.ONE, BigDecimal.ZERO};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.DECIMAL(1, 0))));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, d -> DecimalData.fromBigDecimal(d, 1, 0)));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        s ->
                                                                AttributeValue.builder()
                                                                        .n(String.valueOf(s))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testByteArray() {
        String key = "key";
        Byte[] value = {1, 2, 3, 4};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.TINYINT())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        byt ->
                                                                AttributeValue.builder()
                                                                        .n(String.valueOf(byt))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testShortArray() {
        String key = "key";
        Short[] value = {1, 2, 3, 4};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.SMALLINT())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        sht ->
                                                                AttributeValue.builder()
                                                                        .n(String.valueOf(sht))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testIntegerArray() {
        String key = "key";
        Integer[] value = {1, 2, 3, 4};

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.INT())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        integer ->
                                                                AttributeValue.builder()
                                                                        .n(String.valueOf(integer))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testLongArray() {
        String key = "key";
        Long[] value = {1L, 2L, 3L, 4L};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.BIGINT())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        lng ->
                                                                AttributeValue.builder()
                                                                        .n(String.valueOf(lng))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testFloatArray() {
        String key = "key";
        Float[] value = {1f, 2f, 3f, 4f};

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.FLOAT())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        flt ->
                                                                AttributeValue.builder()
                                                                        .n(String.valueOf(flt))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testDoubleArray() {
        String key = "key";
        Double[] value = {1E1, 2E2, 3E3, 4E4};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.DOUBLE())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        flt ->
                                                                AttributeValue.builder()
                                                                        .n(String.valueOf(flt))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testLocalDateTimeArray() {
        String key = "key";
        LocalDateTime[] value = {LocalDateTime.now(), LocalDateTime.now(), LocalDateTime.now()};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.TIMESTAMP())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, TimestampData::fromLocalDateTime));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        ld ->
                                                                AttributeValue.builder()
                                                                        .s(String.valueOf(ld))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testInstantArray() {
        String key = "key";
        Instant[] value = {Instant.now(), Instant.now(), Instant.now()};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, null);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, TimestampData::fromInstant));
        Map<String, AttributeValue> expectedResult =
                singletonMap(
                        key,
                        AttributeValue.builder()
                                .l(
                                        Arrays.stream(value)
                                                .map(
                                                        ld ->
                                                                AttributeValue.builder()
                                                                        .s(String.valueOf(ld))
                                                                        .build())
                                                .collect(Collectors.toList()))
                                .build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testDeleteOnlyPrimaryKey() {
        String key = "key";
        String value = "some_string";
        String otherField = "other_field";
        String otherValue = "other_value";
        Set<String> primaryKeys = new HashSet<>(Collections.singletonList(key));

        // Create a Row with two fields - "key" and "otherField".  "key" is the single primary key.
        // For a Delete request, only "key" should be included in the expectedResult, and not "otherField".
        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.STRING()), DataTypes.FIELD(otherField, DataTypes.STRING()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, primaryKeys);
        RowData rowData = createElementWithMultipleFields(StringData.fromString(value), StringData.fromString(otherValue));
        rowData.setRowKind(RowKind.DELETE);

        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(rowData);
        Map<String, AttributeValue> expectedResult =
                singletonMap(key, AttributeValue.builder().s(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
        assertThat(expectedResult).containsAllEntriesOf(actualResult);
    }

    @Test
    void testDeleteOnlyPrimaryKeys() {
        String key = "key";
        String value = "some_string";
        String additionalKey = "additional_key";
        String additionalValue = "additional_value";
        String otherField = "other_field";
        String otherValue = "other_value";
        Set<String> primaryKeys = new HashSet<>();
        primaryKeys.add(key);
        primaryKeys.add(additionalKey);

        // Create a Row with three fields - "key", "additional_key", and "otherField".
        // "key" and "additional_key" make up the composite primary key.
        // For a Delete request, only "key" and "additional_key" should be included in the expectedResult, and not "otherField".
        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD(key, DataTypes.STRING()),
                DataTypes.FIELD(additionalKey, DataTypes.STRING()),
                DataTypes.FIELD(otherField, DataTypes.STRING()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, primaryKeys);
        RowData rowData = createElementWithMultipleFields(
                StringData.fromString(value), StringData.fromString(additionalValue), StringData.fromString(otherValue));
        rowData.setRowKind(RowKind.DELETE);

        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(rowData);
        Map<String, AttributeValue> expectedResult = new HashMap<>();
        expectedResult.put(key, AttributeValue.builder().s(value).build());
        expectedResult.put(additionalKey, AttributeValue.builder().s(additionalValue).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
        assertThat(expectedResult).containsAllEntriesOf(actualResult);
    }

    @Test
    void testPKIgnoredForInsert() {
        String key = "key";
        String value = "some_string";
        String otherField = "other_field";
        String otherValue = "other_value";
        Set<String> primaryKeys = new HashSet<>(Collections.singletonList(key));

        // Create a Row with two fields - "key" and "otherField".  "key" is the primary key.
        // For an Insert request, all fields should be included regardless of the Primary Key.
        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.STRING()), DataTypes.FIELD(otherField, DataTypes.STRING()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, primaryKeys);
        RowData rowData = createElementWithMultipleFields(StringData.fromString(value), StringData.fromString(otherValue));
        rowData.setRowKind(RowKind.INSERT);

        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(rowData);
        Map<String, AttributeValue> expectedResult = new HashMap<>();
        expectedResult.put(key, AttributeValue.builder().s(value).build());
        expectedResult.put(otherField, AttributeValue.builder().s(otherValue).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
        assertThat(expectedResult).containsAllEntriesOf(actualResult);
    }

    @Test
    void testPKIgnoredForUpdateAfter() {
        String key = "key";
        String value = "some_string";
        String otherField = "other_field";
        String otherValue = "other_value";
        Set<String> primaryKeys = new HashSet<>(Collections.singletonList(key));

        // Create a Row with two fields - "key" and "otherField".  "key" is the primary key.
        // For an UPDATE_BEFORE request, all fields should be included regardless of the Primary Key.
        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.STRING()), DataTypes.FIELD(otherField, DataTypes.STRING()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType, primaryKeys);
        RowData rowData = createElementWithMultipleFields(StringData.fromString(value), StringData.fromString(otherValue));
        rowData.setRowKind(RowKind.UPDATE_AFTER);

        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(rowData);
        Map<String, AttributeValue> expectedResult = new HashMap<>();
        expectedResult.put(key, AttributeValue.builder().s(value).build());
        expectedResult.put(otherField, AttributeValue.builder().s(otherValue).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
        assertThat(expectedResult).containsAllEntriesOf(actualResult);
    }

    private RowData createElement(Object value) {
        GenericRowData element = new GenericRowData(1);
        element.setField(0, value);
        return element;
    }

    private RowData createElementWithMultipleFields(Object... values) {
        GenericRowData element = new GenericRowData(values.length);
        for (int i = 0; i < values.length; i++) {
            element.setField(i, values[i]);
        }
        return element;
    }

    private <T> RowData createArray(T[] value, Function<T, Object> elementConverter) {
        return createElement(
                new GenericArrayData(Arrays.stream(value).map(elementConverter).toArray()));
    }
}
