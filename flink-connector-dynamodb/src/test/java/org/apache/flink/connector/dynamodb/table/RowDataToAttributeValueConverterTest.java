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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowDataToAttributeValueConverter}. */
public class RowDataToAttributeValueConverterTest {

    @Test
    void testChar() {
        String key = "key";
        String value = "some_char";

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.CHAR(9)));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(StringData.fromString(value)));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().s(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testVarChar() {
        String key = "key";
        String value = "some_var_char";

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.VARCHAR(13)));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(StringData.fromString(value)));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().s(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testString() {
        String key = "key";
        String value = "some_string";

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.STRING()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(StringData.fromString(value)));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().s(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testBoolean() {
        String key = "key";
        boolean value = true;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.BOOLEAN()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().bool(value).build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testDecimal() {
        String key = "key";
        BigDecimal value = BigDecimal.valueOf(1.001);

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.DECIMAL(5, 4)));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(DecimalData.fromBigDecimal(value, 5, 4)));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().n("1.0010").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testTinyInt() {
        String key = "key";
        byte value = 5;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.TINYINT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().n("5").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testSmallInt() {
        String key = "key";
        short value = 256;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.SMALLINT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().n("256").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testInt() {
        String key = "key";
        int value = 65536;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.INT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().n("65536").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testBigInt() {
        String key = "key";
        long value = 4294967295L;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.BIGINT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().n("4294967295").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testFloat() {
        String key = "key";
        float value = 123456789123456789.000001f;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.FLOAT()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().n("1.23456791E17").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testDouble() {
        String key = "key";
        double value = 1.23456789123456789e19;

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.DOUBLE()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createElement(value));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().n("1.234567891234568E19").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testTimestamp() {
        String key = "key";
        LocalDateTime value = LocalDateTime.of(2022, 11, 10, 0, 0);

        DataType dataType = DataTypes.ROW(DataTypes.FIELD(key, DataTypes.TIMESTAMP()));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createElement(TimestampData.fromLocalDateTime(value)));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(key, AttributeValue.builder().s("2022-11-10T00:00").build());

        assertThat(actualResult).containsAllEntriesOf(expectedResult);
    }

    @Test
    void testStringArray() {
        String key = "key";
        String[] value = {"Some", "String", "Array"};

        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD(key, DataTypes.ARRAY(DataTypes.STRING())));
        RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, StringData::fromString));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, d -> DecimalData.fromBigDecimal(d, 1, 0)));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(createArray(value, t -> t));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, TimestampData::fromLocalDateTime));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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
                new RowDataToAttributeValueConverter(dataType);
        Map<String, AttributeValue> actualResult =
                rowDataToAttributeValueConverter.convertRowData(
                        createArray(value, TimestampData::fromInstant));
        Map<String, AttributeValue> expectedResult =
                ImmutableMap.of(
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

    private RowData createElement(Object value) {
        GenericRowData element = new GenericRowData(1);
        element.setField(0, value);
        return element;
    }

    private <T> RowData createArray(T[] value, Function<T, Object> elementConverter) {
        return createElement(
                new GenericArrayData(Arrays.stream(value).map(elementConverter).toArray()));
    }
}
