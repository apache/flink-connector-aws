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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.dynamodb.util.ComplexPayload;
import org.apache.flink.connector.dynamodb.util.Order;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType.PUT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamoDbTypeInformedElementConverter}. */
public class DynamoDbTypeInformedElementConverterTest {

    @Test
    void simpleTypeConversion() {
        DynamoDbTypeInformedElementConverter<Order> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        (CompositeType<Order>) TypeInformation.of(Order.class));

        Order order = new Order("orderId", 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);

        assertThat(actual.getType()).isEqualTo(PUT);
        assertThat(actual.getItem()).containsOnlyKeys("orderId", "quantity", "total");
        assertThat(actual.getItem().get("orderId").s()).isEqualTo("orderId");
        assertThat(actual.getItem().get("quantity").n()).isEqualTo("1");
        assertThat(actual.getItem().get("total").n()).isEqualTo("2.0");
    }

    @Test
    void complexTypeConversion() {
        TypeInformation<ComplexPayload> typeInformation = TypeInformation.of(ComplexPayload.class);
        DynamoDbTypeInformedElementConverter<ComplexPayload> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        (CompositeType<ComplexPayload>) typeInformation);

        ComplexPayload payload =
                new ComplexPayload(
                        "stringFieldVal",
                        new String[] {"stringArrayFieldVal1", "stringArrayFieldVal2"},
                        new int[] {10, 20},
                        new ComplexPayload.InnerPayload(true, new byte[] {1, 0, 10}),
                        Tuple2.of(1, "tuple2FieldVal"));

        DynamoDbWriteRequest actual = elementConverter.apply(payload, null);

        assertThat(actual.getType()).isEqualTo(PUT);
        assertThat(actual.getItem())
                .containsOnlyKeys(
                        "stringField",
                        "stringArrayField",
                        "intArrayField",
                        "innerPayload",
                        "tupleField");
        assertThat(actual.getItem().get("stringArrayField").ss())
                .containsExactly("stringArrayFieldVal1", "stringArrayFieldVal2");
        assertThat(actual.getItem().get("intArrayField").ns()).containsExactly("10", "20");

        // verify tupleField
        assertThat(actual.getItem().get("tupleField").l()).isNotNull();
        assertThat(actual.getItem().get("tupleField").l()).hasSize(2);
        assertThat(actual.getItem().get("tupleField").l().get(0).n()).isEqualTo("1");
        assertThat(actual.getItem().get("tupleField").l().get(1).s()).isEqualTo("tuple2FieldVal");

        // verify innerPayload
        assertThat(actual.getItem().get("innerPayload").m()).isNotNull();
        Map<String, AttributeValue> innerPayload = actual.getItem().get("innerPayload").m();

        assertThat(innerPayload).containsOnlyKeys("primitiveBooleanField", "byteArrayField");
        assertThat(innerPayload.get("primitiveBooleanField").bool()).isTrue();
        assertThat(innerPayload.get("byteArrayField").b())
                .isEqualTo(SdkBytes.fromByteArray(new byte[] {1, 0, 10}));
    }

    @Test
    void convertTupleType() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(
                                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

        Tuple element = Tuple2.of("stringVal", 10);

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem()).containsOnlyKeys("f0", "f1");
        assertThat(actual.getItem().get("f0").s()).isEqualTo("stringVal");
        assertThat(actual.getItem().get("f1").n()).isEqualTo("10");
    }

    @Test
    void convertRowType() {
        RowTypeInfo rowTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.DOUBLE_TYPE_INFO
                        },
                        new String[] {"stringRowFiled", "IntRowField", "DoubleRowField"});
        DynamoDbTypeInformedElementConverter<Row> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(rowTypeInfo);

        LinkedHashMap<String, Integer> rowMap = new LinkedHashMap<>();
        rowMap.put("stringRowFiled", 1);
        rowMap.put("IntRowField", 2);
        rowMap.put("DoubleRowField", 3);
        Row element =
                RowUtils.createRowWithNamedPositions(
                        RowKind.INSERT, new Object[] {"stringVal", 10, 20.0}, rowMap);

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem())
                .containsOnlyKeys("stringRowFiled", "IntRowField", "DoubleRowField");
        assertThat(actual.getItem().get("stringRowFiled").s()).isEqualTo("stringVal");
        assertThat(actual.getItem().get("IntRowField").n()).isEqualTo("10");
        assertThat(actual.getItem().get("DoubleRowField").n()).isEqualTo("20.0");
    }

    @Test
    void convertObjectArray() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(
                                ObjectArrayTypeInfo.getInfoFor(TypeInformation.of(Order.class))));

        Tuple element =
                Tuple1.of(
                        new Order[] {new Order("orderId1", 1, 2.0), new Order("orderId2", 3, 4.0)});

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem()).containsOnlyKeys("f0");
        assertThat(actual.getItem().get("f0").l()).hasSize(2);
        assertThat(actual.getItem().get("f0").l().get(0).m())
                .containsOnlyKeys("orderId", "quantity", "total");
        assertThat(actual.getItem().get("f0").l().get(0).m().get("orderId").s())
                .isEqualTo("orderId1");
        assertThat(actual.getItem().get("f0").l().get(0).m().get("quantity").n()).isEqualTo("1");
        assertThat(actual.getItem().get("f0").l().get(0).m().get("total").n()).isEqualTo("2.0");
        assertThat(actual.getItem().get("f0").l().get(1).m())
                .containsOnlyKeys("orderId", "quantity", "total");
        assertThat(actual.getItem().get("f0").l().get(1).m().get("orderId").s())
                .isEqualTo("orderId2");
        assertThat(actual.getItem().get("f0").l().get(1).m().get("quantity").n()).isEqualTo("3");
        assertThat(actual.getItem().get("f0").l().get(1).m().get("total").n()).isEqualTo("4.0");
    }

    @Test
    void convertLongPrimitiveArray() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO));

        Tuple element = Tuple1.of(new long[] {1, 2, 3});

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem()).containsOnlyKeys("f0");
        assertThat(actual.getItem().get("f0").ns()).containsExactly("1", "2", "3");
    }

    @Test
    void convertFloatPrimitiveArray() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(
                                PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO));

        Tuple element = Tuple1.of(new float[] {1.0f, 2.0f, 3.0f});

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem()).containsOnlyKeys("f0");
        assertThat(actual.getItem().get("f0").ns()).containsExactly("1.0", "2.0", "3.0");
    }

    @Test
    void convertDoublePrimitiveArray() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(
                                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));

        Tuple element = Tuple1.of(new double[] {1.0, 2.0, 3.0});

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem()).containsOnlyKeys("f0");
        assertThat(actual.getItem().get("f0").ns()).containsExactly("1.0", "2.0", "3.0");
    }

    @Test
    void convertShortPrimitiveArray() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(
                                PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO));

        Tuple element = Tuple1.of(new short[] {1, 2, 3});

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem()).containsOnlyKeys("f0");
        assertThat(actual.getItem().get("f0").ns()).containsExactly("1", "2", "3");
    }

    @Test
    void unsupportedTypeIsWrappedByFlinkException() {

        Assertions.assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(
                        () ->
                                new DynamoDbTypeInformedElementConverter<>(
                                        new TupleTypeInfo<>(
                                                BasicTypeInfo.DATE_TYPE_INFO,
                                                BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO)))
                .withCauseInstanceOf(IllegalStateException.class)
                .withMessageContaining("Failed to extract DynamoDb table schema");
    }

    @Test
    void unmatchedTypeIsWrappedByFlinkException() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO));

        Assertions.assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(() -> elementConverter.apply(Tuple1.of("nan"), null))
                .withCauseInstanceOf(ClassCastException.class);
    }

    @Test
    void convertOrderToDynamoDbWriteRequestWithIgnoresNullByDefault() {
        DynamoDbTypeInformedElementConverter<Order> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        (CompositeType<Order>) TypeInformation.of(Order.class));
        Order order = new Order(null, 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);
        assertThat(actual.getItem()).containsOnlyKeys("quantity", "total");
    }

    @Test
    void convertOrderToDynamoDbWriteRequestWritesNullIfConfigured() {
        DynamoDbTypeInformedElementConverter<Order> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        (CompositeType<Order>) TypeInformation.of(Order.class), false);
        Order order = new Order(null, 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);

        assertThat(actual.getItem()).containsOnlyKeys("orderId", "quantity", "total");
        assertThat(actual.getItem().get("orderId").nul()).isTrue();
    }

    @Test
    void convertSdkBytesAsTypeArray() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));

        Tuple element = Tuple1.of(SdkBytes.fromByteArray(new byte[] {1, 0, 10}));

        DynamoDbWriteRequest actual = elementConverter.apply(element, null);
        assertThat(actual.getItem()).containsOnlyKeys("f0");
        assertThat(actual.getItem().get("f0").b())
                .isEqualTo(SdkBytes.fromByteArray(new byte[] {1, 0, 10}));
    }

    @Test
    void convertInvalidByteArrayThrowsException() {
        DynamoDbTypeInformedElementConverter<Tuple> elementConverter =
                new DynamoDbTypeInformedElementConverter<>(
                        new TupleTypeInfo<>(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));

        Tuple element = Tuple1.of("invalidByteArray");

        Assertions.assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(() -> elementConverter.apply(element, null))
                .withCauseInstanceOf(ClassCastException.class);
    }

    @Test
    void convertUnsupportedPrimitiveArrayThrowsException() {
        Assertions.assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(
                        () ->
                                new DynamoDbTypeInformedElementConverter<>(
                                        new TupleTypeInfo<>(
                                                PrimitiveArrayTypeInfo
                                                        .BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)))
                .withCauseInstanceOf(IllegalArgumentException.class)
                .havingCause()
                .withMessageContaining(
                        "Unsupported primitive array typeInfo " + BasicTypeInfo.BOOLEAN_TYPE_INFO);
    }

    @Test
    void convertUnsupportedCompositeTypeThrowsException() {
        CompositeType<Order> unsupportedCompositeType =
                new CompositeType<Order>(Order.class) {
                    @Override
                    public boolean isBasicType() {
                        return false;
                    }

                    @Override
                    public boolean isTupleType() {
                        return false;
                    }

                    @Override
                    public int getArity() {
                        return 0;
                    }

                    @Override
                    public int getTotalFields() {
                        return 0;
                    }

                    @Override
                    public <T> TypeInformation<T> getTypeAt(int pos) {
                        return null;
                    }

                    @Override
                    protected TypeComparatorBuilder<Order> createTypeComparatorBuilder() {
                        return null;
                    }

                    @Override
                    public <T> TypeInformation<T> getTypeAt(String fieldExpression) {
                        return null;
                    }

                    @Override
                    public String[] getFieldNames() {
                        return new String[0];
                    }

                    @Override
                    public int getFieldIndex(String fieldName) {
                        return 0;
                    }

                    @Override
                    public boolean isKeyType() {
                        return false;
                    }

                    @Override
                    public TypeSerializer<Order> createSerializer(
                            SerializerConfig serializerConfig) {
                        return null;
                    }

                    @Override
                    public Class<Order> getTypeClass() {
                        return null;
                    }

                    @Override
                    public void getFlatFields(String s, int i, List<FlatFieldDescriptor> list) {}
                };

        Assertions.assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(
                        () ->
                                new DynamoDbTypeInformedElementConverter<>(unsupportedCompositeType)
                                        .open(null))
                .withCauseInstanceOf(IllegalArgumentException.class)
                .havingCause()
                .withMessageContaining("Unsupported TypeInfo " + unsupportedCompositeType);
    }
}
