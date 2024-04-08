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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.NumericTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.table.converter.ArrayAttributeConverterProvider;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.BeanAttributeGetter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link ElementConverter} that converts an element to a {@link DynamoDbWriteRequest} using
 * TypeInformation provided.
 */
@PublicEvolving
public class DynamoDbTypeInformedElementConverter<T>
        implements ElementConverter<T, DynamoDbWriteRequest> {

    private final CompositeType<T> typeInfo;
    private final boolean ignoreNulls;
    private final TableSchema<T> tableSchema;

    /**
     * Creates a {@link DynamoDbTypeInformedElementConverter} that converts an element to a {@link
     * DynamoDbWriteRequest} using the provided {@link CompositeType}. Usage: {@code new
     * DynamoDbTypeInformedElementConverter<>(TypeInformation.of(MyPojoClass.class))}
     *
     * @param typeInfo The {@link CompositeType} that provides the type information for the element.
     */
    public DynamoDbTypeInformedElementConverter(CompositeType<T> typeInfo) {
        this(typeInfo, true);
    }

    public DynamoDbTypeInformedElementConverter(CompositeType<T> typeInfo, boolean ignoreNulls) {

        try {
            this.typeInfo = typeInfo;
            this.ignoreNulls = ignoreNulls;
            this.tableSchema = createTableSchema(typeInfo);
        } catch (IntrospectionException | IllegalStateException | IllegalArgumentException e) {
            throw new FlinkRuntimeException("Failed to extract DynamoDb table schema", e);
        }
    }

    @Override
    public DynamoDbWriteRequest apply(T input, SinkWriter.Context context) {
        Preconditions.checkNotNull(tableSchema, "TableSchema is not initialized");
        try {
            return DynamoDbWriteRequest.builder()
                    .setType(DynamoDbWriteRequestType.PUT)
                    .setItem(tableSchema.itemToMap(input, ignoreNulls))
                    .build();
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to convert %s to DynamoDbWriteRequest using %s",
                            input, typeInfo),
                    e);
        }
    }

    private <AttributeT> TableSchema<AttributeT> createTableSchema(
            CompositeType<AttributeT> typeInfo) throws IntrospectionException {
        if (typeInfo instanceof RowTypeInfo) {
            return (TableSchema<AttributeT>) createTableSchemaFromRowType((RowTypeInfo) typeInfo);
        } else if (typeInfo instanceof PojoTypeInfo<?>) {
            return createTableSchemaFromPojo((PojoTypeInfo<AttributeT>) typeInfo);
        } else if (typeInfo instanceof TupleTypeInfo<?>) {
            return createTableSchemaFromTuple((TupleTypeInfo<?>) typeInfo);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported TypeInfo %s", typeInfo));
        }
    }

    private TableSchema<Row> createTableSchemaFromRowType(RowTypeInfo typeInfo) {
        StaticTableSchema.Builder<Row> tableSchemaBuilder =
                StaticTableSchema.builder(typeInfo.getTypeClass());

        String[] fieldNames = typeInfo.getFieldNames();
        for (int i = 0; i < typeInfo.getArity(); i++) {
            TypeInformation<?> fieldType = typeInfo.getTypeAt(i);
            int finalI = i;
            addAttribute(
                    tableSchemaBuilder,
                    fieldNames[finalI],
                    (AttributeT) -> AttributeT.getField(finalI),
                    (TypeInformation<? super Object>) fieldType);
        }

        return tableSchemaBuilder.build();
    }

    private <AttributeT> TableSchema<AttributeT> createTableSchemaFromTuple(
            TupleTypeInfo<?> typeInfo) {
        TypeInformation<?>[] fieldTypes = typeInfo.getFieldTypes();
        String[] fieldNames = typeInfo.getFieldNames();

        StaticTableSchema.Builder<?> tableSchemaBuilder =
                StaticTableSchema.builder(typeInfo.getTypeClass());
        for (int i = 0; i < fieldNames.length; i++) {
            int finalI = i;
            addAttribute(
                    tableSchemaBuilder,
                    fieldNames[finalI],
                    (tuple) -> ((Tuple) tuple).getField(finalI),
                    (TypeInformation<?>) fieldTypes[i]);
        }

        return (TableSchema<AttributeT>) tableSchemaBuilder.build();
    }

    private <AttributeT> TableSchema<AttributeT> createTableSchemaFromPojo(
            PojoTypeInfo<AttributeT> typeInfo) throws IntrospectionException {
        StaticTableSchema.Builder<AttributeT> tableSchemaBuilder =
                StaticTableSchema.builder(typeInfo.getTypeClass());
        BeanInfo beanInfo = Introspector.getBeanInfo(typeInfo.getTypeClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            Set<String> fieldNames = new HashSet<>(Arrays.asList(typeInfo.getFieldNames()));
            if (!fieldNames.contains(propertyDescriptor.getName())) {
                // Skip properties that are not part of the PojoTypeInfo
                continue;
            }

            TypeInformation<?> fieldInfo = typeInfo.getTypeAt(propertyDescriptor.getName());
            addAttribute(
                    tableSchemaBuilder,
                    propertyDescriptor.getName(),
                    BeanAttributeGetter.create(
                            typeInfo.getTypeClass(), propertyDescriptor.getReadMethod()),
                    fieldInfo);
        }

        return tableSchemaBuilder.build();
    }

    private <T, AttributeT> void addAttribute(
            StaticTableSchema.Builder<T> builder,
            String fieldName,
            Function<T, AttributeT> getter,
            TypeInformation<AttributeT> typeInfo) {
        builder.addAttribute(
                typeInfo.getTypeClass(),
                a ->
                        a.name(fieldName)
                                .getter(getter)
                                .setter((e, o) -> {})
                                .attributeConverter(getAttributeConverter(typeInfo)));
    }

    private <AttributeT> AttributeConverter<AttributeT> getAttributeConverter(
            TypeInformation<AttributeT> typeInfo) {
        if (typeInfo instanceof BasicTypeInfo) {
            return AttributeConverterProvider.defaultProvider()
                    .converterFor(EnhancedType.of(typeInfo.getTypeClass()));
        } else if (typeInfo.equals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
            return getAttributeConverter(
                    AttributeValueType.B,
                    bytes ->
                            bytes instanceof SdkBytes
                                    ? AttributeValue.fromB((SdkBytes) bytes)
                                    : AttributeValue.fromB(SdkBytes.fromByteArray((byte[]) bytes)));
        } else if (typeInfo instanceof BasicArrayTypeInfo) {
            BasicArrayTypeInfo<AttributeT, ?> basicArrayTypeInfo =
                    (BasicArrayTypeInfo<AttributeT, ?>) typeInfo;
            if (basicArrayTypeInfo.getComponentInfo().equals(BasicTypeInfo.STRING_TYPE_INFO)) {
                return getAttributeConverter(
                        AttributeValueType.SS,
                        array -> AttributeValue.fromSs(Arrays.asList((String[]) array)));
            } else if (basicArrayTypeInfo.getComponentInfo() instanceof NumericTypeInfo) {
                return getAttributeConverter(
                        AttributeValueType.NS,
                        array ->
                                AttributeValue.fromNs(
                                        convertObjectArrayToStringList((Object[]) array)));
            }

            return new ArrayAttributeConverterProvider()
                    .converterFor(EnhancedType.of(typeInfo.getTypeClass()));
        } else if (typeInfo instanceof ObjectArrayTypeInfo) {
            return getObjectArrayTypeConverter((ObjectArrayTypeInfo<AttributeT, ?>) typeInfo);
        } else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
            PrimitiveArrayTypeInfo<AttributeT> primitiveArrayTypeInfo =
                    (PrimitiveArrayTypeInfo<AttributeT>) typeInfo;
            if (primitiveArrayTypeInfo.getComponentType() instanceof NumericTypeInfo) {
                return getAttributeConverter(
                        AttributeValueType.NS,
                        array -> AttributeValue.fromNs(convertPrimitiveArrayToStringList(array)));
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported primitive array typeInfo %s",
                                primitiveArrayTypeInfo.getComponentType()));
            }
        } else if (typeInfo instanceof TupleTypeInfo<?>) {
            return (AttributeConverter<AttributeT>)
                    getTupleTypeConverter((TupleTypeInfo<?>) typeInfo);
        } else if (typeInfo instanceof CompositeType) {
            try {
                TableSchema<AttributeT> schema =
                        createTableSchema((CompositeType<AttributeT>) typeInfo);
                return getAttributeConverter(
                        AttributeValueType.M,
                        o -> AttributeValue.fromM(schema.itemToMap(o, false)));
            } catch (IntrospectionException e) {
                throw new FlinkRuntimeException("Failed to extract nested table schema", e);
            }
        } else {
            throw new IllegalArgumentException(String.format("Unsupported TypeInfo %s", typeInfo));
        }
    }

    private <TupleT extends Tuple> AttributeConverter<TupleT> getTupleTypeConverter(
            TupleTypeInfo<TupleT> typeInfo) {
        AttributeConverter<?>[] tupleFieldConverters =
                new AttributeConverter<?>[typeInfo.getArity()];
        for (int i = 0; i < typeInfo.getArity(); i++) {
            tupleFieldConverters[i] = (getAttributeConverter(typeInfo.getTypeAt(i)));
        }

        return getAttributeConverter(
                AttributeValueType.L,
                tuple -> {
                    List<AttributeValue> attributeValues = new ArrayList<>();
                    for (int i = 0; i < typeInfo.getArity(); i++) {
                        attributeValues.add(
                                tupleFieldConverters[i].transformFrom(tuple.getField(i)));
                    }
                    return AttributeValue.fromL(attributeValues);
                });
    }

    private <ArrayT, AttributeT> AttributeConverter<ArrayT> getObjectArrayTypeConverter(
            ObjectArrayTypeInfo<ArrayT, AttributeT> typeInfo) {
        AttributeConverter<AttributeT> componentAttributeConverter =
                getAttributeConverter(typeInfo.getComponentInfo());
        return getAttributeConverter(
                AttributeValueType.L,
                array -> {
                    AttributeT[] attrArray = (AttributeT[]) array;
                    List<AttributeValue> attributeValues = new ArrayList<>();
                    for (AttributeT attr : attrArray) {
                        attributeValues.add(componentAttributeConverter.transformFrom(attr));
                    }
                    return AttributeValue.fromL(attributeValues);
                });
    }

    private List<String> convertObjectArrayToStringList(Object[] objectArray) {
        List<String> stringList = new ArrayList<>();
        for (Object object : objectArray) {
            stringList.add(object.toString());
        }
        return stringList;
    }

    private List<String> convertPrimitiveArrayToStringList(Object objectArray) {
        if (objectArray instanceof int[]) {
            int[] intArray = (int[]) objectArray;
            List<String> stringList = new ArrayList<>();
            for (int i : intArray) {
                stringList.add(Integer.toString(i));
            }

            return stringList;
        } else if (objectArray instanceof double[]) {
            double[] doubleArray = (double[]) objectArray;
            List<String> stringList = new ArrayList<>();
            for (double d : doubleArray) {
                stringList.add(Double.toString(d));
            }

            return stringList;
        } else if (objectArray instanceof long[]) {
            long[] longArray = (long[]) objectArray;
            List<String> stringList = new ArrayList<>();
            for (long l : longArray) {
                stringList.add(Long.toString(l));
            }

            return stringList;
        } else if (objectArray instanceof float[]) {
            float[] longArray = (float[]) objectArray;
            List<String> stringList = new ArrayList<>();
            for (float f : longArray) {
                stringList.add(Float.toString(f));
            }

            return stringList;
        } else if (objectArray instanceof short[]) {
            short[] longArray = (short[]) objectArray;
            List<String> stringList = new ArrayList<>();
            for (short s : longArray) {
                stringList.add(Short.toString(s));
            }

            return stringList;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported primitive typeInfo %s",
                            objectArray.getClass().getComponentType()));
        }
    }

    private <AttributeT> AttributeConverter<AttributeT> getAttributeConverter(
            AttributeValueType attributeValueType,
            Function<AttributeT, AttributeValue> transformer) {
        return new AttributeConverter<AttributeT>() {
            @Override
            public AttributeValue transformFrom(AttributeT attribute) {
                return transformer.apply(attribute);
            }

            @Override
            public AttributeT transformTo(AttributeValue attributeValue) {
                return null;
            }

            @Override
            public EnhancedType<AttributeT> type() {
                return null;
            }

            @Override
            public AttributeValueType attributeValueType() {
                return attributeValueType;
            }
        };
    }
}
