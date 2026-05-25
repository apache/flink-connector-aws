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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.table.converter.ArrayAttributeConverter;
import org.apache.flink.connector.dynamodb.table.converter.ArrayAttributeConverterProvider;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Converts from Flink Table API internal type of {@link RowData} to {@link AttributeValue}. */
@Internal
public class RowDataToAttributeValueConverter {

    private final DataType physicalDataType;
    private final TableSchema<RowData> tableSchema;
    private boolean ignoreNulls = false;

    public RowDataToAttributeValueConverter(DataType physicalDataType) {
        this.physicalDataType = physicalDataType;
        this.tableSchema = createTableSchema();
    }

    public RowDataToAttributeValueConverter(DataType physicalDataType, boolean ignoreNulls) {
        this.physicalDataType = physicalDataType;
        this.tableSchema = createTableSchema();
        this.ignoreNulls = ignoreNulls;
    }

    public Map<String, AttributeValue> convertRowData(RowData row) {
        return tableSchema.itemToMap(row, ignoreNulls);
    }

    private StaticTableSchema<RowData> createTableSchema() {
        List<DataTypes.Field> fields = DataType.getFields(physicalDataType);
        StaticTableSchema.Builder<RowData> builder = TableSchema.builder(RowData.class);

        AttributeConverterProvider newAttributeConverterProvider =
                new ArrayAttributeConverterProvider();
        builder.attributeConverterProviders(
                newAttributeConverterProvider, AttributeConverterProvider.defaultProvider());

        for (int i = 0; i < fields.size(); i++) {
            DataTypes.Field field = fields.get(i);
            RowData.FieldGetter fieldGetter =
                    createFieldGetter(field.getDataType().getLogicalType(), i);

            builder = addAttribute(builder, field, fieldGetter);
        }
        return builder.build();
    }

    private StaticTableSchema.Builder<RowData> addAttribute(
            StaticTableSchema.Builder<RowData> builder,
            DataTypes.Field field,
            RowData.FieldGetter fieldGetter) {

        EnhancedType<Object> enhancedType = getEnhancedType(field.getDataType());
        return builder.addAttribute(
                enhancedType,
                a -> {
                    a.name(field.getName())
                            .getter(
                                    rowData ->
                                            DataStructureConverters.getConverter(
                                                            field.getDataType())
                                                    .toExternalOrNull(
                                                            fieldGetter.getFieldOrNull(rowData)))
                            .setter(((rowData, t) -> {}));
                    buildRowAttributeConverter(field.getDataType())
                            .ifPresent(a::attributeConverter);
                });
    }

    private Optional<AttributeConverter> buildRowAttributeConverter(DataType dataType) {
        if (LogicalTypeRoot.ROW == dataType.getLogicalType().getTypeRoot()) {
            return Optional.of(createRowDocumentConverter(buildRowTableSchema(dataType)));
        }
        if (dataType instanceof CollectionDataType) {
            DataType elementDataType = ((CollectionDataType) dataType).getElementDataType();
            if (LogicalTypeRoot.ROW.equals(elementDataType.getLogicalType().getTypeRoot())) {
                AttributeConverter<Row> elementConverter =
                        createRowDocumentConverter(buildRowTableSchema(elementDataType));
                return Optional.of(
                        new ArrayAttributeConverter<>(
                                elementConverter, EnhancedType.of(Row[].class)));
            }
        }
        return Optional.empty();
    }

    private static AttributeConverter<Row> createRowDocumentConverter(
            TableSchema<Row> tableSchema) {
        return new AttributeConverter<Row>() {
            @Override
            public AttributeValue transformFrom(Row input) {
                return AttributeValue.builder().m(tableSchema.itemToMap(input, false)).build();
            }

            @Override
            public Row transformTo(AttributeValue input) {
                throw new UnsupportedOperationException();
            }

            @Override
            public EnhancedType<Row> type() {
                return EnhancedType.of(Row.class);
            }

            @Override
            public AttributeValueType attributeValueType() {
                return AttributeValueType.M;
            }
        };
    }

    private <T> EnhancedType<T> getEnhancedType(DataType dataType) {
        if (dataType instanceof KeyValueDataType) {
            return (EnhancedType<T>)
                    EnhancedType.mapOf(
                            getEnhancedType(((KeyValueDataType) dataType).getKeyDataType()),
                            getEnhancedType(((KeyValueDataType) dataType).getValueDataType()));
        } else if (LogicalTypeRoot.ROW.equals(dataType.getLogicalType().getTypeRoot())) {
            return (EnhancedType<T>) EnhancedType.of(Row.class);
        } else {
            return (EnhancedType<T>) EnhancedType.of(dataType.getConversionClass());
        }
    }

    private TableSchema<Row> buildRowTableSchema(DataType dataType) {
        StaticTableSchema.Builder<Row> builder = TableSchema.builder(Row.class);
        AttributeConverterProvider newAttributeConverterProvider =
                new ArrayAttributeConverterProvider();
        builder.attributeConverterProviders(
                newAttributeConverterProvider, AttributeConverterProvider.defaultProvider());

        final List<DataTypes.Field> fields = DataType.getFields(dataType);
        IntStream.range(0, fields.size())
                .forEach(
                        idx -> {
                            final DataTypes.Field field = fields.get(idx);
                            final DataType fieldDataType = field.getDataType();
                            builder.addAttribute(
                                    getEnhancedType(fieldDataType),
                                    a -> {
                                        a.name(field.getName())
                                                .getter(row -> row.getField(idx))
                                                .setter((row, t) -> {});
                                        buildRowAttributeConverter(fieldDataType)
                                                .ifPresent(a::attributeConverter);
                                    });
                        });
        return builder.build();
    }
}
