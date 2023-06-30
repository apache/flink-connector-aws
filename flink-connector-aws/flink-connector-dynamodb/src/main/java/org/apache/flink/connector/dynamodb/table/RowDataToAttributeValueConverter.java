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
import org.apache.flink.connector.dynamodb.table.converter.ArrayAttributeConverterProvider;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Converts from Flink Table API internal type of {@link RowData} to {@link AttributeValue}. */
@Internal
public class RowDataToAttributeValueConverter {

    private final DataType physicalDataType;
    private final TableSchema<RowData> tableSchema;

    public RowDataToAttributeValueConverter(DataType physicalDataType) {
        this.physicalDataType = physicalDataType;
        this.tableSchema = createTableSchema();
    }

    public Map<String, AttributeValue> convertRowData(RowData row) {
        return tableSchema.itemToMap(row, false);
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

        return builder.addAttribute(
                getEnhancedType(field.getDataType()),
                a ->
                        a.name(field.getName())
                                .getter(
                                        rowData ->
                                                DataStructureConverters.getConverter(
                                                                field.getDataType())
                                                        .toExternal(
                                                                fieldGetter.getFieldOrNull(
                                                                        rowData)))
                                .setter(((rowData, t) -> {})));
    }

    private <T> EnhancedType<T> getEnhancedType(DataType dataType) {
        if (dataType instanceof KeyValueDataType) {
            return (EnhancedType<T>)
                    EnhancedType.mapOf(
                            getEnhancedType(((KeyValueDataType) dataType).getKeyDataType()),
                            getEnhancedType(((KeyValueDataType) dataType).getValueDataType()));
        } else {
            return (EnhancedType<T>) EnhancedType.of(dataType.getConversionClass());
        }
    }
}
