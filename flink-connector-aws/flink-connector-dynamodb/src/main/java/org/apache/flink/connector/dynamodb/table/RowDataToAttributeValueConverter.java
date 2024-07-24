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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.types.RowKind;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Converts from Flink Table API internal type of {@link RowData} to {@link AttributeValue}. */
@Internal
public class RowDataToAttributeValueConverter {

    private final DataType physicalDataType;
    private final TableSchema<RowData> tableSchema;
    private final Set<String> primaryKeys;

    public RowDataToAttributeValueConverter(DataType physicalDataType, Set<String> primaryKeys) {
        this.physicalDataType = physicalDataType;
        this.primaryKeys = primaryKeys;
        this.tableSchema = createTableSchema();
    }

    public Map<String, AttributeValue> convertRowData(RowData row) {
        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap = tableSchema.itemToMap(row, false);

        // In case of DELETE, only the primary key field(s) should be sent in the request
        // In order to accomplish this, we need PRIMARY KEY fields to have been set in Table definition.
        if (row.getRowKind() == RowKind.DELETE){
                if (primaryKeys == null || primaryKeys.isEmpty()) {
                        throw new TableException("PRIMARY KEY on Table must be set for DynamoDB DELETE operation");
                }
                Map<String, AttributeValue> pkOnlyMap = new HashMap<String, AttributeValue>();
                for (String key : primaryKeys) {
                        AttributeValue value = itemMap.get(key);
                        pkOnlyMap.put(key, value);
                }
                return pkOnlyMap;
        }
        else {
                return itemMap;
        }

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
