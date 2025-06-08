/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.connector.cloudwatch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.cloudwatch.sink.MetricWriteRequest;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import java.time.Instant;
import java.util.List;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Converts from Flink Table API internal type of {@link RowData} to {@link MetricWriteRequest}. */
@Internal
public class RowDataToMetricWriteRequestConverter {

    private final DataType physicalDataType;
    private final CloudWatchTableConfig tableConfig;

    public RowDataToMetricWriteRequestConverter(
            DataType physicalDataType, CloudWatchTableConfig tableConfig) {
        this.physicalDataType = physicalDataType;
        this.tableConfig = tableConfig;
    }

    public MetricWriteRequest convertRowData(RowData row) {
        List<DataTypes.Field> fields = DataType.getFields(physicalDataType);

        MetricWriteRequest.Builder builder = MetricWriteRequest.builder();

        for (int i = 0; i < fields.size(); i++) {
            DataTypes.Field field = fields.get(i);
            RowData.FieldGetter fieldGetter =
                    createFieldGetter(fields.get(i).getDataType().getLogicalType(), i);
            FieldValue fieldValue = new FieldValue(fieldGetter.getFieldOrNull(row));
            String fieldName = field.getName();

            if (fieldName.equals(tableConfig.getMetricName())) {
                builder.withMetricName(fieldValue.getStringValue());
            } else if (fieldName.equals(tableConfig.getMetricValueKey())) {
                builder.addValue(fieldValue.getDoubleValue());
            } else if (tableConfig.getMetricDimensionKeys().contains(fieldName)) {
                builder.addDimension(fieldName, fieldValue.getStringValue());
            } else if (fieldName.equals(tableConfig.getMetricCountKey())) {
                builder.addCount(fieldValue.getDoubleValue());
            } else if (fieldName.equals(tableConfig.getMetricUnitKey())) {
                builder.withUnit(fieldValue.getStringValue());
            } else if (fieldName.equals(tableConfig.getMetricTimestampKey())) {
                builder.withTimestamp(Instant.ofEpochMilli(fieldValue.getLongValue()));
            } else if (fieldName.equals(tableConfig.getMetricStorageResolutionKey())) {
                builder.withStorageResolution(fieldValue.getIntegerValue());
            } else if (fieldName.equals(tableConfig.getMetricStatisticMaxKey())) {
                builder.withStatisticMax(fieldValue.getDoubleValue());
            } else if (fieldName.equals(tableConfig.getMetricStatisticMinKey())) {
                builder.withStatisticMin(fieldValue.getDoubleValue());
            } else if (fieldName.equals(tableConfig.getMetricStatisticSumKey())) {
                builder.withStatisticSum(fieldValue.getDoubleValue());
            } else if (fieldName.equals(tableConfig.getMetricStatisticSampleCountKey())) {
                builder.withStatisticCount(fieldValue.getDoubleValue());
            } else {
                throw new IllegalArgumentException("Unsupported field: " + fieldName);
            }
        }

        return builder.build();
    }

    private static class FieldValue {
        private final Object value;

        private FieldValue(Object value) {
            this.value = value;
        }

        private String getStringValue() {
            return value.toString();
        }

        private Double getDoubleValue() {
            return Double.valueOf(value.toString());
        }

        private Integer getIntegerValue() {
            return Integer.valueOf(value.toString());
        }

        private Long getLongValue() {
            if (value instanceof TimestampData) {
                return ((TimestampData) value).getMillisecond();
            }
            return Long.valueOf(value.toString());
        }
    }
}
