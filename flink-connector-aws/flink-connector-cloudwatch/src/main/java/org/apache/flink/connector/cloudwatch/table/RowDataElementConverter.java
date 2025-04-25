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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.cloudwatch.sink.MetricWriteRequest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * Converts the sink input {@link RowData} into the Protobuf {@link MetricWriteRequest} that are
 * sent to CloudWatch.
 */
@PublicEvolving
public class RowDataElementConverter implements ElementConverter<RowData, MetricWriteRequest> {
    private final DataType physicalDataType;
    private final CloudWatchTableConfig cloudWatchTableConfig;
    private transient RowDataToMetricWriteRequestConverter rowDataToCloudWatchMetricInputConverter;

    public RowDataElementConverter(
            DataType physicalDataType, CloudWatchTableConfig cloudWatchTableConfig) {
        this.physicalDataType = physicalDataType;
        this.cloudWatchTableConfig = cloudWatchTableConfig;
        this.rowDataToCloudWatchMetricInputConverter =
                new RowDataToMetricWriteRequestConverter(physicalDataType, cloudWatchTableConfig);
    }

    public MetricWriteRequest apply(RowData element, SinkWriter.Context context) {
        if (rowDataToCloudWatchMetricInputConverter == null) {
            rowDataToCloudWatchMetricInputConverter =
                    new RowDataToMetricWriteRequestConverter(
                            physicalDataType, cloudWatchTableConfig);
        }

        return rowDataToCloudWatchMetricInputConverter.convertRowData(element);
    }
}
