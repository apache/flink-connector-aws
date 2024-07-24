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
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSinkBuilder;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link DynamoDbSink} from a logical
 * description.
 */
@Internal
public class DynamoDbDynamicSink extends AsyncDynamicTableSink<DynamoDbWriteRequest>
        implements SupportsPartitioning {

    private final String tableName;
    private final boolean failOnError;
    private final Properties dynamoDbClientProperties;
    private final DataType physicalDataType;
    private final Set<String> overwriteByPartitionKeys;
    private final Set<String> primaryKeys;

    protected DynamoDbDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            String tableName,
            boolean failOnError,
            Properties dynamoDbClientProperties,
            DataType physicalDataType,
            Set<String> overwriteByPartitionKeys,
            Set<String> primaryKeys) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        this.tableName = tableName;
        this.failOnError = failOnError;
        this.dynamoDbClientProperties = dynamoDbClientProperties;
        this.physicalDataType = physicalDataType;
        this.overwriteByPartitionKeys = overwriteByPartitionKeys;
        this.primaryKeys = primaryKeys;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DynamoDbSinkBuilder<RowData> builder =
                DynamoDbSink.<RowData>builder()
                        .setTableName(tableName)
                        .setFailOnError(failOnError)
                        .setOverwriteByPartitionKeys(new ArrayList<>(overwriteByPartitionKeys))
                        .setDynamoDbProperties(dynamoDbClientProperties)
                        .setElementConverter(new RowDataElementConverter(physicalDataType, primaryKeys));

        addAsyncOptionsToSinkBuilder(builder);

        return SinkV2Provider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new DynamoDbDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                tableName,
                failOnError,
                dynamoDbClientProperties,
                physicalDataType,
                overwriteByPartitionKeys,
                primaryKeys);
    }

    @Override
    public String asSummaryString() {
        return "DynamoDB";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partitions) {
        // We don't need to do anything here because the DynamoDB sink handles a static partition
        // just like a normal partition.
    }

    public static DynamoDbDynamicTableSinkBuilder builder() {
        return new DynamoDbDynamicTableSinkBuilder();
    }

    /** Builder class for {@link DynamoDbDynamicSink}. */
    @Internal
    public static class DynamoDbDynamicTableSinkBuilder
            extends AsyncDynamicTableSinkBuilder<
                    DynamoDbWriteRequest, DynamoDbDynamicTableSinkBuilder> {
        private String tableName;
        private boolean failOnError;
        private Properties dynamoDbClientProperties;
        private DataType physicalDataType;
        private Set<String> overwriteByPartitionKeys;
        private Set<String> primaryKeys;

        public DynamoDbDynamicTableSinkBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DynamoDbDynamicTableSinkBuilder setFailOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public DynamoDbDynamicTableSinkBuilder setDynamoDbClientProperties(
                Properties dynamoDbClientProperties) {
            this.dynamoDbClientProperties = dynamoDbClientProperties;
            return this;
        }

        public DynamoDbDynamicTableSinkBuilder setPhysicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        public DynamoDbDynamicTableSinkBuilder setOverwriteByPartitionKeys(
                Set<String> overwriteByPartitionKeys) {
            this.overwriteByPartitionKeys = overwriteByPartitionKeys;
            return this;
        }

        public DynamoDbDynamicTableSinkBuilder setPrimaryKeys(Set<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        @Override
        public AsyncDynamicTableSink<DynamoDbWriteRequest> build() {
            return new DynamoDbDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    tableName,
                    failOnError,
                    dynamoDbClientProperties,
                    physicalDataType,
                    overwriteByPartitionKeys,
                    primaryKeys);
        }
    }
}
