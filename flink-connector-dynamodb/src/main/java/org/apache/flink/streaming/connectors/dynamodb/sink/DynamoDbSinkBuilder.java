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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Builder to construct {@link DynamoDbSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link DynamoDbSink} that writes
 * records into DynamoDb
 *
 * <pre>{@code
 * private static class DummyDynamoDbElementConverter
 *        implements ElementConverter<String, DynamoDbWriteRequest> {
 *
 *    @Override
 *    public DynamoDbWriteRequest apply(String s, SinkWriter.Context context) {
 *        final Map<String, AttributeValue> item = new HashMap<>();
 *        item.put("your-key", AttributeValue.builder().s(s).build());
 *        return DynamoDbWriteRequest.builder()
 *                .setItem(item)
 *                .setType(DynamoDbWriteRequestType.PUT)
 *                .build();
 *    }
 * }
 *
 *  DynamoDbSink<String> dynamoDbSink =
 *            DynamoDbSink.<String>builder()
 *                    .setElementConverter(new DummyDynamoDbElementConverter())
 *                    .setDestinationTableName("your-table-name")
 *                    .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 25
 *   <li>{@code maxInFlightRequests} will be 50
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code maxBatchSizeInBytes} setting is not supported by this implementation
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} setting is not supported by this implementation
 *   <li>{@code failOnError} will be false
 *   <li>{@code destinationTableName} destination table for the sink
 *   <li>{@code overwriteByPartitionKeys} will be empty meaning no records deduplication will be
 *       performed by the batch sink
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class DynamoDbSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, DynamoDbWriteRequest, DynamoDbSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 25;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10000;

    // Max record size in bytes and max batch size in bytes are not supported by this implementation
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 400 * 1000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 16 * 1000 * 1000;

    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;

    private boolean failOnError;
    private Properties dynamodbClientProperties;

    private ElementConverter<InputT, DynamoDbWriteRequest> elementConverter;
    private String tableName;

    private List<String> overwriteByPartitionKeys;

    public DynamoDbSinkBuilder<InputT> setDynamoDbProperties(Properties properties) {
        this.dynamodbClientProperties = properties;
        return this;
    }

    public DynamoDbSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, DynamoDbWriteRequest> elementConverter) {
        this.elementConverter = elementConverter;
        return this;
    }

    /** Destination table name for the DynamoDB sink. */
    public DynamoDbSinkBuilder<InputT> setDestinationTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * @param overwriteByPartitionKeys list of attribute key names for the sink to deduplicate on if
     *     you want to bypass the no duplication limitation of a single batch write request.
     *     Batching DynamoDB sink will drop request items in the buffer if their primary
     *     keys(composite) values are the same as the newly added ones. The newer request item in a
     *     single batch takes precedence.
     */
    public DynamoDbSinkBuilder<InputT> setOverwriteByPartitionKeys(
            List<String> overwriteByPartitionKeys) {
        this.overwriteByPartitionKeys = overwriteByPartitionKeys;
        return this;
    }

    public DynamoDbSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    @Override
    public DynamoDbSinkBuilder<InputT> setMaxBatchSizeInBytes(long maxBatchSizeInBytes) {
        throw new InvalidConfigurationException(
                "Max batch size in bytes is not supported by the DynamoDB sink implementation.");
    }

    @Override
    public DynamoDbSinkBuilder<InputT> setMaxRecordSizeInBytes(long maxRecordSizeInBytes) {
        throw new InvalidConfigurationException(
                "Max record size in bytes is not supported by the DynamoDB sink implementation.");
    }

    @Override
    public DynamoDbSink<InputT> build() {
        return new DynamoDbSink<>(
                elementConverter,
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                Optional.of(failOnError).orElse(DEFAULT_FAIL_ON_ERROR),
                tableName,
                Optional.ofNullable(overwriteByPartitionKeys).orElse(new ArrayList<>()),
                Optional.ofNullable(dynamodbClientProperties).orElse(new Properties()));
    }
}
