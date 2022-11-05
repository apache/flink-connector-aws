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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A DynamoDB Sink that performs async requests against a destination table using the buffering
 * protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link
 * software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient} to communicate with the AWS
 * endpoint.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be written to
 *       DynamoDb. DynamoDB client supports only up to 25 elements in the batch.
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *       any more in flight requests need to be initiated once the maximum has been reached, then it
 *       will be blocked until some have completed
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *       sink will backpressure while the number of elements in the buffer is at the maximum
 *   <li>{@code maxBatchSizeInBytes}: this setting will not have any effect on DynamoDBSink batch
 *       implementation
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed immediately
 *   <li>{@code maxRecordSizeInBytes}: this setting will not have any effect on DynamoDBSink batch
 *       implementation
 *   <li>{@code failOnError}: when an exception is encountered while persisting to DynamoDb, the job
 *       will fail immediately if failOnError is set
 *   <li>{@code overwriteByPartitionKeys}: list of attribute key names for the sink to deduplicate
 *       on if you want to bypass the no duplication limitation of a single batch write request.
 *       Batching DynamoDB sink will drop request items in the buffer if their primary
 *       keys(composite) values are the same as the newly added ones. The newer request item in a
 *       single batch takes precedence.
 * </ul>
 *
 * <p>Please see the writer implementation in {@link DynamoDbSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class DynamoDbSink<InputT> extends AsyncSinkBase<InputT, DynamoDbWriteRequest> {

    private final Properties dynamoDbClientProperties;
    private final boolean failOnError;
    private final String tableName;
    private final List<String> overwriteByPartitionKeys;

    protected DynamoDbSink(
            ElementConverter<InputT, DynamoDbWriteRequest> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String tableName,
            List<String> overwriteByPartitionKeys,
            Properties dynamoDbClientProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        checkNotNull(
                tableName,
                "Destination table name must be set when initializing the DynamoDB Sink.");
        checkArgument(
                !tableName.isEmpty(),
                "Destination table name must be set when initializing the DynamoDB Sink.");
        checkArgument(
                maxBatchSize <= 25,
                "DynamoDB client supports only up to 25 elements in the batch.");
        checkNotNull(dynamoDbClientProperties, "DynamoDB client properties must be set.");
        this.failOnError = failOnError;
        this.tableName = tableName;
        this.overwriteByPartitionKeys = overwriteByPartitionKeys;
        this.dynamoDbClientProperties = dynamoDbClientProperties;
    }

    /**
     * Create a {@link DynamoDbSinkBuilder} to construct a new {@link DynamoDbSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link DynamoDbSinkBuilder}
     */
    public static <InputT> DynamoDbSinkBuilder<InputT> builder() {
        return new DynamoDbSinkBuilder<>();
    }

    @Internal
    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<DynamoDbWriteRequest>> createWriter(
            InitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Internal
    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<DynamoDbWriteRequest>> restoreWriter(
            InitContext context,
            Collection<BufferedRequestState<DynamoDbWriteRequest>> recoveredState)
            throws IOException {
        return new DynamoDbSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                tableName,
                overwriteByPartitionKeys,
                dynamoDbClientProperties,
                recoveredState);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<BufferedRequestState<DynamoDbWriteRequest>>
            getWriterStateSerializer() {
        return new DynamoDbWriterStateSerializer();
    }
}
