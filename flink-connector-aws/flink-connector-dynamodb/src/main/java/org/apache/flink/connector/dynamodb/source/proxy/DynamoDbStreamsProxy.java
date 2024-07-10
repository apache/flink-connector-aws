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

package org.apache.flink.connector.dynamodb.source.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.ExpiredIteratorException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Implementation of the {@link StreamProxy} for DynamoDB streams. */
@Internal
public class DynamoDbStreamsProxy implements StreamProxy {

    private final DynamoDbStreamsClient dynamoDbStreamsClient;
    private final SdkHttpClient httpClient;
    private final Map<String, String> shardIdToIteratorStore;

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamsProxy.class);

    public DynamoDbStreamsProxy(
            DynamoDbStreamsClient dynamoDbStreamsClient, SdkHttpClient httpClient) {
        this.dynamoDbStreamsClient = dynamoDbStreamsClient;
        this.httpClient = httpClient;
        this.shardIdToIteratorStore = new ConcurrentHashMap<>();
    }

    @Override
    public List<Shard> listShards(String streamArn, @Nullable String lastSeenShardId) {
        return this.getShardsOfStream(streamArn, lastSeenShardId);
    }

    @Override
    public GetRecordsResponse getRecords(
            String streamArn, String shardId, StartingPosition startingPosition) {
        String shardIterator =
                shardIdToIteratorStore.computeIfAbsent(
                        shardId, (s) -> getShardIterator(streamArn, s, startingPosition));

        if (shardIterator == null) {
            return GetRecordsResponse.builder()
                    .records(Collections.emptyList())
                    .nextShardIterator(null)
                    .build();
        }
        try {
            GetRecordsResponse getRecordsResponse = getRecords(shardIterator);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        } catch (ExpiredIteratorException e) {
            // Eagerly retry getRecords() if the iterator is expired
            shardIterator = getShardIterator(streamArn, shardId, startingPosition);
            GetRecordsResponse getRecordsResponse = getRecords(shardIterator);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        }
    }

    @Override
    public void close() throws IOException {
        dynamoDbStreamsClient.close();
        httpClient.close();
    }

    private List<Shard> getShardsOfStream(String streamName, @Nullable String lastSeenShardId) {
        List<Shard> shardsOfStream = new ArrayList<>();

        DescribeStreamResponse describeStreamResponse;
        do {
            describeStreamResponse = this.describeStream(streamName, lastSeenShardId);
            List<Shard> shards = describeStreamResponse.streamDescription().shards();
            shardsOfStream.addAll(shards);

        } while (describeStreamResponse.streamDescription().lastEvaluatedShardId() != null);

        return shardsOfStream;
    }

    private DescribeStreamResponse describeStream(String streamArn, @Nullable String startShardId) {
        final DescribeStreamRequest describeStreamRequest =
                DescribeStreamRequest.builder()
                        .streamArn(streamArn)
                        .exclusiveStartShardId(startShardId)
                        .build();

        DescribeStreamResponse describeStreamResponse =
                dynamoDbStreamsClient.describeStream(describeStreamRequest);

        StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();
        if (!(streamStatus.equals(StreamStatus.DISABLED)
                || streamStatus.equals(StreamStatus.ENABLING.toString()))) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        String.format(
                                "The status of stream %s is %s ; result of the current "
                                        + "describeStream operation will not contain any shard information.",
                                streamArn, streamStatus));
            }
        }

        return describeStreamResponse;
    }

    private String getShardIterator(
            String streamArn, String shardId, StartingPosition startingPosition) {
        GetShardIteratorRequest.Builder requestBuilder =
                GetShardIteratorRequest.builder()
                        .streamArn(streamArn)
                        .shardId(shardId)
                        .shardIteratorType(startingPosition.getShardIteratorType());

        switch (startingPosition.getShardIteratorType()) {
            case TRIM_HORIZON:
            case LATEST:
                break;
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                if (startingPosition.getStartingMarker() instanceof String) {
                    requestBuilder =
                            requestBuilder.sequenceNumber(
                                    (String) startingPosition.getStartingMarker());
                } else {
                    throw new IllegalArgumentException(
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
                }
        }

        try {
            return dynamoDbStreamsClient.getShardIterator(requestBuilder.build()).shardIterator();
        } catch (ResourceNotFoundException e) {
            LOG.info(
                    "Received ResourceNotFoundException. "
                            + "Shard {} of stream {} is no longer valid, marking it as complete.",
                    shardId,
                    streamArn);
            return null;
        }
    }

    private GetRecordsResponse getRecords(String shardIterator) {
        return dynamoDbStreamsClient.getRecords(
                GetRecordsRequest.builder().shardIterator(shardIterator).build());
    }
}
