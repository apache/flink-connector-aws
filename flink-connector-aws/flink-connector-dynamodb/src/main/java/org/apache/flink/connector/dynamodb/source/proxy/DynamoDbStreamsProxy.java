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
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.ListShardsResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.ExpiredIteratorException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ShardFilter;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Implementation of the {@link StreamProxy} for DynamoDB streams. */
@Internal
public class DynamoDbStreamsProxy implements StreamProxy {

    /** Client for the blocking methods. {@code null} if the proxy only makes non-blocking calls. */
    @Nullable private final DynamoDbStreamsClient dynamoDbStreamsClient;

    @Nullable private final SdkHttpClient httpClient;

    /**
     * Client for the non-blocking methods {@link #getRecordsAsync} and {@link
     * #listShardsWithFilterAsync}. {@code null} if the proxy only makes blocking calls.
     */
    @Nullable private final DynamoDbStreamsAsyncClient dynamoDbStreamsAsyncClient;

    @Nullable private final SdkAsyncHttpClient asyncHttpClient;
    private final Map<String, String> shardIdToIteratorStore;

    private static final GetRecordsResponse EMPTY_GET_RECORDS_RESPONSE =
            GetRecordsResponse.builder()
                    .records(Collections.emptyList())
                    .nextShardIterator(null)
                    .build();

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamsProxy.class);

    public DynamoDbStreamsProxy(
            DynamoDbStreamsClient dynamoDbStreamsClient, SdkHttpClient httpClient) {
        this(dynamoDbStreamsClient, httpClient, null, null);
    }

    /**
     * Creates a proxy that can make non-blocking calls. The proxy uses {@code
     * dynamoDbStreamsAsyncClient} for {@link #getRecordsAsync} and {@link
     * #listShardsWithFilterAsync}, and {@code dynamoDbStreamsClient} for all other methods. Either
     * client can be {@code null} if the proxy makes only the other kind of call.
     */
    public DynamoDbStreamsProxy(
            @Nullable DynamoDbStreamsClient dynamoDbStreamsClient,
            @Nullable SdkHttpClient httpClient,
            @Nullable DynamoDbStreamsAsyncClient dynamoDbStreamsAsyncClient,
            @Nullable SdkAsyncHttpClient asyncHttpClient) {
        this.dynamoDbStreamsClient = dynamoDbStreamsClient;
        this.httpClient = httpClient;
        this.dynamoDbStreamsAsyncClient = dynamoDbStreamsAsyncClient;
        this.asyncHttpClient = asyncHttpClient;
        this.shardIdToIteratorStore = new ConcurrentHashMap<>();
    }

    @Override
    public ListShardsResult listShards(String streamArn, @Nullable String lastSeenShardId) {
        ListShardsResult listShardsResult = new ListShardsResult();

        String lastEvaluatedShardId = lastSeenShardId;
        DescribeStreamResponse describeStreamResponse;
        do {
            describeStreamResponse = this.describeStream(streamArn, lastEvaluatedShardId);
            listShardsResult.addShards(describeStreamResponse.streamDescription().shards());
            listShardsResult.setStreamStatus(
                    describeStreamResponse.streamDescription().streamStatus());
            lastEvaluatedShardId =
                    describeStreamResponse.streamDescription().lastEvaluatedShardId();
            LOG.debug(
                    "DescribeStream lastEvaluatedShardId: {}, returned shards: {}",
                    lastEvaluatedShardId,
                    describeStreamResponse.streamDescription().shards());
        } while (describeStreamResponse.streamDescription().lastEvaluatedShardId() != null);

        return listShardsResult;
    }

    @Override
    public ListShardsResult listShardsWithFilter(String streamArn, ShardFilter shardFilter) {
        LOG.debug("Child shards with filter called, for shardId: {}", shardFilter.shardId());
        ListShardsResult listShardsResult = new ListShardsResult();

        try {
            DescribeStreamResponse describeStreamResponse =
                    this.describeStream(streamArn, shardFilter);
            listShardsResult.addShards(describeStreamResponse.streamDescription().shards());
            listShardsResult.setStreamStatus(
                    describeStreamResponse.streamDescription().streamStatus());
        } catch (Exception e) {
            LOG.warn("DescribeStream with Filter API threw an exception", e);
        }
        LOG.info("Child shards returned for shardId: {}", listShardsResult);
        return listShardsResult;
    }

    @Override
    public GetRecordsResponse getRecords(
            String streamArn, String shardId, StartingPosition startingPosition) {
        String shardIterator =
                shardIdToIteratorStore.computeIfAbsent(
                        shardId, (s) -> getShardIterator(streamArn, s, startingPosition));

        if (shardIterator == null) {
            return EMPTY_GET_RECORDS_RESPONSE;
        }
        try {
            return storeNextShardIterator(shardId, getRecords(shardIterator));
        } catch (ExpiredIteratorException e) {
            LOG.info(
                    "Received ExpiredIteratorException from GetRecords. "
                            + "Calling GetShardIterator for shard: {} with position: {}",
                    startingPosition,
                    shardId);
            // Eagerly retry getRecords() if the iterator is expired
            shardIterator = getShardIterator(streamArn, shardId, startingPosition);
            return storeNextShardIterator(shardId, getRecords(shardIterator));
        } catch (TrimmedDataAccessException e) {
            // TrimmedDataAccessException means that the record pointed by shard iterator has
            // expired.
            // We should read the shard back from TRIM_HORIZON
            LOG.warn(
                    "Received TrimmedDataAccessException from GetRecords. "
                            + "Calling GetShardIterator for shard: {} with TRIM_HORIZON",
                    shardId);
            shardIterator = getShardIterator(streamArn, shardId, StartingPosition.fromStart());
            return storeNextShardIterator(shardId, getRecords(shardIterator));
        } catch (ResourceNotFoundException e) {
            LOG.warn(
                    "Received ResourceNotFoundException from GetRecords for shard: {}. "
                            + "This might indicate that there is restore happening from stale snapshot or data loss from backpressure",
                    shardId);
            return EMPTY_GET_RECORDS_RESPONSE;
        }
    }

    /**
     * Non-blocking version of {@link #getRecords(String, String, StartingPosition)}. Each step
     * matches a step of the blocking method: the shard iterator store, the retry after
     * ExpiredIteratorException, the read from TRIM_HORIZON after TrimmedDataAccessException, and
     * the empty response after ResourceNotFoundException.
     */
    @Override
    public CompletableFuture<GetRecordsResponse> getRecordsAsync(
            String streamArn, String shardId, StartingPosition startingPosition) {
        DynamoDbStreamsAsyncClient asyncClient = getAsyncClient();

        String storedShardIterator = shardIdToIteratorStore.get(shardId);
        CompletableFuture<String> shardIteratorFuture =
                storedShardIterator != null
                        ? CompletableFuture.completedFuture(storedShardIterator)
                        : getShardIteratorAsync(asyncClient, streamArn, shardId, startingPosition)
                                .thenApply(
                                        shardIterator -> {
                                            if (shardIterator != null) {
                                                shardIdToIteratorStore.put(shardId, shardIterator);
                                            }
                                            return shardIterator;
                                        });

        return shardIteratorFuture.thenCompose(
                shardIterator -> {
                    if (shardIterator == null) {
                        return CompletableFuture.completedFuture(EMPTY_GET_RECORDS_RESPONSE);
                    }
                    return getRecordsAsync(asyncClient, shardIterator)
                            .thenApply(response -> storeNextShardIterator(shardId, response))
                            // Same as exceptionallyCompose, which needs Java 12.
                            .handle(
                                    (response, error) ->
                                            error == null
                                                    ? CompletableFuture.completedFuture(response)
                                                    : recoverGetRecordsAsync(
                                                            asyncClient,
                                                            streamArn,
                                                            shardId,
                                                            startingPosition,
                                                            ExceptionUtils.stripCompletionException(
                                                                    error)))
                            .thenCompose(Function.identity());
                });
    }

    private CompletableFuture<GetRecordsResponse> recoverGetRecordsAsync(
            DynamoDbStreamsAsyncClient asyncClient,
            String streamArn,
            String shardId,
            StartingPosition startingPosition,
            Throwable error) {
        if (ExceptionUtils.findThrowable(error, ExpiredIteratorException.class).isPresent()) {
            LOG.info(
                    "Received ExpiredIteratorException from GetRecords. "
                            + "Calling GetShardIterator for shard: {} with position: {}",
                    shardId,
                    startingPosition);
            // Eagerly retry getRecords() if the iterator is expired
            return getShardIteratorAsync(asyncClient, streamArn, shardId, startingPosition)
                    .thenCompose(shardIterator -> getRecordsAsync(asyncClient, shardIterator))
                    .thenApply(response -> storeNextShardIterator(shardId, response));
        }
        if (ExceptionUtils.findThrowable(error, TrimmedDataAccessException.class).isPresent()) {
            // TrimmedDataAccessException means that the record pointed by shard iterator has
            // expired. We should read the shard back from TRIM_HORIZON
            LOG.warn(
                    "Received TrimmedDataAccessException from GetRecords. "
                            + "Calling GetShardIterator for shard: {} with TRIM_HORIZON",
                    shardId);
            return getShardIteratorAsync(
                            asyncClient, streamArn, shardId, StartingPosition.fromStart())
                    .thenCompose(shardIterator -> getRecordsAsync(asyncClient, shardIterator))
                    .thenApply(response -> storeNextShardIterator(shardId, response));
        }
        if (ExceptionUtils.findThrowable(error, ResourceNotFoundException.class).isPresent()) {
            LOG.warn(
                    "Received ResourceNotFoundException from GetRecords for shard: {}. "
                            + "This might indicate that there is restore happening from stale snapshot or data loss from backpressure",
                    shardId);
            return CompletableFuture.completedFuture(EMPTY_GET_RECORDS_RESPONSE);
        }
        return CompletableFuture.failedFuture(error);
    }

    /** Non-blocking version of {@link #listShardsWithFilter(String, ShardFilter)}. */
    @Override
    public CompletableFuture<ListShardsResult> listShardsWithFilterAsync(
            String streamArn, ShardFilter shardFilter) {
        DynamoDbStreamsAsyncClient asyncClient = getAsyncClient();
        LOG.debug("Child shards with filter called, for shardId: {}", shardFilter.shardId());
        return asyncClient
                .describeStream(describeStreamRequest(streamArn, shardFilter))
                .thenApply(
                        describeStreamResponse -> {
                            logIfStreamIsEnabling(streamArn, describeStreamResponse);
                            ListShardsResult listShardsResult = new ListShardsResult();
                            listShardsResult.addShards(
                                    describeStreamResponse.streamDescription().shards());
                            listShardsResult.setStreamStatus(
                                    describeStreamResponse.streamDescription().streamStatus());
                            return listShardsResult;
                        })
                .exceptionally(
                        error -> {
                            LOG.warn(
                                    "DescribeStream with Filter API threw an exception",
                                    ExceptionUtils.stripCompletionException(error));
                            return new ListShardsResult();
                        })
                .thenApply(
                        listShardsResult -> {
                            LOG.info("Child shards returned for shardId: {}", listShardsResult);
                            return listShardsResult;
                        });
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(
                dynamoDbStreamsClient, httpClient, dynamoDbStreamsAsyncClient, asyncHttpClient);
    }

    private DescribeStreamResponse describeStream(String streamArn, @Nullable String startShardId) {
        DescribeStreamResponse describeStreamResponse =
                getSyncClient()
                        .describeStream(
                                DescribeStreamRequest.builder()
                                        .streamArn(streamArn)
                                        .exclusiveStartShardId(startShardId)
                                        .build());
        logIfStreamIsEnabling(streamArn, describeStreamResponse);
        return describeStreamResponse;
    }

    private DescribeStreamResponse describeStream(String streamArn, ShardFilter shardFilter) {
        DescribeStreamResponse describeStreamResponse =
                getSyncClient().describeStream(describeStreamRequest(streamArn, shardFilter));
        logIfStreamIsEnabling(streamArn, describeStreamResponse);
        return describeStreamResponse;
    }

    private static DescribeStreamRequest describeStreamRequest(
            String streamArn, ShardFilter shardFilter) {
        return DescribeStreamRequest.builder()
                .streamArn(streamArn)
                .shardFilter(shardFilter)
                .build();
    }

    private String getShardIterator(
            String streamArn, String shardId, StartingPosition startingPosition) {
        GetShardIteratorRequest getShardIteratorRequest =
                buildGetShardIteratorRequest(streamArn, shardId, startingPosition);
        try {
            return getSyncClient().getShardIterator(getShardIteratorRequest).shardIterator();
        } catch (ResourceNotFoundException e) {
            logGetShardIteratorResourceNotFound(streamArn, shardId);
            return null;
        } catch (TrimmedDataAccessException e) {
            logGetShardIteratorTrimmedDataAccess(streamArn, shardId);
            return null;
        }
    }

    /**
     * Non-blocking version of {@link #getShardIterator(String, String, StartingPosition)}. The
     * future completes with {@code null} where the blocking method returns {@code null}.
     */
    private CompletableFuture<String> getShardIteratorAsync(
            DynamoDbStreamsAsyncClient asyncClient,
            String streamArn,
            String shardId,
            StartingPosition startingPosition) {
        GetShardIteratorRequest getShardIteratorRequest =
                buildGetShardIteratorRequest(streamArn, shardId, startingPosition);
        return asyncClient
                .getShardIterator(getShardIteratorRequest)
                .thenApply(GetShardIteratorResponse::shardIterator)
                .exceptionally(
                        error -> {
                            Throwable cause = ExceptionUtils.stripCompletionException(error);
                            if (ExceptionUtils.findThrowable(cause, ResourceNotFoundException.class)
                                    .isPresent()) {
                                logGetShardIteratorResourceNotFound(streamArn, shardId);
                                return null;
                            }
                            if (ExceptionUtils.findThrowable(
                                            cause, TrimmedDataAccessException.class)
                                    .isPresent()) {
                                logGetShardIteratorTrimmedDataAccess(streamArn, shardId);
                                return null;
                            }
                            throw new CompletionException(cause);
                        });
    }

    private static GetShardIteratorRequest buildGetShardIteratorRequest(
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
            case AT_TIMESTAMP:
                if (startingPosition.getStartingMarker() instanceof Instant) {
                    requestBuilder =
                            requestBuilder.timestamp(
                                    (Instant) startingPosition.getStartingMarker());
                } else {
                    throw new IllegalArgumentException(
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_TIMESTAMP. Must be an Instant.");
                }
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
        return requestBuilder.build();
    }

    private static void logGetShardIteratorResourceNotFound(String streamArn, String shardId) {
        LOG.warn(
                "Received ResourceNotFoundException from GetShardIterator. "
                        + "Shard {} of stream {} is no longer valid, marking it as complete."
                        + "This might indicate that there is restore happening from stale snapshot or data loss from backpressure",
                shardId,
                streamArn);
    }

    private static void logGetShardIteratorTrimmedDataAccess(String streamArn, String shardId) {
        LOG.warn(
                "Received TrimmedDataAccessException from GetShardIterator. "
                        + "Shard {} of stream {} is no longer valid, marking it as complete."
                        + "This might indicate that there is restore happening from stale snapshot or data loss from backpressure",
                shardId,
                streamArn);
    }

    private GetRecordsResponse getRecords(String shardIterator) {
        if (Objects.isNull(shardIterator)) {
            return EMPTY_GET_RECORDS_RESPONSE;
        }
        return getSyncClient()
                .getRecords(GetRecordsRequest.builder().shardIterator(shardIterator).build());
    }

    private CompletableFuture<GetRecordsResponse> getRecordsAsync(
            DynamoDbStreamsAsyncClient asyncClient, @Nullable String shardIterator) {
        if (Objects.isNull(shardIterator)) {
            return CompletableFuture.completedFuture(EMPTY_GET_RECORDS_RESPONSE);
        }
        return asyncClient.getRecords(
                GetRecordsRequest.builder().shardIterator(shardIterator).build());
    }

    /**
     * Stores the next shard iterator of the shard, or removes the stored iterator at the end of the
     * shard, so that the store does not keep an entry for every shard the proxy has read.
     */
    private GetRecordsResponse storeNextShardIterator(
            String shardId, GetRecordsResponse getRecordsResponse) {
        if (getRecordsResponse.nextShardIterator() != null) {
            shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
        } else {
            shardIdToIteratorStore.remove(shardId);
        }
        return getRecordsResponse;
    }

    private DynamoDbStreamsClient getSyncClient() {
        if (dynamoDbStreamsClient == null) {
            throw new IllegalStateException(
                    "DynamoDbStreamsProxy has no DynamoDbStreamsClient. "
                            + "Blocking calls are not available.");
        }
        return dynamoDbStreamsClient;
    }

    private DynamoDbStreamsAsyncClient getAsyncClient() {
        if (dynamoDbStreamsAsyncClient == null) {
            throw new IllegalStateException(
                    "DynamoDbStreamsProxy has no DynamoDbStreamsAsyncClient. "
                            + "Non-blocking calls are not available.");
        }
        return dynamoDbStreamsAsyncClient;
    }

    private static void logIfStreamIsEnabling(
            String streamArn, DescribeStreamResponse describeStreamResponse) {
        StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();
        if (StreamStatus.ENABLING.equals(streamStatus) && LOG.isWarnEnabled()) {
            LOG.warn(
                    String.format(
                            "The status of stream %s is %s ; result of the current "
                                    + "describeStream operation will not contain any shard information.",
                            streamArn, streamStatus));
        }
    }
}
