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

import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsAsyncClientProvider.TestingDynamoDbStreamsAsyncClient;
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsClientProvider.TestingDynamoDbStreamsClient;
import org.apache.flink.connector.dynamodb.source.util.ListShardsResult;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.ExpiredIteratorException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardFilter;
import software.amazon.awssdk.services.dynamodb.model.ShardFilterType;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

/** Tests the non-blocking methods of {@link DynamoDbStreamsProxy}. */
class DynamoDbStreamsProxyAsyncTest {
    private static final SdkHttpClient HTTP_CLIENT = ApacheHttpClient.builder().build();
    private static final String STREAM_ARN =
            "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
    private static final String SHARD_ID = "shardId-000000000002";
    private static final GetRecordsResponse EMPTY_GET_RECORDS_RESPONSE =
            GetRecordsResponse.builder()
                    .records(Collections.emptyList())
                    .nextShardIterator(null)
                    .build();

    private TestingDynamoDbStreamsClient blockingClient;
    private TestingDynamoDbStreamsAsyncClient asyncClient;

    @BeforeEach
    void init() {
        blockingClient = new TestingDynamoDbStreamsClient();
        asyncClient = new TestingDynamoDbStreamsAsyncClient(blockingClient);
    }

    @Test
    void testGetRecordsAsyncInitialReadFromTrimHorizon() {
        GetRecordsResponse expectedResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();
        blockingClient.setNextShardIterator("some-shard-iterator");
        blockingClient.setShardIteratorValidation(
                request ->
                        assertThat(request)
                                .isEqualTo(
                                        GetShardIteratorRequest.builder()
                                                .streamArn(STREAM_ARN)
                                                .shardId(SHARD_ID)
                                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                                .build()));
        blockingClient.setGetRecordsResponse(expectedResponse);
        blockingClient.setGetRecordsValidation(
                request ->
                        assertThat(request)
                                .isEqualTo(
                                        GetRecordsRequest.builder()
                                                .shardIterator("some-shard-iterator")
                                                .build()));

        assertThat(getRecords(createProxy(), StartingPosition.fromStart()))
                .isEqualTo(expectedResponse);
    }

    @Test
    void testGetRecordsAsyncUsesStoredShardIterator() {
        AtomicInteger getShardIteratorCalls = new AtomicInteger();
        List<String> usedShardIterators = new ArrayList<>();
        blockingClient.setNextShardIterator("first-shard-iterator");
        blockingClient.setShardIteratorValidation(
                request -> getShardIteratorCalls.incrementAndGet());
        blockingClient.setGetRecordsResponse(
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-shard-iterator")
                        .build());
        blockingClient.setGetRecordsValidation(
                request -> usedShardIterators.add(request.shardIterator()));
        DynamoDbStreamsProxy proxy = createProxy();

        getRecords(proxy, StartingPosition.fromStart());
        getRecords(proxy, StartingPosition.fromStart());

        assertThat(getShardIteratorCalls.get()).isEqualTo(1);
        assertThat(usedShardIterators)
                .containsExactly("first-shard-iterator", "next-shard-iterator");
    }

    @Test
    void testGetRecordsAsyncWhenNextShardIteratorIsNull() {
        blockingClient.setNextShardIterator("some-shard-iterator");
        blockingClient.setShardIteratorValidation(
                request -> {
                    assertThat(request.shardIteratorType())
                            .isEqualTo(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
                    assertThat(request.sequenceNumber()).isEqualTo("123456002332");
                });
        blockingClient.setGetRecordsResponse(EMPTY_GET_RECORDS_RESPONSE);
        blockingClient.setGetRecordsValidation(request -> {});

        assertThat(
                        getRecords(
                                createProxy(),
                                StartingPosition.continueFromSequenceNumber("123456002332")))
                .isEqualTo(EMPTY_GET_RECORDS_RESPONSE);
    }

    @Test
    void testGetRecordsAsyncEagerlyRetriesExpiredIterators() {
        GetRecordsResponse expectedResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();
        AtomicBoolean firstGetRecordsCall = new AtomicBoolean(true);
        List<ShardIteratorType> shardIteratorTypes = new ArrayList<>();
        blockingClient.setNextShardIterator("first-shard-iterator");
        blockingClient.setNextShardIterator("second-shard-iterator");
        blockingClient.setShardIteratorValidation(
                request -> shardIteratorTypes.add(request.shardIteratorType()));
        blockingClient.setGetRecordsResponse(expectedResponse);
        blockingClient.setGetRecordsValidation(
                request -> {
                    if (firstGetRecordsCall.getAndSet(false)) {
                        throw ExpiredIteratorException.builder().build();
                    }
                    assertThat(request.shardIterator()).isEqualTo("second-shard-iterator");
                });

        assertThat(getRecords(createProxy(), StartingPosition.latest()))
                .isEqualTo(expectedResponse);
        assertThat(shardIteratorTypes)
                .containsExactly(ShardIteratorType.LATEST, ShardIteratorType.LATEST);
    }

    @Test
    void testGetRecordsAsyncReadsFromTrimHorizonAfterTrimmedData() {
        GetRecordsResponse expectedResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();
        AtomicBoolean firstGetRecordsCall = new AtomicBoolean(true);
        List<ShardIteratorType> shardIteratorTypes = new ArrayList<>();
        blockingClient.setNextShardIterator("first-shard-iterator");
        blockingClient.setNextShardIterator("second-shard-iterator");
        blockingClient.setShardIteratorValidation(
                request -> shardIteratorTypes.add(request.shardIteratorType()));
        blockingClient.setGetRecordsResponse(expectedResponse);
        blockingClient.setGetRecordsValidation(
                request -> {
                    if (firstGetRecordsCall.getAndSet(false)) {
                        throw TrimmedDataAccessException.builder().build();
                    }
                });

        assertThat(getRecords(createProxy(), StartingPosition.latest()))
                .isEqualTo(expectedResponse);
        assertThat(shardIteratorTypes)
                .containsExactly(ShardIteratorType.LATEST, ShardIteratorType.TRIM_HORIZON);
    }

    @Test
    void testGetRecordsAsyncResourceNotFoundReturnsEmptyResponse() {
        blockingClient.setNextShardIterator("some-shard-iterator");
        blockingClient.setShardIteratorValidation(ignored -> {});
        blockingClient.setGetRecordsValidation(
                request -> {
                    throw ResourceNotFoundException.builder().build();
                });

        assertThat(getRecords(createProxy(), StartingPosition.fromStart()))
                .isEqualTo(EMPTY_GET_RECORDS_RESPONSE);
    }

    @Test
    void testGetShardIteratorAsyncResourceNotFoundCompletesShard() {
        AtomicBoolean getRecordsCalled = new AtomicBoolean(false);
        blockingClient.setShardIteratorValidation(
                request -> {
                    throw ResourceNotFoundException.builder().build();
                });
        blockingClient.setGetRecordsValidation(request -> getRecordsCalled.set(true));

        assertThat(getRecords(createProxy(), StartingPosition.fromStart()))
                .isEqualTo(EMPTY_GET_RECORDS_RESPONSE);
        assertThat(getRecordsCalled.get()).isFalse();
    }

    @Test
    void testGetShardIteratorAsyncTrimmedDataCompletesShard() {
        AtomicBoolean getRecordsCalled = new AtomicBoolean(false);
        blockingClient.setShardIteratorValidation(
                request -> {
                    throw TrimmedDataAccessException.builder().build();
                });
        blockingClient.setGetRecordsValidation(request -> getRecordsCalled.set(true));

        assertThat(getRecords(createProxy(), StartingPosition.fromStart()))
                .isEqualTo(EMPTY_GET_RECORDS_RESPONSE);
        assertThat(getRecordsCalled.get()).isFalse();
    }

    @Test
    void testGetRecordsAsyncFailsOnOtherErrors() {
        blockingClient.setNextShardIterator("some-shard-iterator");
        blockingClient.setShardIteratorValidation(ignored -> {});
        blockingClient.setGetRecordsValidation(
                request -> {
                    throw new IllegalArgumentException("unexpected error");
                });

        assertThatThrownBy(
                        () ->
                                createProxy()
                                        .getRecordsAsync(
                                                STREAM_ARN, SHARD_ID, StartingPosition.fromStart())
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testListShardsWithFilterAsyncForChildShards() {
        String parentShardId = "shardId-000000000001";
        List<Shard> childShards =
                Arrays.asList(
                        Shard.builder()
                                .shardId("shardId-000000000002")
                                .parentShardId(parentShardId)
                                .build(),
                        Shard.builder()
                                .shardId("shardId-000000000003")
                                .parentShardId(parentShardId)
                                .build());
        List<Shard> allShards = new ArrayList<>(childShards);
        allShards.add(Shard.builder().shardId("shardId-000000000004").build());
        blockingClient.setDescribeStreamResponse(
                DescribeStreamResponse.builder()
                        .streamDescription(
                                StreamDescription.builder()
                                        .shards(allShards)
                                        .streamStatus(StreamStatus.ENABLED)
                                        .build())
                        .build());
        blockingClient.setDescribeStreamValidation(
                request -> {
                    assertThat(request.streamArn()).isEqualTo(STREAM_ARN);
                    assertThat(request.shardFilter().type())
                            .isEqualTo(ShardFilterType.CHILD_SHARDS);
                    assertThat(request.shardFilter().shardId()).isEqualTo(parentShardId);
                });

        ListShardsResult result =
                createProxy()
                        .listShardsWithFilterAsync(STREAM_ARN, childShardFilter(parentShardId))
                        .join();

        assertThat(result.getShards()).containsExactlyInAnyOrderElementsOf(childShards);
        assertThat(result.getStreamStatus()).isEqualTo(StreamStatus.ENABLED);
    }

    @Test
    void testListShardsWithFilterAsyncReturnsEmptyResultOnError() {
        blockingClient.setDescribeStreamValidation(
                request -> {
                    throw new IllegalStateException("DescribeStream failed");
                });

        ListShardsResult result =
                createProxy()
                        .listShardsWithFilterAsync(STREAM_ARN, childShardFilter(SHARD_ID))
                        .join();

        // Same result as the blocking method: no shards and the default status, ENABLED. The
        // reader therefore tries child shard discovery again.
        assertThat(result).isEqualTo(new ListShardsResult());
    }

    @Test
    void testNonBlockingMethodsNeedAsyncClient() {
        DynamoDbStreamsProxy proxy = new DynamoDbStreamsProxy(blockingClient, HTTP_CLIENT);

        assertThatThrownBy(
                        () ->
                                proxy.getRecordsAsync(
                                        STREAM_ARN, SHARD_ID, StartingPosition.fromStart()))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                proxy.listShardsWithFilterAsync(
                                        STREAM_ARN, childShardFilter(SHARD_ID)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCloseClosesAsyncClient() {
        DynamoDbStreamsProxy proxy = createProxy();

        assertThatNoException().isThrownBy(proxy::close);
        assertThat(blockingClient.isClosed()).isTrue();
        assertThat(asyncClient.isClosed()).isTrue();
    }

    @Test
    void testGetRecordsAsyncForgetsShardIteratorAtShardEnd() {
        AtomicInteger getShardIteratorCalls = new AtomicInteger();
        blockingClient.setNextShardIterator("first-shard-iterator");
        blockingClient.setNextShardIterator("second-shard-iterator");
        blockingClient.setShardIteratorValidation(
                request -> getShardIteratorCalls.incrementAndGet());
        blockingClient.setGetRecordsResponse(EMPTY_GET_RECORDS_RESPONSE);
        blockingClient.setGetRecordsValidation(request -> {});
        DynamoDbStreamsProxy proxy = createProxy();

        getRecords(proxy, StartingPosition.fromStart());
        getRecords(proxy, StartingPosition.fromStart());

        // The proxy keeps no iterator for the ended shard, so the second call gets a new one.
        assertThat(getShardIteratorCalls.get()).isEqualTo(2);
    }

    @Test
    void testGetRecordsForgetsShardIteratorAtShardEnd() {
        AtomicInteger getShardIteratorCalls = new AtomicInteger();
        blockingClient.setNextShardIterator("first-shard-iterator");
        blockingClient.setNextShardIterator("second-shard-iterator");
        blockingClient.setShardIteratorValidation(
                request -> getShardIteratorCalls.incrementAndGet());
        blockingClient.setGetRecordsResponse(EMPTY_GET_RECORDS_RESPONSE);
        blockingClient.setGetRecordsValidation(request -> {});
        DynamoDbStreamsProxy proxy = createProxy();

        proxy.getRecords(STREAM_ARN, SHARD_ID, StartingPosition.fromStart());
        proxy.getRecords(STREAM_ARN, SHARD_ID, StartingPosition.fromStart());

        assertThat(getShardIteratorCalls.get()).isEqualTo(2);
    }

    @Test
    void testBlockingMethodsNeedBlockingClient() {
        DynamoDbStreamsProxy proxy = new DynamoDbStreamsProxy(null, null, asyncClient, null);

        assertThatThrownBy(
                        () -> proxy.getRecords(STREAM_ARN, SHARD_ID, StartingPosition.fromStart()))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> proxy.listShards(STREAM_ARN, null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCloseClosesAsyncClientOfProxyWithoutBlockingClient() {
        DynamoDbStreamsProxy proxy = new DynamoDbStreamsProxy(null, null, asyncClient, null);

        proxy.close();

        assertThat(asyncClient.isClosed()).isTrue();
    }

    @Test
    void testCloseClosesAllClientsWhenOneFails() {
        TestingDynamoDbStreamsClient failingClient =
                new TestingDynamoDbStreamsClient() {
                    @Override
                    public void close() {
                        super.close();
                        throw new IllegalStateException("close failed");
                    }
                };
        DynamoDbStreamsProxy proxy =
                new DynamoDbStreamsProxy(
                        failingClient, ApacheHttpClient.builder().build(), asyncClient, null);

        assertThatThrownBy(proxy::close).hasMessage("close failed");
        assertThat(asyncClient.isClosed()).isTrue();
    }

    private DynamoDbStreamsProxy createProxy() {
        return new DynamoDbStreamsProxy(
                blockingClient, ApacheHttpClient.builder().build(), asyncClient, null);
    }

    private static GetRecordsResponse getRecords(
            DynamoDbStreamsProxy proxy, StartingPosition startingPosition) {
        return proxy.getRecordsAsync(STREAM_ARN, SHARD_ID, startingPosition).join();
    }

    private static ShardFilter childShardFilter(String shardId) {
        return ShardFilter.builder().type(ShardFilterType.CHILD_SHARDS).shardId(shardId).build();
    }
}
