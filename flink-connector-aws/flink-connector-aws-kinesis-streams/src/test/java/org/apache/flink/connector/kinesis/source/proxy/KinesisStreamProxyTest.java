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

package org.apache.flink.connector.kinesis.source.proxy;

import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.KinesisClientProvider.ListShardItem;
import org.apache.flink.connector.kinesis.source.util.KinesisClientProvider.TestingKinesisClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.KinesisRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.ENDING_HASH_KEY_TEST_VALUE;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STARTING_HASH_KEY_TEST_VALUE;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class KinesisStreamProxyTest {
    private static final SdkHttpClient HTTP_CLIENT = ApacheHttpClient.builder().build();

    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/stream-name";

    private TestingKinesisClient testKinesisClient;
    private KinesisStreamProxy kinesisStreamProxy;

    @BeforeEach
    public void setUp() {
        testKinesisClient = new TestingKinesisClient();
        kinesisStreamProxy = new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);
    }

    @Test
    public void testDescribeStreamSummary() {
        StreamDescriptionSummary streamDescriptionSummary =
                StreamDescriptionSummary.builder()
                        .streamARN(STREAM_ARN)
                        .streamCreationTimestamp(Instant.now())
                        .retentionPeriodHours(24)
                        .build();
        DescribeStreamSummaryResponse describeStreamSummaryResponse =
                DescribeStreamSummaryResponse.builder()
                        .streamDescriptionSummary(streamDescriptionSummary)
                        .build();
        testKinesisClient.setDescribeStreamSummaryResponse(describeStreamSummaryResponse);
        testKinesisClient.setDescribeStreamSummaryRequestValidation(
                request -> {
                    DescribeStreamSummaryRequest expectedRequest =
                            DescribeStreamSummaryRequest.builder().streamARN(STREAM_ARN).build();
                    assertThat(request).isEqualTo(expectedRequest);
                });

        StreamDescriptionSummary response =
                kinesisStreamProxy.getStreamDescriptionSummary(STREAM_ARN);

        assertThat(response).isEqualTo(streamDescriptionSummary);
    }

    @ParameterizedTest
    @MethodSource("provideListShardStartingPosition")
    void testListShardsSingleCall(final ListShardsStartingPosition startingPosition) {
        final List<Shard> expectedShards = getTestShards(0, 3);

        List<ListShardItem> listShardItems =
                Collections.singletonList(
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                STREAM_ARN,
                                                startingPosition.getShardFilter(),
                                                null))
                                .shards(expectedShards)
                                .nextToken(null)
                                .build());
        testKinesisClient.setListShardsResponses(listShardItems);

        assertThat(kinesisStreamProxy.listShards(STREAM_ARN, startingPosition))
                .isEqualTo(expectedShards);
    }

    private static Stream<ListShardsStartingPosition> provideListShardStartingPosition() {
        return Stream.of(
                ListShardsStartingPosition.fromStart(),
                ListShardsStartingPosition.fromTimestamp(Instant.ofEpochSecond(1720622954)),
                ListShardsStartingPosition.fromShardId(generateShardId(12)));
    }

    @Test
    void testListShardsMultipleCalls() {
        final String lastSeenShardId = "shardId-000000000000";
        final List<Shard> expectedShards = getTestShards(0, 3);

        ListShardsStartingPosition startingPosition =
                ListShardsStartingPosition.fromShardId(lastSeenShardId);
        List<ListShardItem> listShardItems =
                Stream.of(
                                ListShardItem.builder()
                                        .validation(
                                                getListShardRequestValidation(
                                                        STREAM_ARN,
                                                        startingPosition.getShardFilter(),
                                                        null))
                                        .shards(expectedShards.subList(0, 1))
                                        .nextToken("next-token-1")
                                        .build(),
                                ListShardItem.builder()
                                        .validation(
                                                getListShardRequestValidation(
                                                        STREAM_ARN, null, "next-token-1"))
                                        .shards(expectedShards.subList(1, 2))
                                        .nextToken("next-token-2")
                                        .build(),
                                ListShardItem.builder()
                                        .validation(
                                                getListShardRequestValidation(
                                                        STREAM_ARN, null, "next-token-2"))
                                        .shards(expectedShards.subList(2, 4))
                                        .nextToken(null)
                                        .build())
                        .collect(Collectors.toList());

        testKinesisClient.setListShardsResponses(listShardItems);

        assertThat(kinesisStreamProxy.listShards(STREAM_ARN, startingPosition))
                .isEqualTo(expectedShards);
    }

    @Test
    void testGetRecordsInitialReadFromTrimHorizon() {
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();

        final String expectedShardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardIterator(expectedShardIterator)
                                .build()));

        assertThat(kinesisStreamProxy.getRecords(STREAM_ARN, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testGetRecordsInitialReadFromTimestamp() {
        final String shardId = "shardId-000000000002";
        final Instant timestamp = Instant.now();
        final StartingPosition startingPosition = StartingPosition.fromTimestamp(timestamp);

        final String expectedShardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                                .timestamp(timestamp)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardIterator(expectedShardIterator)
                                .build()));

        assertThat(kinesisStreamProxy.getRecords(STREAM_ARN, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testGetRecordsInitialReadFromSequenceNumber() {
        final String shardId = "shardId-000000000002";
        final String sequenceNumber = "some-sequence-number";
        final StartingPosition startingPosition =
                StartingPosition.continueFromSequenceNumber(sequenceNumber);

        final String expectedShardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .startingSequenceNumber(sequenceNumber)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardIterator(expectedShardIterator)
                                .build()));

        assertThat(kinesisStreamProxy.getRecords(STREAM_ARN, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testConsecutiveGetRecordsUsesShardIteratorFromResponse() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();

        final String firstShardIterator = "first-shard-iterator";
        final String secondShardIterator = "second-shard-iterator";
        final GetRecordsResponse firstGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator(secondShardIterator)
                        .build();
        final GetRecordsResponse secondGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("third-shard-iterator")
                        .build();

        // When read for the first time
        testKinesisClient.setNextShardIterator(firstShardIterator);
        // Then getShardIterator called
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                .build()));
        testKinesisClient.setGetRecordsResponse(firstGetRecordsResponse);
        // Then getRecords called with returned shard iterator
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(streamArn)
                                .shardIterator(firstShardIterator)
                                .build()));
        assertThat(kinesisStreamProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(firstGetRecordsResponse);

        // When read for the second time
        // Then getShardsIterator() not called
        testKinesisClient.setShardIteratorValidation(
                (request) -> {
                    throw new RuntimeException(
                            "Call to GetShardIterator not expected on subsequent get records call");
                });
        testKinesisClient.setGetRecordsResponse(secondGetRecordsResponse);
        // Then getRecords() called with shardIterator from previous response
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(streamArn)
                                .shardIterator(secondShardIterator)
                                .build()));
        assertThat(kinesisStreamProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(secondGetRecordsResponse);
    }

    @Test
    void testGetRecordsEagerlyRetriesExpiredIterators() {
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();

        final String firstShardIterator = "first-shard-iterator";
        final String secondShardIterator = "second-shard-iterator";
        final GetRecordsResponse getRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator(secondShardIterator)
                        .build();

        // When expired shard iterator is thrown on the first GetRecords() call
        AtomicBoolean firstGetRecordsCall = new AtomicBoolean(true);
        testKinesisClient.setNextShardIterator(firstShardIterator);
        testKinesisClient.setShardIteratorValidation(ignored -> {});
        testKinesisClient.setNextShardIterator(secondShardIterator);
        testKinesisClient.setGetRecordsResponse(getRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                request -> {
                    if (firstGetRecordsCall.get()) {
                        firstGetRecordsCall.set(false);
                        throw ExpiredIteratorException.builder().build();
                    }
                    // Then getRecords called with second shard iterator
                    validateEqual(
                            GetRecordsRequest.builder()
                                    .streamARN(STREAM_ARN)
                                    .shardIterator(secondShardIterator)
                                    .build());
                });

        // Then getRecords called with second shard iterator
        assertThat(kinesisStreamProxy.getRecords(STREAM_ARN, shardId, startingPosition))
                .isEqualTo(getRecordsResponse);
        assertThat(firstGetRecordsCall.get()).isFalse();
    }

    @Test
    void testGetRecordsHandlesCompletedShard() {
        final String shardId = "shardId-000000000002";
        final String sequenceNumber = "some-sequence-number";
        final StartingPosition startingPosition =
                StartingPosition.continueFromSequenceNumber(sequenceNumber);
        final String expectedShardIterator = "some-shard-iterator";

        // When completed shard has null nextShardIterator
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder().records(Record.builder().build()).build();

        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .startingSequenceNumber(sequenceNumber)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(STREAM_ARN)
                                .shardIterator(expectedShardIterator)
                                .build()));

        assertThatNoException()
                .isThrownBy(
                        () -> kinesisStreamProxy.getRecords(STREAM_ARN, shardId, startingPosition));
    }

    @Test
    void testCloseClosesKinesisClient() {
        TestingKinesisClient testKinesisClient = new TestingKinesisClient();

        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

        assertThatNoException().isThrownBy(kinesisStreamProxy::close);
        assertThat(testKinesisClient.isClosed()).isTrue();
    }

    private List<Shard> getTestShards(final int startShardId, final int endShardId) {
        List<Shard> shards = new ArrayList<>();
        for (int i = startShardId; i <= endShardId; i++) {
            shards.add(
                    Shard.builder()
                            .shardId(generateShardId(i))
                            .hashKeyRange(
                                    HashKeyRange.builder()
                                            .startingHashKey(STARTING_HASH_KEY_TEST_VALUE)
                                            .endingHashKey(ENDING_HASH_KEY_TEST_VALUE)
                                            .build())
                            .build());
        }
        return shards;
    }

    private Consumer<ListShardsRequest> getListShardRequestValidation(
            final String streamArn, final ShardFilter shardFilter, final String nextToken) {
        return req -> {
            ListShardsRequest expectedReq =
                    ListShardsRequest.builder()
                            .streamARN(streamArn)
                            .shardFilter(shardFilter)
                            .nextToken(nextToken)
                            .build();
            assertThat(req).isEqualTo(expectedReq);
        };
    }

    private <R extends KinesisRequest> Consumer<R> validateEqual(final R request) {
        return req -> assertThat(req).isEqualTo(request);
    }
}
