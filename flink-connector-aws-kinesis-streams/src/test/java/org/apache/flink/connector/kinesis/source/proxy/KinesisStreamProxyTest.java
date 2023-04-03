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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class KinesisStreamProxyTest {
    private static final SdkHttpClient HTTP_CLIENT = ApacheHttpClient.builder().build();

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"shardId-000000000002"})
    void testListShardsSingleCall(String lastSeenShardId) {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final List<Shard> expectedShards = getTestShards(0, 3);

        List<ListShardItem> listShardItems =
                ImmutableList.of(
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, lastSeenShardId, null))
                                .shards(expectedShards)
                                .nextToken(null)
                                .build());
        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        testKinesisClient.setListShardsResponses(listShardItems);

        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

        assertThat(kinesisStreamProxy.listShards(streamArn, lastSeenShardId))
                .isEqualTo(expectedShards);
    }

    @Test
    void testListShardsMultipleCalls() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String lastSeenShardId = "shardId-000000000000";
        final List<Shard> expectedShards = getTestShards(0, 3);

        List<ListShardItem> listShardItems =
                ImmutableList.of(
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, lastSeenShardId, null))
                                .shards(expectedShards.subList(0, 1))
                                .nextToken("next-token-1")
                                .build(),
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, null, "next-token-1"))
                                .shards(expectedShards.subList(1, 2))
                                .nextToken("next-token-2")
                                .build(),
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, null, "next-token-2"))
                                .shards(expectedShards.subList(2, 4))
                                .nextToken(null)
                                .build());
        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        testKinesisClient.setListShardsResponses(listShardItems);

        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

        assertThat(kinesisStreamProxy.listShards(streamArn, lastSeenShardId))
                .isEqualTo(expectedShards);
    }

    @Test
    void testGetRecordsInitialReadFromTrimHorizon() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();

        final String expectedShardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(streamArn)
                                .shardIterator(expectedShardIterator)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

        assertThat(kinesisStreamProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testGetRecordsInitialReadFromTimestamp() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final Instant timestamp = Instant.now();
        final StartingPosition startingPosition = StartingPosition.fromTimestamp(timestamp);

        final String expectedShardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                                .timestamp(timestamp)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(streamArn)
                                .shardIterator(expectedShardIterator)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

        assertThat(kinesisStreamProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testGetRecordsInitialReadFromSequenceNumber() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
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

        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .startingSequenceNumber(sequenceNumber)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(streamArn)
                                .shardIterator(expectedShardIterator)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

        assertThat(kinesisStreamProxy.getRecords(streamArn, shardId, startingPosition))
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

        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

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
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();

        final String firstShardIterator = "first-shard-iterator";
        final String secondShardIterator = "second-shard-iterator";
        final GetRecordsResponse getRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator(secondShardIterator)
                        .build();

        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

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
                                    .streamARN(streamArn)
                                    .shardIterator(secondShardIterator)
                                    .build());
                });

        // Then getRecords called with second shard iterator
        assertThat(kinesisStreamProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(getRecordsResponse);
        assertThat(firstGetRecordsCall.get()).isFalse();
    }

    @Test
    void testGetRecordsHandlesCompletedShard() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final String sequenceNumber = "some-sequence-number";
        final StartingPosition startingPosition =
                StartingPosition.continueFromSequenceNumber(sequenceNumber);
        final String expectedShardIterator = "some-shard-iterator";

        // When completed shard has null nextShardIterator
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder().records(Record.builder().build()).build();

        TestingKinesisClient testKinesisClient = new TestingKinesisClient();
        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .startingSequenceNumber(sequenceNumber)
                                .build()));
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(streamArn)
                                .shardIterator(expectedShardIterator)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy =
                new KinesisStreamProxy(testKinesisClient, HTTP_CLIENT);

        assertThatNoException()
                .isThrownBy(
                        () -> kinesisStreamProxy.getRecords(streamArn, shardId, startingPosition));
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
            shards.add(Shard.builder().shardId(generateShardId(i)).build());
        }
        return shards;
    }

    private Consumer<ListShardsRequest> getListShardRequestValidation(
            final String streamArn, final String startShardId, final String nextToken) {
        return req -> {
            ListShardsRequest expectedReq =
                    ListShardsRequest.builder()
                            .streamARN(streamArn)
                            .exclusiveStartShardId(startShardId)
                            .nextToken(nextToken)
                            .build();
            assertThat(req).isEqualTo(expectedReq);
        };
    }

    private <R extends KinesisRequest> Consumer<R> validateEqual(final R request) {
        return req -> assertThat(req).isEqualTo(request);
    }
}
