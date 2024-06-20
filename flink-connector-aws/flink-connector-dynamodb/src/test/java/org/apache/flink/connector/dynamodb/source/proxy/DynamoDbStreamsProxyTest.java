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
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsClientProvider.TestingDynamoDbStreamsClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ExpiredIteratorException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

/** Tests to validate {@link DynamoDbStreamsProxy}. */
public class DynamoDbStreamsProxyTest {
    private static final SdkHttpClient HTTP_CLIENT = ApacheHttpClient.builder().build();

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"shardId-000000000002"})
    void testListShards(String lastSeenShardId) {
        final String streamArn =
                "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
        final List<Shard> expectedShards = getTestShards(0, 3);
        DescribeStreamResponse describeStreamResponse =
                DescribeStreamResponse.builder()
                        .streamDescription(
                                StreamDescription.builder()
                                        .shards(expectedShards)
                                        .streamStatus(StreamStatus.ENABLED)
                                        .lastEvaluatedShardId(null)
                                        .build())
                        .build();
        TestingDynamoDbStreamsClient testingDynamoDbStreamsClient =
                new TestingDynamoDbStreamsClient();
        testingDynamoDbStreamsClient.setDescribeStreamValidation(
                getDescribeStreamRequestValidation(streamArn, lastSeenShardId));
        testingDynamoDbStreamsClient.setDescribeStreamResponse(describeStreamResponse);

        DynamoDbStreamsProxy dynamoDbStreamsProxy =
                new DynamoDbStreamsProxy(testingDynamoDbStreamsClient, HTTP_CLIENT);

        assertThat(dynamoDbStreamsProxy.listShards(streamArn, lastSeenShardId))
                .isEqualTo(expectedShards);
    }

    @Test
    void testGetRecordsInitialReadFromTrimHorizon() {
        final String streamArn =
                "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();

        final String expectedShardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        TestingDynamoDbStreamsClient testingDynamoDbStreamsClient =
                new TestingDynamoDbStreamsClient();
        testingDynamoDbStreamsClient.setNextShardIterator(expectedShardIterator);
        testingDynamoDbStreamsClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamArn(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                .build()));
        testingDynamoDbStreamsClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testingDynamoDbStreamsClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder().shardIterator(expectedShardIterator).build()));

        DynamoDbStreamsProxy dynamoDbStreamsProxy =
                new DynamoDbStreamsProxy(testingDynamoDbStreamsClient, HTTP_CLIENT);

        assertThat(dynamoDbStreamsProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testGetRecordsInitialReadFromLatest() {
        final String streamArn =
                "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.latest();

        final String expectedShardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        TestingDynamoDbStreamsClient testingDynamoDbStreamsClient =
                new TestingDynamoDbStreamsClient();
        testingDynamoDbStreamsClient.setNextShardIterator(expectedShardIterator);
        testingDynamoDbStreamsClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamArn(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.LATEST)
                                .build()));
        testingDynamoDbStreamsClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testingDynamoDbStreamsClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder().shardIterator(expectedShardIterator).build()));

        DynamoDbStreamsProxy dynamoDbStreamsProxy =
                new DynamoDbStreamsProxy(testingDynamoDbStreamsClient, HTTP_CLIENT);

        assertThat(dynamoDbStreamsProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testGetRecordsWhenNextShardIteratorIsNull() {
        final String streamArn =
                "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition =
                StartingPosition.continueFromSequenceNumber("123456002332");

        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Collections.emptyList())
                        .nextShardIterator(null)
                        .build();

        TestingDynamoDbStreamsClient testingDynamoDbStreamsClient =
                new TestingDynamoDbStreamsClient();
        testingDynamoDbStreamsClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamArn(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .sequenceNumber("123456002332")
                                .build()));
        testingDynamoDbStreamsClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testingDynamoDbStreamsClient.setGetRecordsValidation(
                validateEqual(GetRecordsRequest.builder().shardIterator(null).build()));

        DynamoDbStreamsProxy dynamoDbStreamsProxy =
                new DynamoDbStreamsProxy(testingDynamoDbStreamsClient, HTTP_CLIENT);

        assertThat(dynamoDbStreamsProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(expectedGetRecordsResponse);
    }

    @Test
    void testCloseClosesDynamoDbStreamsClient() {
        TestingDynamoDbStreamsClient testingDynamoDbStreamsClient =
                new TestingDynamoDbStreamsClient();

        DynamoDbStreamsProxy dynamoDbStreamsProxy =
                new DynamoDbStreamsProxy(testingDynamoDbStreamsClient, HTTP_CLIENT);

        assertThatNoException().isThrownBy(dynamoDbStreamsProxy::close);
        assertThat(testingDynamoDbStreamsClient.isClosed()).isTrue();
    }

    @Test
    void testGetRecordsEagerlyRetriesExpiredIterators() {
        final String streamArn =
                "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();

        final String firstShardIterator = "first-shard-iterator";
        final String secondShardIterator = "second-shard-iterator";
        final GetRecordsResponse getRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator(secondShardIterator)
                        .build();

        TestingDynamoDbStreamsClient testingDynamoDbStreamsClient =
                new TestingDynamoDbStreamsClient();
        DynamoDbStreamsProxy dynamoDbStreamsProxy =
                new DynamoDbStreamsProxy(testingDynamoDbStreamsClient, HTTP_CLIENT);

        // When expired shard iterator is thrown on the first GetRecords() call
        AtomicBoolean firstGetRecordsCall = new AtomicBoolean(true);
        testingDynamoDbStreamsClient.setNextShardIterator(firstShardIterator);
        testingDynamoDbStreamsClient.setShardIteratorValidation(ignored -> {});
        testingDynamoDbStreamsClient.setNextShardIterator(secondShardIterator);
        testingDynamoDbStreamsClient.setGetRecordsResponse(getRecordsResponse);
        testingDynamoDbStreamsClient.setGetRecordsValidation(
                request -> {
                    if (firstGetRecordsCall.get()) {
                        firstGetRecordsCall.set(false);
                        throw ExpiredIteratorException.builder().build();
                    }
                    // Then getRecords called with second shard iterator
                    validateEqual(
                            GetRecordsRequest.builder().shardIterator(secondShardIterator).build());
                });

        // Then getRecords called with second shard iterator
        assertThat(dynamoDbStreamsProxy.getRecords(streamArn, shardId, startingPosition))
                .isEqualTo(getRecordsResponse);
        assertThat(firstGetRecordsCall.get()).isFalse();
    }

    @Test
    void testGetRecordsHandlesCompletedShard() {
        final String streamArn =
                "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
        final String shardId = "shardId-000000000002";
        final String sequenceNumber = "some-sequence-number";
        final StartingPosition startingPosition =
                StartingPosition.continueFromSequenceNumber(sequenceNumber);
        final String expectedShardIterator = "some-shard-iterator";

        // When completed shard has null nextShardIterator
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder().records(Record.builder().build()).build();

        TestingDynamoDbStreamsClient testingDynamoDbStreamsClient =
                new TestingDynamoDbStreamsClient();
        testingDynamoDbStreamsClient.setNextShardIterator(expectedShardIterator);
        testingDynamoDbStreamsClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamArn(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .sequenceNumber(sequenceNumber)
                                .build()));
        testingDynamoDbStreamsClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testingDynamoDbStreamsClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder().shardIterator(expectedShardIterator).build()));

        DynamoDbStreamsProxy dynamoDbStreamsProxy =
                new DynamoDbStreamsProxy(testingDynamoDbStreamsClient, HTTP_CLIENT);

        assertThatNoException()
                .isThrownBy(
                        () ->
                                dynamoDbStreamsProxy.getRecords(
                                        streamArn, shardId, startingPosition));
    }

    private List<Shard> getTestShards(final int startShardId, final int endShardId) {
        List<Shard> shards = new ArrayList<>();
        for (int i = startShardId; i <= endShardId; i++) {
            shards.add(Shard.builder().shardId(generateShardId(i)).build());
        }
        return shards;
    }

    private Consumer<DescribeStreamRequest> getDescribeStreamRequestValidation(
            final String streamArn, final String startShardId) {
        return req -> {
            DescribeStreamRequest expectedReq =
                    DescribeStreamRequest.builder()
                            .streamArn(streamArn)
                            .exclusiveStartShardId(startShardId)
                            .build();
            assertThat(req).isEqualTo(expectedReq);
        };
    }

    private <R extends DynamoDbStreamsRequest> Consumer<R> validateEqual(final R request) {
        return req -> assertThat(req).isEqualTo(request);
    }
}
