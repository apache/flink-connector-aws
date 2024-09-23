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
import org.apache.flink.connector.kinesis.source.util.KinesisAsyncClientProvider.TestingAsyncKinesisClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.KinesisAsyncClientProvider.TestingAsyncKinesisClient.SUBSCRIBE_TO_SHARD_RESPONSE_FUTURE;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class KinesisAsyncStreamProxyTest {
    private static final SdkAsyncHttpClient HTTP_CLIENT = NettyNioAsyncHttpClient.builder().build();

    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/stream-name";

    private TestingAsyncKinesisClient testKinesisClient;
    private KinesisAsyncStreamProxy kinesisAsyncStreamProxy;

    @BeforeEach
    public void setUp() {
        testKinesisClient = new TestingAsyncKinesisClient();
        kinesisAsyncStreamProxy = new KinesisAsyncStreamProxy(testKinesisClient, HTTP_CLIENT);
    }

    @ParameterizedTest
    @MethodSource("provideSubscribeToShardStartingPosition")
    public void testSubscribeToShard(final String shardId, final StartingPosition startingPosition) {
        // Given subscription arguments
        SubscribeToShardResponseHandler noOpResponseHandler = SubscribeToShardResponseHandler.builder()
                .subscriber(event -> {
                })
                .onError(throwable -> {
                })
                .onComplete(() -> {
                })
                .build();

        // When proxy is invoked
        CompletableFuture<Void> result = kinesisAsyncStreamProxy.subscribeToShard(CONSUMER_ARN, shardId, startingPosition, noOpResponseHandler);

        // Then correct request is passed through to the Kinesis client
        SubscribeToShardRequest expectedRequest = SubscribeToShardRequest.builder()
                .consumerARN(CONSUMER_ARN)
                .shardId(shardId)
                .startingPosition(startingPosition.getSdkStartingPosition())
                .build();
        assertThat(result).isEqualTo(SUBSCRIBE_TO_SHARD_RESPONSE_FUTURE);
        assertThat(testKinesisClient.getSubscribeToShardRequest())
                .isEqualTo(expectedRequest);
        assertThat(testKinesisClient.getSubscribeToShardResponseHandler())
                .isEqualTo(noOpResponseHandler);
    }


    private static Stream<Arguments> provideSubscribeToShardStartingPosition() {
        return Stream.of(
                // Randomly generated shardIds
                Arguments.of(generateShardId(0), StartingPosition.fromStart()),
                Arguments.of(generateShardId(1), StartingPosition.fromStart()),
                // Check all starting positions
                Arguments.of(generateShardId(0), StartingPosition.fromTimestamp(Instant.now())),
                Arguments.of(generateShardId(0), StartingPosition.continueFromSequenceNumber("seq-num")));
    }

}
