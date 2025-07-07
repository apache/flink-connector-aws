/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Implementation of the {@link StreamProxy} for Kinesis data streams. */
@Internal
public class KinesisStreamProxy implements StreamProxy {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamProxy.class);
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5000;
    private final long shutdownTimeoutMillis;

    private final KinesisClient kinesisClient;
    private final SdkHttpClient httpClient;
    private final Map<String, String> shardIdToIteratorStore;

    public KinesisStreamProxy(KinesisClient kinesisClient, SdkHttpClient httpClient) {
        this(kinesisClient, httpClient, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    public KinesisStreamProxy(KinesisClient kinesisClient, SdkHttpClient httpClient, long shutdownTimeoutMillis) {
        this.kinesisClient = kinesisClient;
        this.httpClient = httpClient;
        this.shardIdToIteratorStore = new ConcurrentHashMap<>();
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    }

    @Override
    public StreamDescriptionSummary getStreamDescriptionSummary(String streamArn) {
        DescribeStreamSummaryResponse response =
                kinesisClient.describeStreamSummary(
                        DescribeStreamSummaryRequest.builder().streamARN(streamArn).build());
        return response.streamDescriptionSummary();
    }

    @Override
    public List<Shard> listShards(String streamArn, ListShardsStartingPosition startingPosition) {
        List<Shard> shards = new ArrayList<>();

        ListShardsResponse listShardsResponse;
        String nextToken = null;
        do {
            listShardsResponse =
                    kinesisClient.listShards(
                            ListShardsRequest.builder()
                                    .streamARN(nextToken == null ? streamArn : null)
                                    .shardFilter(
                                            nextToken == null
                                                    ? startingPosition.getShardFilter()
                                                    : null)
                                    .nextToken(nextToken)
                                    .build());

            shards.addAll(listShardsResponse.shards());
            nextToken = listShardsResponse.nextToken();
        } while (nextToken != null);

        return shards;
    }

    @Override
    public GetRecordsResponse getRecords(
            String streamArn,
            String shardId,
            StartingPosition startingPosition,
            int maxRecordsToGet) {
        String shardIterator =
                shardIdToIteratorStore.computeIfAbsent(
                        shardId, (s) -> getShardIterator(streamArn, s, startingPosition));

        try {
            GetRecordsResponse getRecordsResponse =
                    getRecords(streamArn, shardIterator, maxRecordsToGet);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        } catch (ExpiredIteratorException e) {
            // Eagerly retry getRecords() if the iterator is expired
            shardIterator = getShardIterator(streamArn, shardId, startingPosition);
            GetRecordsResponse getRecordsResponse =
                    getRecords(streamArn, shardIterator, maxRecordsToGet);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        }
    }

    private String getShardIterator(
            String streamArn, String shardId, StartingPosition startingPosition) {
        GetShardIteratorRequest.Builder requestBuilder =
                GetShardIteratorRequest.builder()
                        .streamARN(streamArn)
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
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_TIMESTAMP. Must be a Instant object.");
                }
                break;
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                if (startingPosition.getStartingMarker() instanceof String) {
                    requestBuilder =
                            requestBuilder.startingSequenceNumber(
                                    (String) startingPosition.getStartingMarker());
                } else {
                    throw new IllegalArgumentException(
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
                }
        }

        return kinesisClient.getShardIterator(requestBuilder.build()).shardIterator();
    }

    private GetRecordsResponse getRecords(
            String streamArn, String shardIterator, int maxRecordsToGet) {
        return kinesisClient.getRecords(
                GetRecordsRequest.builder()
                        .streamARN(streamArn)
                        .shardIterator(shardIterator)
                        .limit(maxRecordsToGet)
                        .build());
    }

    // Enhanced Fan-Out Consumer - related methods

    @Override
    public RegisterStreamConsumerResponse registerStreamConsumer(
            String streamArn, String consumerName) {
        return kinesisClient.registerStreamConsumer(
                RegisterStreamConsumerRequest.builder()
                        .streamARN(streamArn)
                        .consumerName(consumerName)
                        .build());
    }

    @Override
    public DeregisterStreamConsumerResponse deregisterStreamConsumer(String consumerArn) {
        return kinesisClient.deregisterStreamConsumer(
                DeregisterStreamConsumerRequest.builder().consumerARN(consumerArn).build());
    }

    @Override
    public DescribeStreamConsumerResponse describeStreamConsumer(
            String streamArn, String consumerName) {
        return kinesisClient.describeStreamConsumer(
                DescribeStreamConsumerRequest.builder()
                        .streamARN(streamArn)
                        .consumerName(consumerName)
                        .build());
    }

    /**
     * Gracefully closes the Kinesis clients with a timeout.
     *
     * @param timeoutMillis maximum time to wait for clients to close
     */
    public void gracefulClose(long timeoutMillis) {
        try {
            LOG.debug("Closing Kinesis clients with timeout of {} ms", timeoutMillis);

            // Close the Kinesis client first with half the timeout
            long kinesisClientTimeout = timeoutMillis / 2;
            LOG.debug("Closing KinesisClient with timeout of {} ms", kinesisClientTimeout);
            try {
                CompletableFuture<Void> kinesisClientFuture = CompletableFuture.runAsync(() -> kinesisClient.close());
                kinesisClientFuture.get(kinesisClientTimeout, TimeUnit.MILLISECONDS);
                LOG.debug("Successfully closed KinesisClient");
            } catch (TimeoutException e) {
                LOG.warn("Closing KinesisClient timed out after {} ms", kinesisClientTimeout);
            } catch (Exception e) {
                LOG.warn("Error while closing KinesisClient", e);
            }

            // Then close the HTTP client with the remaining timeout
            long httpClientTimeout = timeoutMillis - kinesisClientTimeout;
            LOG.debug("Closing SdkHttpClient with timeout of {} ms", httpClientTimeout);
            try {
                CompletableFuture<Void> httpClientFuture = CompletableFuture.runAsync(() -> httpClient.close());
                httpClientFuture.get(httpClientTimeout, TimeUnit.MILLISECONDS);
                LOG.debug("Successfully closed SdkHttpClient");
            } catch (TimeoutException e) {
                LOG.warn("Closing SdkHttpClient timed out after {} ms", httpClientTimeout);
            } catch (Exception e) {
                LOG.warn("Error while closing SdkHttpClient", e);
            }

            LOG.debug("Completed graceful shutdown of Kinesis clients");
        } catch (Exception e) {
            LOG.warn("Error during graceful shutdown of Kinesis clients", e);
        }
    }

    @Override
    public void close() throws IOException {
        gracefulClose(shutdownTimeoutMillis);
    }
}
