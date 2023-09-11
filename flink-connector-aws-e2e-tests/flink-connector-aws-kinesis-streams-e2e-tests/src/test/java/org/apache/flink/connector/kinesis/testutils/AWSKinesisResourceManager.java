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

package org.apache.flink.connector.kinesis.testutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.StreamMode;
import software.amazon.awssdk.services.kinesis.model.StreamModeDetails;
import software.amazon.awssdk.services.kinesis.model.StreamSummary;
import software.amazon.awssdk.services.kinesis.paginators.ListStreamsIterable;
import software.amazon.awssdk.services.kinesis.waiters.KinesisWaiter;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/** Resource manager to create and clean up Kinesis Data Stream resources used for each test. */
public class AWSKinesisResourceManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSKinesisResourceManager.class);

    private static final String KINESIS_DATA_STREAMS_NAME_PREFIX = "flink-test-";
    private static final int DEFAULT_SHARD_COUNT = 1;
    private final KinesisClient kinesisClient;
    private final KinesisWaiter kinesisWaiter;
    private final Set<String> kinesisDataStreamARNs = new HashSet<>();

    public AWSKinesisResourceManager(KinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
        this.kinesisWaiter = KinesisWaiter.builder().client(kinesisClient).build();
    }

    public String createKinesisDataStream() {
        String streamName = KINESIS_DATA_STREAMS_NAME_PREFIX + UUID.randomUUID();
        LOGGER.info("Creating Kinesis Data Stream with name {}.", streamName);
        kinesisClient.createStream(
                CreateStreamRequest.builder()
                        .streamName(streamName)
                        .shardCount(DEFAULT_SHARD_COUNT)
                        .streamModeDetails(
                                StreamModeDetails.builder()
                                        .streamMode(StreamMode.PROVISIONED)
                                        .build())
                        .build());
        DescribeStreamResponse response =
                kinesisClient.describeStream(
                        DescribeStreamRequest.builder().streamName(streamName).build());
        String streamARN = response.streamDescription().streamARN();
        kinesisDataStreamARNs.add(streamARN);
        LOGGER.info("Successfully created Kinesis Data Stream with ARN {}.", streamARN);

        LOGGER.info("Waiting until Kinesis Data Stream with ARN {} is ACTIVE.", streamARN);
        kinesisWaiter.waitUntilStreamExists(
                DescribeStreamRequest.builder().streamARN(streamARN).build());
        return streamARN;
    }

    public void close() {
        kinesisDataStreamARNs.forEach(this::deleteStream);
    }

    public void cleanUpStaleKinesisDataStreams() {
        // Delete all test streams created at least 2 days ago.
        Instant twoDaysAgo = Instant.now().minus(2, ChronoUnit.DAYS);

        LOGGER.info(
                "Listing all Kinesis Data Stream with prefix {} created more than 2 days ago.",
                KINESIS_DATA_STREAMS_NAME_PREFIX);
        ListStreamsIterable listStreamsResponses = kinesisClient.listStreamsPaginator();
        listStreamsResponses.forEach(
                listStreamsResponse ->
                        listStreamsResponse.streamSummaries().stream()
                                .filter(
                                        streamSummary ->
                                                streamSummary
                                                        .streamName()
                                                        .startsWith(
                                                                KINESIS_DATA_STREAMS_NAME_PREFIX))
                                .filter(
                                        streamSummary ->
                                                streamSummary
                                                        .streamCreationTimestamp()
                                                        .isBefore(twoDaysAgo))
                                .map(StreamSummary::streamARN)
                                .forEach(
                                        streamARN -> {
                                            LOGGER.warn(
                                                    "Found stale Kinesis Data Stream with ARN {}. Deleting stream",
                                                    streamARN);
                                            deleteStream(streamARN);
                                        }));
    }

    private void deleteStream(String streamARN) {
        LOGGER.info("Deleting Kinesis Data Stream with ARN {}.", streamARN);
        kinesisClient.deleteStream(DeleteStreamRequest.builder().streamARN(streamARN).build());
    }
}
