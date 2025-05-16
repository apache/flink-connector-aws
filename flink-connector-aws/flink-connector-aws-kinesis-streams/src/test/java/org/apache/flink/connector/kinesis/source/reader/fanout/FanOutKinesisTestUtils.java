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

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.TestUtil;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Kinesis tests.
 */
public class FanOutKinesisTestUtils {

    /**
     * Creates a test Record with the given data.
     *
     * @param data The data to include in the record
     * @return A test Record
     */
    public static Record createTestRecord(String data) {
        return Record.builder()
                .data(SdkBytes.fromString(data, StandardCharsets.UTF_8))
                .approximateArrivalTimestamp(Instant.now())
                .partitionKey("partitionKey")
                .sequenceNumber("sequenceNumber")
                .build();
    }

    /**
     * Creates a test SubscribeToShardEvent with the given continuation sequence number and records.
     *
     * @param continuationSequenceNumber The continuation sequence number
     * @param records The records to include in the event
     * @return A test SubscribeToShardEvent
     */
    public static SubscribeToShardEvent createTestEvent(String continuationSequenceNumber, List<Record> records) {
        return SubscribeToShardEvent.builder()
                .continuationSequenceNumber(continuationSequenceNumber)
                .millisBehindLatest(0L)
                .records(records)
                .build();
    }

    /**
     * Creates a test SubscribeToShardEvent with the given continuation sequence number, records, and millisBehindLatest.
     *
     * @param continuationSequenceNumber The continuation sequence number
     * @param records The records to include in the event
     * @param millisBehindLatest The milliseconds behind latest
     * @return A test SubscribeToShardEvent
     */
    public static SubscribeToShardEvent createTestEvent(
            String continuationSequenceNumber, List<Record> records, long millisBehindLatest) {
        return SubscribeToShardEvent.builder()
                .continuationSequenceNumber(continuationSequenceNumber)
                .millisBehindLatest(millisBehindLatest)
                .records(records)
                .build();
    }

    /**
     * Creates a test KinesisShardSplit.
     *
     * @param streamArn The stream ARN
     * @param shardId The shard ID
     * @param startingPosition The starting position
     * @return A test KinesisShardSplit
     */
    public static KinesisShardSplit createTestSplit(
            String streamArn, String shardId, StartingPosition startingPosition) {
        return new KinesisShardSplit(
                streamArn,
                shardId,
                startingPosition,
                Collections.emptySet(),
                TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                TestUtil.ENDING_HASH_KEY_TEST_VALUE);
    }

    /**
     * Gets the subscription for a specific shard from the reader using reflection.
     *
     * @param reader The reader
     * @param shardId The shard ID
     * @return The subscription
     * @throws Exception If an error occurs
     */
    public static FanOutKinesisShardSubscription getSubscriptionFromReader(
            FanOutKinesisShardSplitReader reader, String shardId) throws Exception {
        // Get access to the subscriptions map
        java.lang.reflect.Field field = FanOutKinesisShardSplitReader.class.getDeclaredField("splitSubscriptions");
        field.setAccessible(true);
        Map<String, FanOutKinesisShardSubscription> subscriptions =
                (Map<String, FanOutKinesisShardSubscription>) field.get(reader);
        return subscriptions.get(shardId);
    }

    /**
     * Sets the starting position in a subscription using reflection.
     *
     * @param subscription The subscription
     * @param startingPosition The starting position
     * @throws Exception If an error occurs
     */
    public static void setStartingPositionInSubscription(
            FanOutKinesisShardSubscription subscription, StartingPosition startingPosition) throws Exception {
        // Get access to the startingPosition field
        java.lang.reflect.Field field = subscription.getClass().getDeclaredField("startingPosition");
        field.setAccessible(true);
        field.set(subscription, startingPosition);
    }
}
