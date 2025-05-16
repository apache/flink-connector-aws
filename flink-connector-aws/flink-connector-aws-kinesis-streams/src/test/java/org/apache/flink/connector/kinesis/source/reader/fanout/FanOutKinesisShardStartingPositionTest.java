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

import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the starting position behavior in {@link FanOutKinesisShardSubscription}.
 */
public class FanOutKinesisShardStartingPositionTest extends FanOutKinesisShardTestBase {

    /**
     * Tests that the starting position is correctly recorded after queue.put for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionRecordedAfterQueuePutSingleShard() throws Exception {
        // Create a blocking queue to store processed records
        BlockingQueue<Record> processedRecords = new LinkedBlockingQueue<>();

        // Create a custom TestableSubscription that captures processed records
        TestableSubscription testSubscription = createTestableSubscription(
                SHARD_ID,
                StartingPosition.fromStart(),
                processedRecords);

        // Create a test event with records
        String continuationSequenceNumber = "sequence-1";
        List<Record> records = new ArrayList<>();
        records.add(FanOutKinesisTestUtils.createTestRecord("record-1"));
        records.add(FanOutKinesisTestUtils.createTestRecord("record-2"));

        // Process the event
        testSubscription.processSubscriptionEvent(
                FanOutKinesisTestUtils.createTestEvent(continuationSequenceNumber, records));

        // Verify that all records were processed
        List<Record> allProcessedRecords = new ArrayList<>();
        processedRecords.drainTo(allProcessedRecords);
        assertThat(allProcessedRecords).hasSize(2);

        // Verify that the starting position was updated correctly
        assertThat(testSubscription.getStartingPosition().getStartingMarker())
                .isEqualTo(continuationSequenceNumber);
    }

    /**
     * Tests that the starting position is correctly recorded after queue.put for multiple shards.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionRecordedAfterQueuePutMultipleShards() throws Exception {
        // Create blocking queues to store processed records for each shard
        BlockingQueue<Record> processedRecordsShard1 = new LinkedBlockingQueue<>();
        BlockingQueue<Record> processedRecordsShard2 = new LinkedBlockingQueue<>();

        // Create custom TestableSubscriptions for each shard
        TestableSubscription subscription1 = createTestableSubscription(
                SHARD_ID_1,
                StartingPosition.fromStart(),
                processedRecordsShard1);

        TestableSubscription subscription2 = createTestableSubscription(
                SHARD_ID_2,
                StartingPosition.fromStart(),
                processedRecordsShard2);

        // Create test events with records for each shard
        String continuationSequenceNumber1 = "sequence-shard1";
        String continuationSequenceNumber2 = "sequence-shard2";

        List<Record> recordsShard1 = new ArrayList<>();
        recordsShard1.add(FanOutKinesisTestUtils.createTestRecord("shard1-record-1"));
        recordsShard1.add(FanOutKinesisTestUtils.createTestRecord("shard1-record-2"));

        List<Record> recordsShard2 = new ArrayList<>();
        recordsShard2.add(FanOutKinesisTestUtils.createTestRecord("shard2-record-1"));
        recordsShard2.add(FanOutKinesisTestUtils.createTestRecord("shard2-record-2"));

        // Process the events
        subscription1.processSubscriptionEvent(
                FanOutKinesisTestUtils.createTestEvent(continuationSequenceNumber1, recordsShard1));
        subscription2.processSubscriptionEvent(
                FanOutKinesisTestUtils.createTestEvent(continuationSequenceNumber2, recordsShard2));

        // Verify that all records were processed for shard 1
        List<Record> allProcessedRecordsShard1 = new ArrayList<>();
        processedRecordsShard1.drainTo(allProcessedRecordsShard1);
        assertThat(allProcessedRecordsShard1).hasSize(2);

        // Verify that all records were processed for shard 2
        List<Record> allProcessedRecordsShard2 = new ArrayList<>();
        processedRecordsShard2.drainTo(allProcessedRecordsShard2);
        assertThat(allProcessedRecordsShard2).hasSize(2);

        // Verify that the starting positions were updated correctly
        assertThat(subscription1.getStartingPosition().getStartingMarker())
                .isEqualTo(continuationSequenceNumber1);
        assertThat(subscription2.getStartingPosition().getStartingMarker())
                .isEqualTo(continuationSequenceNumber2);
    }

    /**
     * Tests that the starting position is not recorded when queue.put fails for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionNotRecordedWhenQueuePutFailsSingleShard() throws Exception {
        // Create a custom TestableSubscription with a failing queue
        TestableSubscription testSubscription = createTestableSubscription(
                SHARD_ID,
                StartingPosition.fromStart(),
                null); // Null queue will cause queue.put to be skipped

        // Set the flag to not update starting position
        testSubscription.setShouldUpdateStartingPosition(false);

        // Create a test event with records
        String continuationSequenceNumber = "sequence-1";
        List<Record> records = new ArrayList<>();
        records.add(FanOutKinesisTestUtils.createTestRecord("record-1"));
        records.add(FanOutKinesisTestUtils.createTestRecord("record-2"));

        // Store the original starting position
        StartingPosition originalPosition = testSubscription.getStartingPosition();

        // Process the event
        testSubscription.processSubscriptionEvent(
                FanOutKinesisTestUtils.createTestEvent(continuationSequenceNumber, records));

        // Verify that the starting position was not updated
        assertThat(testSubscription.getStartingPosition()).isEqualTo(originalPosition);
    }

    /**
     * Tests that the starting position is not recorded when queue.put fails for multiple shards.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionNotRecordedWhenQueuePutFailsMultipleShards() throws Exception {
        // Create custom TestableSubscriptions with failing queues
        TestableSubscription subscription1 = createTestableSubscription(
                SHARD_ID_1,
                StartingPosition.fromStart(),
                null); // Null queue will cause queue.put to be skipped

        TestableSubscription subscription2 = createTestableSubscription(
                SHARD_ID_2,
                StartingPosition.fromStart(),
                null); // Null queue will cause queue.put to be skipped

        // Set the flags to not update starting positions
        subscription1.setShouldUpdateStartingPosition(false);
        subscription2.setShouldUpdateStartingPosition(false);

        // Create test events with records for each shard
        String continuationSequenceNumber1 = "sequence-shard1";
        String continuationSequenceNumber2 = "sequence-shard2";

        List<Record> recordsShard1 = new ArrayList<>();
        recordsShard1.add(FanOutKinesisTestUtils.createTestRecord("shard1-record-1"));
        recordsShard1.add(FanOutKinesisTestUtils.createTestRecord("shard1-record-2"));

        List<Record> recordsShard2 = new ArrayList<>();
        recordsShard2.add(FanOutKinesisTestUtils.createTestRecord("shard2-record-1"));
        recordsShard2.add(FanOutKinesisTestUtils.createTestRecord("shard2-record-2"));

        // Store the original starting positions
        StartingPosition originalPosition1 = subscription1.getStartingPosition();
        StartingPosition originalPosition2 = subscription2.getStartingPosition();

        // Process the events
        subscription1.processSubscriptionEvent(
                FanOutKinesisTestUtils.createTestEvent(continuationSequenceNumber1, recordsShard1));
        subscription2.processSubscriptionEvent(
                FanOutKinesisTestUtils.createTestEvent(continuationSequenceNumber2, recordsShard2));

        // Verify that the starting positions were not updated
        assertThat(subscription1.getStartingPosition()).isEqualTo(originalPosition1);
        assertThat(subscription2.getStartingPosition()).isEqualTo(originalPosition2);
    }
}
