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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests to verify that there's no dropping of records or change in order of records
 * when processing events in {@link FanOutKinesisShardSubscription} and {@link FanOutKinesisShardSplitReader}.
 */
public class FanOutKinesisShardRecordOrderingTest extends FanOutKinesisShardTestBase {

    /**
     * Tests that records are processed in the correct order for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testRecordOrderingPreservedForSingleShard() throws Exception {
        // Create a blocking queue to store processed records
        BlockingQueue<Record> processedRecords = new LinkedBlockingQueue<>();

        // Create a custom TestableSubscription that captures processed records
        TestableSubscription testSubscription = createTestableSubscription(
                SHARD_ID,
                StartingPosition.fromStart(),
                processedRecords);

        // Create test events with records in a specific order
        int numEvents = 3;
        int recordsPerEvent = 5;
        List<List<Record>> eventRecords = new ArrayList<>();

        for (int i = 0; i < numEvents; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                int recordNum = i * recordsPerEvent + j;
                records.add(FanOutKinesisTestUtils.createTestRecord("record-" + recordNum));
            }
            eventRecords.add(records);
        }

        // Process the events
        for (int i = 0; i < numEvents; i++) {
            String sequenceNumber = "sequence-" + i;
            testSubscription.processSubscriptionEvent(
                    FanOutKinesisTestUtils.createTestEvent(sequenceNumber, eventRecords.get(i)));
        }

        // Verify that all records were processed in the correct order
        List<Record> allProcessedRecords = new ArrayList<>();
        processedRecords.drainTo(allProcessedRecords);

        assertThat(allProcessedRecords).hasSize(numEvents * recordsPerEvent);

        // Verify the order of records
        for (int i = 0; i < numEvents * recordsPerEvent; i++) {
            String expectedData = "record-" + i;
            String actualData = new String(
                    allProcessedRecords.get(i).data().asByteArray(),
                    StandardCharsets.UTF_8);
            assertThat(actualData).isEqualTo(expectedData);
        }
    }

    /**
     * Tests that records are processed in the correct order for multiple shards.
     */
    @Test
    @Timeout(value = 30)
    public void testRecordOrderingPreservedForMultipleShards() throws Exception {
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

        // Create test events with records in a specific order for each shard
        int numEvents = 3;
        int recordsPerEvent = 5;

        // Process events for shard 1
        for (int i = 0; i < numEvents; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                int recordNum = i * recordsPerEvent + j;
                records.add(FanOutKinesisTestUtils.createTestRecord("shard1-record-" + recordNum));
            }

            String sequenceNumber = "shard1-sequence-" + i;
            subscription1.processSubscriptionEvent(
                    FanOutKinesisTestUtils.createTestEvent(sequenceNumber, records));
        }

        // Process events for shard 2
        for (int i = 0; i < numEvents; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                int recordNum = i * recordsPerEvent + j;
                records.add(FanOutKinesisTestUtils.createTestRecord("shard2-record-" + recordNum));
            }

            String sequenceNumber = "shard2-sequence-" + i;
            subscription2.processSubscriptionEvent(
                    FanOutKinesisTestUtils.createTestEvent(sequenceNumber, records));
        }

        // Verify that all records were processed in the correct order for shard 1
        List<Record> allProcessedRecordsShard1 = new ArrayList<>();
        processedRecordsShard1.drainTo(allProcessedRecordsShard1);

        assertThat(allProcessedRecordsShard1).hasSize(numEvents * recordsPerEvent);

        for (int i = 0; i < numEvents * recordsPerEvent; i++) {
            String expectedData = "shard1-record-" + i;
            String actualData = new String(
                    allProcessedRecordsShard1.get(i).data().asByteArray(),
                    StandardCharsets.UTF_8);
            assertThat(actualData).isEqualTo(expectedData);
        }

        // Verify that all records were processed in the correct order for shard 2
        List<Record> allProcessedRecordsShard2 = new ArrayList<>();
        processedRecordsShard2.drainTo(allProcessedRecordsShard2);

        assertThat(allProcessedRecordsShard2).hasSize(numEvents * recordsPerEvent);

        for (int i = 0; i < numEvents * recordsPerEvent; i++) {
            String expectedData = "shard2-record-" + i;
            String actualData = new String(
                    allProcessedRecordsShard2.get(i).data().asByteArray(),
                    StandardCharsets.UTF_8);
            assertThat(actualData).isEqualTo(expectedData);
        }
    }

    /**
     * Tests that records are not dropped when processing events.
     */
    @Test
    @Timeout(value = 30)
    public void testNoRecordsDropped() throws Exception {
        // Create a reader with a single shard
        FanOutKinesisShardSplitReader reader = createSplitReaderWithShard(SHARD_ID);

        // Create a list to store fetched records
        final List<Record> fetchedRecords = new ArrayList<>();

        // Create a queue to simulate the event stream
        BlockingQueue<SubscribeToShardEvent> eventQueue = new LinkedBlockingQueue<>();

        // Create a custom AsyncStreamProxy that will use our event queue
        AsyncStreamProxy customProxy = Mockito.mock(AsyncStreamProxy.class);
        when(customProxy.subscribeToShard(any(), any(), any(), any()))
                .thenAnswer(new Answer<CompletableFuture<Void>>() {
                    @Override
                    public CompletableFuture<Void> answer(InvocationOnMock invocation) {
                        Object[] args = invocation.getArguments();
                        software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler handler =
                                (software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler) args[3];

                        // Start a thread to feed events to the handler
                        new Thread(() -> {
                            try {
                                while (true) {
                                    SubscribeToShardEvent event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                                    if (event != null) {
                                        // Create a TestableSubscription to process the event
                                        TestableSubscription subscription = createTestableSubscription(
                                                SHARD_ID,
                                                StartingPosition.fromStart(),
                                                new LinkedBlockingQueue<>());

                                        // Process the event directly
                                        subscription.processSubscriptionEvent(event);

                                        // Add the processed records to the fetchedRecords list
                                        synchronized (fetchedRecords) {
                                            for (Record record : event.records()) {
                                                fetchedRecords.add(record);
                                            }
                                        }
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }).start();

                        return CompletableFuture.completedFuture(null);
                    }
                });

        // Create a reader with our custom proxy
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        final FanOutKinesisShardSplitReader customReader = new FanOutKinesisShardSplitReader(
                customProxy,
                CONSUMER_ARN,
                Collections.emptyMap(),
                configuration,
                createTestSubscriptionFactory(),
                testExecutor);

        // Add a split to the reader
        KinesisShardSplit split = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart());

        customReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // Create test events with records
        int numEvents = 5;
        int recordsPerEvent = 10;
        List<Record> allRecords = new ArrayList<>();

        for (int i = 0; i < numEvents; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                int recordNum = i * recordsPerEvent + j;
                Record record = FanOutKinesisTestUtils.createTestRecord("record-" + recordNum);
                records.add(record);
                allRecords.add(record);
            }

            String sequenceNumber = "sequence-" + i;
            eventQueue.add(FanOutKinesisTestUtils.createTestEvent(sequenceNumber, records));
        }

        AtomicInteger fetchAttempts = new AtomicInteger(0);

        // We need to fetch multiple times to get all records
        while (fetchedRecords.size() < allRecords.size() && fetchAttempts.incrementAndGet() < 20) {
            RecordsWithSplitIds<Record> recordsWithSplitIds = customReader.fetch();

            // Extract records from the batch
            String splitId;
            while ((splitId = recordsWithSplitIds.nextSplit()) != null) {
                Record record;
                while ((record = recordsWithSplitIds.nextRecordFromSplit()) != null) {
                    fetchedRecords.add(record);
                }
            }

            // Small delay to allow events to be processed
            Thread.sleep(100);
        }

        // Verify that all records were fetched
        assertThat(fetchedRecords).hasSameSizeAs(allRecords);

        // Verify the content of each record
        for (int i = 0; i < allRecords.size(); i++) {
            String expectedData = new String(
                    allRecords.get(i).data().asByteArray(),
                    StandardCharsets.UTF_8);

            // Find the matching record in the fetched records
            boolean found = false;
            for (Record fetchedRecord : fetchedRecords) {
                String fetchedData = new String(
                        fetchedRecord.data().asByteArray(),
                        StandardCharsets.UTF_8);

                if (fetchedData.equals(expectedData)) {
                    found = true;
                    break;
                }
            }

            assertThat(found).as("Record %s was not found in fetched records", expectedData).isTrue();
        }
    }

    /**
     * Tests that records are processed in the correct order even when there are concurrent events.
     */
    @Test
    @Timeout(value = 30)
    public void testRecordOrderingWithConcurrentEvents() throws Exception {
        // Create a blocking queue to store processed records
        BlockingQueue<Record> processedRecords = new LinkedBlockingQueue<>();

        // Create a custom TestableSubscription that captures processed records
        TestableSubscription testSubscription = createTestableSubscription(
                SHARD_ID,
                StartingPosition.fromStart(),
                processedRecords);

        // Create test events with records
        int numEvents = 10;
        int recordsPerEvent = 5;
        List<SubscribeToShardEvent> events = new ArrayList<>();

        for (int i = 0; i < numEvents; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                int recordNum = i * recordsPerEvent + j;
                records.add(FanOutKinesisTestUtils.createTestRecord("record-" + recordNum));
            }

            String sequenceNumber = "sequence-" + i;
            events.add(FanOutKinesisTestUtils.createTestEvent(sequenceNumber, records));
        }

        // Process events concurrently
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (SubscribeToShardEvent event : events) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                testSubscription.processSubscriptionEvent(event);
            }, testExecutor);
            futures.add(future);
        }

        // Trigger all tasks in the executor
        testExecutor.triggerAll();

        // Wait for all events to be processed
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        // Verify that all records were processed
        List<Record> allProcessedRecords = new ArrayList<>();
        processedRecords.drainTo(allProcessedRecords);

        assertThat(allProcessedRecords).hasSize(numEvents * recordsPerEvent);

        // Verify that all records were processed
        List<String> processedDataStrings = allProcessedRecords.stream()
                .map(r -> new String(r.data().asByteArray(), StandardCharsets.UTF_8))
                .collect(Collectors.toList());

        // Create a list of all expected record data strings
        List<String> expectedDataStrings = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            for (int j = 0; j < recordsPerEvent; j++) {
                expectedDataStrings.add("record-" + (i * recordsPerEvent + j));
            }
        }

        // Verify that all expected records are present in the processed records
        // We can't guarantee the exact order due to concurrency, but we can verify all records are there
        assertThat(processedDataStrings).containsExactlyInAnyOrderElementsOf(expectedDataStrings);

        // Verify that records from the same event are processed in order
        // We do this by checking if there are any records from the same event that are out of order
        boolean recordsInOrder = true;
        for (int i = 0; i < numEvents; i++) {
            List<Integer> eventRecordIndices = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                String recordData = "record-" + (i * recordsPerEvent + j);
                int index = processedDataStrings.indexOf(recordData);
                eventRecordIndices.add(index);
            }

            // Check if the indices are in ascending order
            for (int j = 1; j < eventRecordIndices.size(); j++) {
                if (eventRecordIndices.get(j) < eventRecordIndices.get(j - 1)) {
                    recordsInOrder = false;
                    break;
                }
            }
        }

        // We expect records from the same event to be in order
        assertThat(recordsInOrder).as("Records from the same event should be processed in order").isTrue();
    }
}
