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

package org.apache.flink.connector.dynamodb.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy;
import org.apache.flink.connector.dynamodb.source.util.TestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestRecord;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class PollingDynamoDbStreamsShardSplitReaderTest {
    private PollingDynamoDbStreamsShardSplitReader splitReader;
    private TestDynamoDbStreamsProxy testStreamProxy;

    @BeforeEach
    public void init() {
        testStreamProxy = getTestStreamProxy();
        splitReader = new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);
    }

    @Test
    void testNoAssignedSplitsHandledGracefully() throws Exception {
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).isEmpty();
    }

    @Test
    void testAssignedSplitHasNoRecordsHandledGracefully() throws Exception {
        // Given assigned split with no records
        String shardId = generateShardId(1);
        testStreamProxy.addShards(shardId);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(shardId))));

        // When fetching records
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Then retrieve no records
        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).isEmpty();
    }

    @Test
    void testSingleAssignedSplitAllConsumed() throws Exception {
        // Given assigned split with records
        String shardId = generateShardId(1);
        testStreamProxy.addShards(shardId);
        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, shardId, Collections.singletonList(expectedRecords.get(0)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, shardId, Collections.singletonList(expectedRecords.get(1)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, shardId, Collections.singletonList(expectedRecords.get(2)));
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(shardId))));

        // When fetching records
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < expectedRecords.size(); i++) {
            RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
            records.addAll(readAllRecords(retrievedRecords));
        }

        assertThat(records).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    @Test
    void testMultipleAssignedSplitsAllConsumed() throws Exception {
        // Given assigned split with records
        String shardId = generateShardId(1);
        String shardId2 = generateShardId(2);
        testStreamProxy.addShards(shardId, shardId2);
        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, shardId, Collections.singletonList(expectedRecords.get(0)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, shardId, Collections.singletonList(expectedRecords.get(1)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, shardId2, Collections.singletonList(expectedRecords.get(2)));
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Arrays.asList(getTestSplit(shardId), getTestSplit(shardId2))));

        // When records are fetched
        List<Record> fetchedRecords = new ArrayList<>();
        for (int i = 0; i < expectedRecords.size(); i++) {
            RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
            fetchedRecords.addAll(readAllRecords(retrievedRecords));
        }

        // Then all records are fetched
        assertThat(fetchedRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    @Test
    void testHandleEmptyCompletedShard() throws Exception {
        // Given assigned split with no records, and the shard is complete
        String shardId = generateShardId(1);
        testStreamProxy.addShards(shardId);
        testStreamProxy.addRecords(TestUtil.STREAM_ARN, shardId, Collections.emptyList());
        DynamoDbStreamsShardSplit split = getTestSplit(shardId);
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
        testStreamProxy.setShouldCompleteNextShard(true);

        // When fetching records
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Returns completed split with no records
        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).contains(split.splitId());
    }

    @Test
    void testFinishedSplitsReturned() throws Exception {
        // Given assigned split with records from completed shard
        String shardId = generateShardId(1);
        testStreamProxy.addShards(shardId);
        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(TestUtil.STREAM_ARN, shardId, expectedRecords);
        DynamoDbStreamsShardSplit split = getTestSplit(shardId);
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // When fetching records
        List<Record> fetchedRecords = new ArrayList<>();
        testStreamProxy.setShouldCompleteNextShard(true);
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Then records can be read successfully, with finishedSplit returned once all records are
        // completed
        for (int i = 0; i < expectedRecords.size(); i++) {
            assertThat(retrievedRecords.nextSplit()).isEqualTo(split.splitId());
            assertThat(retrievedRecords.finishedSplits()).isEmpty();
            fetchedRecords.add(retrievedRecords.nextRecordFromSplit());
        }
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).contains(split.splitId());
        assertThat(fetchedRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    @Test
    void testWakeUpIsNoOp() {
        assertThatNoException().isThrownBy(splitReader::wakeUp);
    }

    @Test
    void testPauseOrResumeSplits() throws Exception {
        String splitId = generateShardId(1);

        testStreamProxy.addShards(splitId);
        DynamoDbStreamsShardSplit testSplit = getTestSplit(splitId);

        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, splitId, Collections.singletonList(expectedRecords.get(0)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN, splitId, Collections.singletonList(expectedRecords.get(1)));
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(testSplit)));

        // read data from split
        RecordsWithSplitIds<Record> records = splitReader.fetch();
        assertThat(readAllRecords(records)).containsExactlyInAnyOrder(expectedRecords.get(0));

        // pause split
        splitReader.pauseOrResumeSplits(
                Collections.singletonList(testSplit), Collections.emptyList());
        records = splitReader.fetch();
        // returns incomplete split with no records
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(records.nextSplit()).isNull();
        assertThat(records.nextRecordFromSplit()).isNull();

        // resume split
        splitReader.pauseOrResumeSplits(
                Collections.emptyList(), Collections.singletonList(testSplit));
        records = splitReader.fetch();
        assertThat(readAllRecords(records)).containsExactlyInAnyOrder(expectedRecords.get(1));
    }

    @Test
    void testPauseOrResumeSplitsOnlyPauseReadsFromSpecifiedSplits() throws Exception {
        DynamoDbStreamsShardSplit testSplit1 = getTestSplit(generateShardId(1));
        DynamoDbStreamsShardSplit testSplit2 = getTestSplit(generateShardId(2));
        DynamoDbStreamsShardSplit testSplit3 = getTestSplit(generateShardId(3));

        testStreamProxy.addShards(testSplit1.splitId(), testSplit2.splitId(), testSplit3.splitId());

        List<Record> recordsFromSplit1 =
                Arrays.asList(getTestRecord("split-1-data-1"), getTestRecord("split-1-data-2"));
        List<Record> recordsFromSplit2 =
                Arrays.asList(getTestRecord("split-2-data-1"), getTestRecord("split-2-data-2"));
        List<Record> recordsFromSplit3 =
                Arrays.asList(getTestRecord("split-3-data-1"), getTestRecord("split-3-data-2"));

        recordsFromSplit1.forEach(
                record ->
                        testStreamProxy.addRecords(
                                TestUtil.STREAM_ARN,
                                testSplit1.getShardId(),
                                Collections.singletonList(record)));
        recordsFromSplit2.forEach(
                record ->
                        testStreamProxy.addRecords(
                                TestUtil.STREAM_ARN,
                                testSplit2.getShardId(),
                                Collections.singletonList(record)));
        recordsFromSplit3.forEach(
                record ->
                        testStreamProxy.addRecords(
                                TestUtil.STREAM_ARN,
                                testSplit3.getShardId(),
                                Collections.singletonList(record)));

        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Arrays.asList(testSplit1, testSplit2, testSplit3)));

        // pause split 1 and split 3
        splitReader.pauseOrResumeSplits(
                Arrays.asList(testSplit1, testSplit3), Collections.emptyList());

        // read data from splits and verify that only records from split 2 were fetched by reader
        List<Record> fetchedRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            RecordsWithSplitIds<Record> records = splitReader.fetch();
            fetchedRecords.addAll(readAllRecords(records));
        }
        assertThat(fetchedRecords).containsExactly(recordsFromSplit2.toArray(new Record[0]));

        // resume split 3
        splitReader.pauseOrResumeSplits(
                Collections.emptyList(), Collections.singletonList(testSplit3));

        // read data from splits and verify that only records from split 3 had been read
        fetchedRecords.clear();
        for (int i = 0; i < 10; i++) {
            RecordsWithSplitIds<Record> records = splitReader.fetch();
            fetchedRecords.addAll(readAllRecords(records));
        }
        assertThat(fetchedRecords).containsExactly(recordsFromSplit3.toArray(new Record[0]));
    }

    @Test
    void testCloseClosesStreamProxy() {
        assertThatNoException().isThrownBy(splitReader::close);
        assertThat(testStreamProxy.isClosed()).isTrue();
    }

    private List<Record> readAllRecords(RecordsWithSplitIds<Record> recordsWithSplitIds) {
        List<Record> outputRecords = new ArrayList<>();
        Record record;
        do {
            record = recordsWithSplitIds.nextRecordFromSplit();
            if (record != null) {
                outputRecords.add(record);
            }
        } while (record != null);

        return outputRecords;
    }
}
