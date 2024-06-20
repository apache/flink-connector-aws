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
import org.apache.flink.connector.dynamodb.source.proxy.StreamProxy;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy;
import org.apache.flink.connector.dynamodb.source.util.TestUtil;

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
    @Test
    void testNoAssignedSplitsHandledGracefully() throws Exception {
        StreamProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).isEmpty();
    }

    @Test
    void testAssignedSplitHasNoRecordsHandledGracefully() throws Exception {
        TestDynamoDbStreamsProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

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
        TestDynamoDbStreamsProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

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
        TestDynamoDbStreamsProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

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
        TestDynamoDbStreamsProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

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
        TestDynamoDbStreamsProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

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
        TestDynamoDbStreamsProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

        assertThatNoException().isThrownBy(splitReader::wakeUp);
    }

    @Test
    void testCloseClosesStreamProxy() {
        TestDynamoDbStreamsProxy testStreamProxy = getTestStreamProxy();
        PollingDynamoDbStreamsShardSplitReader splitReader =
                new PollingDynamoDbStreamsShardSplitReader(testStreamProxy);

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
