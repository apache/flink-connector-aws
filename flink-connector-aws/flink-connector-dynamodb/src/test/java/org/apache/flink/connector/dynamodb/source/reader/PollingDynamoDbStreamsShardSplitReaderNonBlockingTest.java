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
import org.apache.flink.connector.dynamodb.source.metrics.DynamoDbStreamsShardMetrics;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy;
import org.apache.flink.connector.dynamodb.source.util.ListShardsResult;
import org.apache.flink.connector.dynamodb.source.util.TestUtil;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardFilter;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_GET_RECORDS_MAX_IN_FLIGHT;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.NON_BLOCKING_FETCH_MAX_WAIT;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestRecord;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the non-blocking calls of {@link PollingDynamoDbStreamsShardSplitReader} with a proxy whose
 * calls complete only when the test completes them.
 */
class PollingDynamoDbStreamsShardSplitReaderNonBlockingTest {

    /** With no delay, a shard is ready again as soon as the reader collects its response. */
    private static final Duration NO_DELAY = Duration.ZERO;

    private ControllableStreamProxy streamProxy;
    private Map<String, List<Shard>> childShardMap;
    private Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap;
    private MetricListener metricListener;

    @BeforeEach
    void init() {
        streamProxy = new ControllableStreamProxy();
        childShardMap = new ConcurrentHashMap<>();
        shardMetricGroupMap = new ConcurrentHashMap<>();
        metricListener = new MetricListener();
    }

    @Test
    void testStartsOneCallForEachReadyShard() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY);
        List<DynamoDbStreamsShardSplit> splits = addSplits(splitReader, 20);

        assertThat(readAllRecords(splitReader.fetch())).isEmpty();
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(20);

        // All calls are in progress, so the reader starts no new call.
        assertThat(readAllRecords(splitReader.fetch())).isEmpty();
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(20);

        String shardId = splits.get(0).getShardId();
        streamProxy.completeGetRecords(shardId, getTestRecord("data-1"));
        assertThat(readAllRecords(splitReader.fetch())).hasSize(1);

        // Only the shard whose response the reader collected gets a new call.
        splitReader.fetch();
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(21);
        assertThat(streamProxy.getRecordsCalls).containsKey(shardId);
    }

    @Test
    void testSlowCallDoesNotDelayOtherShards() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY);
        List<DynamoDbStreamsShardSplit> splits = addSplits(splitReader, 3);
        splitReader.fetch();

        // The call on the first shard does not complete.
        streamProxy.completeGetRecords(splits.get(1).getShardId(), getTestRecord("data-2"));
        streamProxy.completeGetRecords(splits.get(2).getShardId(), getTestRecord("data-3"));

        List<String> splitIds = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            RecordsWithSplitIds<Record> records = splitReader.fetch();
            splitIds.add(records.nextSplit());
            assertThat(readAllRecords(records)).hasSize(1);
        }
        assertThat(splitIds)
                .containsExactlyInAnyOrder(splits.get(1).splitId(), splits.get(2).splitId());

        splitReader.fetch();
        assertThat(streamProxy.getRecordsCalls.keySet())
                .containsExactlyInAnyOrder(
                        splits.get(0).getShardId(),
                        splits.get(1).getShardId(),
                        splits.get(2).getShardId());
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(5);
    }

    @Test
    void testNextCallStartsAfterLastRecordOfCollectedResponse() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY);
        String shardId = addSplits(splitReader, 1).get(0).getShardId();
        Record record = getTestRecord("data-1");

        splitReader.fetch();
        streamProxy.completeGetRecords(shardId, record);
        assertThat(readAllRecords(splitReader.fetch())).containsExactly(record);
        splitReader.fetch();

        assertThat(streamProxy.startingPositions).hasSize(2);
        StartingPosition nextPosition = streamProxy.startingPositions.get(1);
        assertThat(nextPosition.getShardIteratorType())
                .isEqualTo(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        assertThat(nextPosition.getStartingMarker()).isEqualTo(record.dynamodb().sequenceNumber());
    }

    @Test
    void testResponseOfPausedSplitIsHeldUntilResume() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY);
        DynamoDbStreamsShardSplit split = addSplits(splitReader, 1).get(0);
        Record record = getTestRecord("data-1");

        splitReader.fetch();
        splitReader.pauseOrResumeSplits(Collections.singletonList(split), Collections.emptyList());
        streamProxy.completeGetRecords(split.getShardId(), record);

        // The reader holds the response and starts no new call for the paused split.
        assertThat(readAllRecords(splitReader.fetch())).isEmpty();
        assertThat(readAllRecords(splitReader.fetch())).isEmpty();
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(1);

        splitReader.pauseOrResumeSplits(Collections.emptyList(), Collections.singletonList(split));
        assertThat(readAllRecords(splitReader.fetch())).containsExactly(record);
        splitReader.fetch();
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(2);
    }

    @Test
    void testFailedCallFailsFetch() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY);
        String shardId = addSplits(splitReader, 1).get(0).getShardId();

        splitReader.fetch();
        streamProxy.failGetRecords(
                shardId, new CompletionException(new IllegalStateException("call failed")));

        assertThatThrownBy(splitReader::fetch)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("call failed");
    }

    @Test
    void testFailedCallOfPausedSplitFailsFetch() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY);
        DynamoDbStreamsShardSplit split = addSplits(splitReader, 1).get(0);

        splitReader.fetch();
        splitReader.pauseOrResumeSplits(Collections.singletonList(split), Collections.emptyList());
        streamProxy.failGetRecords(split.getShardId(), new IllegalStateException("call failed"));

        assertThatThrownBy(splitReader::fetch)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("call failed");
    }

    @Test
    void testChildShardDiscoveryDoesNotDelayOtherShards() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY);
        List<DynamoDbStreamsShardSplit> splits = addSplits(splitReader, 2);
        DynamoDbStreamsShardSplit closedSplit = splits.get(0);
        splitReader.fetch();

        // The first shard ends. The call on the second shard does not complete yet.
        streamProxy.completeShardEnd(closedSplit.getShardId(), getTestRecord("data-1"));
        assertThat(readAllRecords(splitReader.fetch())).hasSize(1);

        splitReader.fetch();
        assertThat(streamProxy.childShardCalls).containsKey(closedSplit.getShardId());

        Shard childShard =
                Shard.builder()
                        .shardId(TestUtil.generateShardId(100))
                        .parentShardId(closedSplit.getShardId())
                        .build();
        streamProxy.completeChildShardDiscovery(closedSplit.getShardId(), childShard);
        RecordsWithSplitIds<Record> finished = splitReader.fetch();
        assertThat(readAllRecords(finished)).isEmpty();
        assertThat(finished.finishedSplits()).containsExactly(closedSplit.splitId());
        assertThat(childShardMap.get(closedSplit.splitId())).containsExactly(childShard);

        streamProxy.completeGetRecords(splits.get(1).getShardId(), getTestRecord("data-2"));
        assertThat(readAllRecords(splitReader.fetch())).hasSize(1);
    }

    @Test
    void testStartsNoMoreThanMaxCallsInFlight() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY, 2);
        List<DynamoDbStreamsShardSplit> splits = addSplits(splitReader, 5);

        splitReader.fetch();
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(2);

        streamProxy.completeGetRecords(splits.get(0).getShardId(), getTestRecord("data-1"));
        assertThat(readAllRecords(splitReader.fetch())).hasSize(1);

        // Collecting the response frees one call.
        splitReader.fetch();
        assertThat(streamProxy.startedGetRecordsCalls).isEqualTo(3);
        assertThat(streamProxy.getRecordsCalls).hasSize(2);
    }

    @Test
    void testWaitingShardsGetFreeCallsFirst() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY, 2);
        List<String> shardIds =
                addSplits(splitReader, 4).stream()
                        .map(DynamoDbStreamsShardSplit::getShardId)
                        .collect(Collectors.toList());

        splitReader.fetch();
        for (int i = 0; i < 2; i++) {
            streamProxy.completeGetRecords(shardIds.get(i), getTestRecord("data-" + i));
            splitReader.fetch();
            splitReader.fetch();
        }

        // The first two shards are ready again when their responses are collected, but the
        // shards that were waiting start first.
        assertThat(streamProxy.startedShardIds).containsExactlyElementsOf(shardIds);
    }

    @Test
    void testHeldCallOfPausedSplitFreesItsCall() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader = createSplitReader(NO_DELAY, 1);
        List<DynamoDbStreamsShardSplit> splits = addSplits(splitReader, 2);
        DynamoDbStreamsShardSplit pausedSplit = splits.get(0);
        String otherShardId = splits.get(1).getShardId();

        splitReader.fetch();
        splitReader.pauseOrResumeSplits(
                Collections.singletonList(pausedSplit), Collections.emptyList());
        streamProxy.completeGetRecords(pausedSplit.getShardId(), getTestRecord("data-1"));
        // The reader holds the response of the paused split, and the other split gets the call.
        assertThat(readAllRecords(splitReader.fetch())).isEmpty();
        splitReader.fetch();
        assertThat(streamProxy.startedShardIds)
                .containsExactly(pausedSplit.getShardId(), otherShardId);

        // Collecting the held response after resume does not free a second call.
        splitReader.pauseOrResumeSplits(
                Collections.emptyList(), Collections.singletonList(pausedSplit));
        assertThat(readAllRecords(splitReader.fetch())).hasSize(1);
        splitReader.fetch();
        assertThat(streamProxy.startedShardIds)
                .containsExactly(pausedSplit.getShardId(), otherShardId);

        streamProxy.completeGetRecords(otherShardId, getTestRecord("data-2"));
        assertThat(readAllRecords(splitReader.fetch())).hasSize(1);
        splitReader.fetch();
        assertThat(streamProxy.startedShardIds)
                .containsExactly(pausedSplit.getShardId(), otherShardId, pausedSplit.getShardId());
    }

    @Test
    void testMaxCallsInFlightMustBePositive() {
        assertThatThrownBy(() -> createSplitReader(NO_DELAY, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWakeUpReturnsWaitingFetch() throws Exception {
        PollingDynamoDbStreamsShardSplitReader splitReader =
                createSplitReader(Duration.ofMinutes(10));
        addSplits(splitReader, 1);

        splitReader.wakeUp();
        long startNanos = System.nanoTime();
        assertThat(readAllRecords(splitReader.fetch())).isEmpty();
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

        assertThat(elapsedMillis).isLessThan(NON_BLOCKING_FETCH_MAX_WAIT.toMillis());
        assertThat(streamProxy.startedGetRecordsCalls).isZero();
    }

    private PollingDynamoDbStreamsShardSplitReader createSplitReader(Duration pollingDelay) {
        return createSplitReader(
                pollingDelay, DYNAMODB_STREAMS_GET_RECORDS_MAX_IN_FLIGHT.defaultValue());
    }

    private PollingDynamoDbStreamsShardSplitReader createSplitReader(
            Duration pollingDelay, int maxCallsInFlight) {
        return new PollingDynamoDbStreamsShardSplitReader(
                streamProxy,
                pollingDelay,
                pollingDelay,
                childShardMap,
                shardMetricGroupMap,
                false,
                maxCallsInFlight);
    }

    private List<DynamoDbStreamsShardSplit> addSplits(
            PollingDynamoDbStreamsShardSplitReader splitReader, int count) {
        List<DynamoDbStreamsShardSplit> splits =
                IntStream.rangeClosed(1, count)
                        .mapToObj(i -> getTestSplit(TestUtil.generateShardId(i)))
                        .collect(Collectors.toList());
        for (DynamoDbStreamsShardSplit split : splits) {
            shardMetricGroupMap.put(
                    split.splitId(),
                    new DynamoDbStreamsShardMetrics(split, metricListener.getMetricGroup()));
        }
        splitReader.handleSplitsChanges(new SplitsAddition<>(splits));
        return splits;
    }

    private static List<Record> readAllRecords(RecordsWithSplitIds<Record> recordsWithSplitIds) {
        List<Record> outputRecords = new ArrayList<>();
        Record record;
        while ((record = recordsWithSplitIds.nextRecordFromSplit()) != null) {
            outputRecords.add(record);
        }
        return outputRecords;
    }

    /** A proxy whose non-blocking calls complete only when the test completes them. */
    private static final class ControllableStreamProxy extends TestDynamoDbStreamsProxy {
        private final Map<String, CompletableFuture<GetRecordsResponse>> getRecordsCalls =
                new LinkedHashMap<>();
        private final Map<String, CompletableFuture<ListShardsResult>> childShardCalls =
                new LinkedHashMap<>();
        private final List<StartingPosition> startingPositions = new ArrayList<>();
        private final List<String> startedShardIds = new ArrayList<>();
        private int startedGetRecordsCalls;

        @Override
        public CompletableFuture<GetRecordsResponse> getRecordsAsync(
                String streamArn, String shardId, StartingPosition startingPosition) {
            // Each shard has at most one call in progress.
            assertThat(getRecordsCalls).doesNotContainKey(shardId);
            startedGetRecordsCalls++;
            startingPositions.add(startingPosition);
            startedShardIds.add(shardId);
            CompletableFuture<GetRecordsResponse> call = new CompletableFuture<>();
            getRecordsCalls.put(shardId, call);
            return call;
        }

        @Override
        public CompletableFuture<ListShardsResult> listShardsWithFilterAsync(
                String streamArn, ShardFilter shardFilter) {
            assertThat(childShardCalls).doesNotContainKey(shardFilter.shardId());
            CompletableFuture<ListShardsResult> call = new CompletableFuture<>();
            childShardCalls.put(shardFilter.shardId(), call);
            return call;
        }

        void completeGetRecords(String shardId, Record record) {
            getRecordsCalls
                    .remove(shardId)
                    .complete(
                            GetRecordsResponse.builder()
                                    .records(record)
                                    .nextShardIterator("next-shard-iterator")
                                    .build());
        }

        void completeShardEnd(String shardId, Record record) {
            getRecordsCalls
                    .remove(shardId)
                    .complete(
                            GetRecordsResponse.builder()
                                    .records(record)
                                    .nextShardIterator(null)
                                    .build());
        }

        void failGetRecords(String shardId, Throwable error) {
            getRecordsCalls.remove(shardId).completeExceptionally(error);
        }

        void completeChildShardDiscovery(String shardId, Shard childShard) {
            ListShardsResult result = new ListShardsResult();
            result.addShards(Collections.singletonList(childShard));
            result.setStreamStatus(StreamStatus.ENABLED);
            childShardCalls.remove(shardId).complete(result);
        }
    }
}
