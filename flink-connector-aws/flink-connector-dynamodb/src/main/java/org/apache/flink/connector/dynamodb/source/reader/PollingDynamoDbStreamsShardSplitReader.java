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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.dynamodb.source.metrics.DynamoDbStreamsShardMetrics;
import org.apache.flink.connector.dynamodb.source.proxy.StreamProxy;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * An implementation of the SplitReader that periodically polls the DynamoDb stream to retrieve
 * records.
 */
@Internal
public class PollingDynamoDbStreamsShardSplitReader
        implements SplitReader<Record, DynamoDbStreamsShardSplit> {

    private static final RecordsWithSplitIds<Record> INCOMPLETE_SHARD_EMPTY_RECORDS =
            new DynamoDbStreamRecordsWithSplitIds(Collections.emptyIterator(), null, false);

    private final StreamProxy dynamodbStreams;
    private final Duration getRecordsIdlePollingTimeBetweenNonEmptyPolls;
    private final Duration getRecordsIdlePollingTimeBetweenEmptyPolls;

    private final Deque<DynamoDbStreamsShardSplitWithContext> assignedSplits;
    private final Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap;
    private final Set<String> pausedSplitIds;
    private static final Logger LOG =
            LoggerFactory.getLogger(PollingDynamoDbStreamsShardSplitReader.class);

    public PollingDynamoDbStreamsShardSplitReader(
            StreamProxy dynamodbStreamsProxy,
            Duration getRecordsIdlePollingTimeBetweenNonEmptyPolls,
            Duration getRecordsIdlePollingTimeBetweenEmptyPolls,
            Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap) {
        this.dynamodbStreams = dynamodbStreamsProxy;
        this.getRecordsIdlePollingTimeBetweenNonEmptyPolls =
                getRecordsIdlePollingTimeBetweenNonEmptyPolls;
        this.getRecordsIdlePollingTimeBetweenEmptyPolls =
                getRecordsIdlePollingTimeBetweenEmptyPolls;
        this.shardMetricGroupMap = shardMetricGroupMap;
        this.assignedSplits = new ArrayDeque<>();
        this.pausedSplitIds = new HashSet<>();
    }

    private long getNextEligibleTime(DynamoDbStreamsShardSplitWithContext splitContext) {
        long requiredDelay =
                splitContext.wasLastPollEmpty
                        ? getRecordsIdlePollingTimeBetweenEmptyPolls.toMillis()
                        : getRecordsIdlePollingTimeBetweenNonEmptyPolls.toMillis();

        return splitContext.lastPollTimeMillis + requiredDelay;
    }

    @Override
    public RecordsWithSplitIds<Record> fetch() throws IOException {
        if (assignedSplits.isEmpty()) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }
        DynamoDbStreamsShardSplitWithContext splitContext = assignedSplits.poll();

        if (pausedSplitIds.contains(splitContext.splitState.getSplitId())) {
            assignedSplits.add(splitContext);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        long currentTime = System.currentTimeMillis();
        long nextEligibleTime = getNextEligibleTime(splitContext);

        LOG.debug(
                "Polling split: {}, currentTime: {}, eligibleTime: {}, wasEmptyPoll: {}",
                splitContext.splitState.getSplitId(),
                currentTime,
                nextEligibleTime,
                splitContext.wasLastPollEmpty);

        // Check if split is not ready due to empty poll and non-empty poll delay
        if (nextEligibleTime > currentTime) {
            assignedSplits.add(splitContext);
            sleep(1);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        GetRecordsResponse getRecordsResponse =
                dynamodbStreams.getRecords(
                        splitContext.splitState.getStreamArn(),
                        splitContext.splitState.getShardId(),
                        splitContext.splitState.getNextStartingPosition());
        boolean isComplete = getRecordsResponse.nextShardIterator() == null;
        boolean isEmptyPoll = hasNoRecords(getRecordsResponse);

        splitContext.lastPollTimeMillis = currentTime;
        splitContext.wasLastPollEmpty = isEmptyPoll;

        if (isEmptyPoll) {
            if (isComplete) {
                return new DynamoDbStreamRecordsWithSplitIds(
                        Collections.emptyIterator(), splitContext.splitState.getSplitId(), true);
            } else {
                assignedSplits.add(splitContext);
                return INCOMPLETE_SHARD_EMPTY_RECORDS;
            }
        } else {
            DynamoDbStreamsShardMetrics shardMetrics =
                    shardMetricGroupMap.get(splitContext.splitState.getShardId());
            Record lastRecord =
                    getRecordsResponse.records().get(getRecordsResponse.records().size() - 1);
            shardMetrics.setMillisBehindLatest(
                    Math.max(
                            System.currentTimeMillis()
                                    - lastRecord
                                            .dynamodb()
                                            .approximateCreationDateTime()
                                            .toEpochMilli(),
                            0));
        }

        splitContext.splitState.setNextStartingPosition(
                StartingPosition.continueFromSequenceNumber(
                        getRecordsResponse
                                .records()
                                .get(getRecordsResponse.records().size() - 1)
                                .dynamodb()
                                .sequenceNumber()));

        if (!isComplete) {
            assignedSplits.add(splitContext);
        }
        return new DynamoDbStreamRecordsWithSplitIds(
                getRecordsResponse.records().iterator(),
                splitContext.splitState.getSplitId(),
                isComplete);
    }

    private void sleep(long milliseconds) throws IOException {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new IOException("Split reader was interrupted while sleeping", e);
        }
    }

    private boolean hasNoRecords(GetRecordsResponse getRecordsResponse) {
        return !getRecordsResponse.hasRecords() || getRecordsResponse.records().isEmpty();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DynamoDbStreamsShardSplit> splitsChanges) {
        for (DynamoDbStreamsShardSplit split : splitsChanges.splits()) {
            assignedSplits.add(
                    new DynamoDbStreamsShardSplitWithContext(
                            new DynamoDbStreamsShardSplitState(split)));
        }
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<DynamoDbStreamsShardSplit> splitsToPause,
            Collection<DynamoDbStreamsShardSplit> splitsToResume) {
        splitsToPause.forEach(split -> pausedSplitIds.add(split.splitId()));
        splitsToResume.forEach(split -> pausedSplitIds.remove(split.splitId()));
    }

    @Override
    public void wakeUp() {
        // Do nothing because we don't have any sleep mechanism
    }

    @Override
    public void close() throws Exception {
        dynamodbStreams.close();
    }

    private static class DynamoDbStreamRecordsWithSplitIds implements RecordsWithSplitIds<Record> {

        private final Iterator<Record> recordsIterator;
        private final String splitId;
        private final boolean isComplete;

        public DynamoDbStreamRecordsWithSplitIds(
                Iterator<Record> recordsIterator, String splitId, boolean isComplete) {
            this.recordsIterator = recordsIterator;
            this.splitId = splitId;
            this.isComplete = isComplete;
        }

        @Nullable
        @Override
        public String nextSplit() {
            return recordsIterator.hasNext() ? splitId : null;
        }

        @Nullable
        @Override
        public Record nextRecordFromSplit() {
            return recordsIterator.hasNext() ? recordsIterator.next() : null;
        }

        @Override
        public Set<String> finishedSplits() {
            if (splitId == null) {
                return Collections.emptySet();
            }
            if (recordsIterator.hasNext()) {
                return Collections.emptySet();
            }
            return isComplete ? singleton(splitId) : Collections.emptySet();
        }
    }

    @Internal
    private static class DynamoDbStreamsShardSplitWithContext {
        final DynamoDbStreamsShardSplitState splitState;
        long lastPollTimeMillis;
        boolean wasLastPollEmpty;

        DynamoDbStreamsShardSplitWithContext(DynamoDbStreamsShardSplitState splitState) {
            this.splitState = splitState;
            this.lastPollTimeMillis = System.currentTimeMillis();
            this.wasLastPollEmpty = false;
        }
    }
}
