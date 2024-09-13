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

import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;

import javax.annotation.Nullable;

import java.io.IOException;
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

    private final Deque<DynamoDbStreamsShardSplitState> assignedSplits = new ArrayDeque<>();
    private final Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap;
    private final Set<String> pausedSplitIds = new HashSet<>();

    public PollingDynamoDbStreamsShardSplitReader(
            StreamProxy dynamodbStreamsProxy,
            Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap) {
        this.dynamodbStreams = dynamodbStreamsProxy;
        this.shardMetricGroupMap = shardMetricGroupMap;
    }

    @Override
    public RecordsWithSplitIds<Record> fetch() throws IOException {
        DynamoDbStreamsShardSplitState splitState = assignedSplits.poll();
        if (splitState == null) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        if (pausedSplitIds.contains(splitState.getSplitId())) {
            assignedSplits.add(splitState);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        GetRecordsResponse getRecordsResponse =
                dynamodbStreams.getRecords(
                        splitState.getStreamArn(),
                        splitState.getShardId(),
                        splitState.getNextStartingPosition());
        boolean isComplete = getRecordsResponse.nextShardIterator() == null;

        if (hasNoRecords(getRecordsResponse)) {
            if (isComplete) {
                return new DynamoDbStreamRecordsWithSplitIds(
                        Collections.emptyIterator(), splitState.getSplitId(), true);
            } else {
                assignedSplits.add(splitState);
                return INCOMPLETE_SHARD_EMPTY_RECORDS;
            }
        } else {
            DynamoDbStreamsShardMetrics shardMetrics =
                    shardMetricGroupMap.get(splitState.getShardId());
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

        splitState.setNextStartingPosition(
                StartingPosition.continueFromSequenceNumber(
                        getRecordsResponse
                                .records()
                                .get(getRecordsResponse.records().size() - 1)
                                .dynamodb()
                                .sequenceNumber()));

        if (!isComplete) {
            assignedSplits.add(splitState);
        }
        return new DynamoDbStreamRecordsWithSplitIds(
                getRecordsResponse.records().iterator(), splitState.getSplitId(), isComplete);
    }

    private boolean hasNoRecords(GetRecordsResponse getRecordsResponse) {
        return !getRecordsResponse.hasRecords() || getRecordsResponse.records().isEmpty();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DynamoDbStreamsShardSplit> splitsChanges) {
        for (DynamoDbStreamsShardSplit split : splitsChanges.splits()) {
            assignedSplits.add(new DynamoDbStreamsShardSplitState(split));
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
}
