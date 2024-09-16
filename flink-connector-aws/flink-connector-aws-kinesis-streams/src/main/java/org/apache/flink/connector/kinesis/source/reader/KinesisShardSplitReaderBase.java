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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;

/** Base implementation of the SplitReader for reading from KinesisShardSplits. */
@Internal
public abstract class KinesisShardSplitReaderBase
        implements SplitReader<Record, KinesisShardSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardSplitReaderBase.class);
    private static final RecordsWithSplitIds<Record> INCOMPLETE_SHARD_EMPTY_RECORDS =
            new KinesisRecordsWithSplitIds(Collections.emptyIterator(), null, false);

    private final Deque<KinesisShardSplitState> assignedSplits = new ArrayDeque<>();
    private final Set<String> pausedSplitIds = new HashSet<>();
    private final Map<String, KinesisShardMetrics> shardMetricGroupMap;

    protected KinesisShardSplitReaderBase(Map<String, KinesisShardMetrics> shardMetricGroupMap) {
        this.shardMetricGroupMap = shardMetricGroupMap;
    }

    @Override
    public RecordsWithSplitIds<Record> fetch() throws IOException {
        KinesisShardSplitState splitState = assignedSplits.poll();

        // When there are no assigned splits, return quickly
        if (splitState == null) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        // When assigned splits have been paused, skip the split
        if (pausedSplitIds.contains(splitState.getSplitId())) {
            assignedSplits.add(splitState);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        RecordBatch recordBatch;
        try {
            recordBatch = fetchRecords(splitState);
        } catch (ResourceNotFoundException e) {
            LOG.warn(
                    "Failed to fetch records from shard {}: shard no longer exists. Marking split as complete",
                    splitState.getSplitId());
            return new KinesisRecordsWithSplitIds(
                    Collections.emptyIterator(), splitState.getSplitId(), true);
        }

        if (recordBatch == null) {
            assignedSplits.add(splitState);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        if (!recordBatch.isCompleted()) {
            assignedSplits.add(splitState);
        }

        shardMetricGroupMap
                .get(splitState.getShardId())
                .setMillisBehindLatest(recordBatch.getMillisBehindLatest());

        if (recordBatch.getRecords().isEmpty()) {
            if (recordBatch.isCompleted()) {
                return new KinesisRecordsWithSplitIds(
                        Collections.emptyIterator(), splitState.getSplitId(), true);
            } else {
                return INCOMPLETE_SHARD_EMPTY_RECORDS;
            }
        }

        splitState.setNextStartingPosition(
                StartingPosition.continueFromSequenceNumber(
                        recordBatch
                                .getRecords()
                                .get(recordBatch.getRecords().size() - 1)
                                .sequenceNumber()));

        return new KinesisRecordsWithSplitIds(
                recordBatch.getRecords().iterator(),
                splitState.getSplitId(),
                recordBatch.isCompleted());
    }

    /**
     * Main method implementations must implement to fetch records from Kinesis.
     *
     * @param splitState split to fetch records for
     * @return RecordBatch containing the fetched records and metadata. Returns null if there are no
     *     records but fetching should be retried at a later time.
     */
    protected abstract RecordBatch fetchRecords(KinesisShardSplitState splitState);

    @Override
    public void handleSplitsChanges(SplitsChange<KinesisShardSplit> splitsChanges) {
        for (KinesisShardSplit split : splitsChanges.splits()) {
            assignedSplits.add(new KinesisShardSplitState(split));
        }
    }

    @Override
    public void wakeUp() {
        // Do nothing because we don't have any sleep mechanism
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<KinesisShardSplit> splitsToPause,
            Collection<KinesisShardSplit> splitsToResume) {
        splitsToPause.forEach(split -> pausedSplitIds.add(split.splitId()));
        splitsToResume.forEach(split -> pausedSplitIds.remove(split.splitId()));
    }

    /**
     * Dataclass to store a batch of Kinesis records with metadata. Used to pass Kinesis records
     * from the SplitReader implementation to the SplitReaderBase.
     */
    @Internal
    protected static class RecordBatch {
        private final List<Record> records;
        private final long millisBehindLatest;
        private final boolean completed;

        public RecordBatch(List<Record> records, long millisBehindLatest, boolean completed) {
            this.records = records;
            this.millisBehindLatest = millisBehindLatest;
            this.completed = completed;
        }

        public List<Record> getRecords() {
            return records;
        }

        public long getMillisBehindLatest() {
            return millisBehindLatest;
        }

        public boolean isCompleted() {
            return completed;
        }
    }

    /**
     * Implementation of {@link RecordsWithSplitIds} for sending Kinesis records from fetcher to the
     * SourceReader.
     */
    @Internal
    private static class KinesisRecordsWithSplitIds implements RecordsWithSplitIds<Record> {

        private final Iterator<Record> recordsIterator;
        private final String splitId;
        private final boolean isComplete;

        public KinesisRecordsWithSplitIds(
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
