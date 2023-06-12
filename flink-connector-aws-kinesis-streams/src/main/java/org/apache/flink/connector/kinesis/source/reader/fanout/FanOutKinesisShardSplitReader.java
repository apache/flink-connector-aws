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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.reader.fanout.FanOutShardSubscriber.FanOutSubscriptionEvent;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of the SplitReader that periodically polls the Kinesis stream to retrieve
 * records.
 */
@Internal
public class FanOutKinesisShardSplitReader implements SplitReader<Record, KinesisShardSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSplitReader.class);
    private static final RecordsWithSplitIds<Record> INCOMPLETE_SHARD_EMPTY_RECORDS =
            new KinesisRecordsWithSplitIds(Collections.emptyIterator(), null, false);

    private final StreamProxy kinesis;
    private final Map<String, KinesisShardSplitState> splitStates = new HashMap<>();
    private final BlockingQueue<FanOutSubscriptionEvent> consumingQueue = new LinkedBlockingQueue<>();
    private final Map<String, FanOutShardSubscriber> shardSubscriberLookup = new HashMap<>();
    private AtomicBoolean running = new AtomicBoolean(true);
    private Queue<Exception> exceptions = new ConcurrentLinkedQueue<>();
    private int attempt = 0;

    public FanOutKinesisShardSplitReader(StreamProxy kinesisProxy) {
        this.kinesis = kinesisProxy;
    }

    @Override
    public RecordsWithSplitIds<Record> fetch() throws IOException {
        FanOutSubscriptionEvent e = consumingQueue.poll();

        if (e == null) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        } else if (e.isSubscribeToShardEvent()) {
            shardSubscriberLookup.get(e.getShardId())
                .requestRecords();

            SubscribeToShardEvent event = e.getSubscribeToShardEvent();
            boolean isComplete = event.continuationSequenceNumber() == null;
            if (!event.records().isEmpty()) {
                return new KinesisRecordsWithSplitIds(event.records().iterator(), e.getShardId(), isComplete);
            }
        } else if (e.isSubscriptionComplete()) {
            subscribeToShard(splitStates.get(e.getShardId()));
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        Throwable t = e.getThrowable();
        LOG.warn("Caught exception when reading from consumer.", t);
        subscribeToShard(splitStates.get(e.getShardId()));
        return INCOMPLETE_SHARD_EMPTY_RECORDS;
    }

    private void backoff(final Throwable ex) throws InterruptedException {
        Thread.sleep(1);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KinesisShardSplit> splitsChanges) {
        for (KinesisShardSplit split : splitsChanges.splits()) {
            KinesisShardSplitState splitState = new KinesisShardSplitState(split);
            splitStates.put(split.getShardId(), splitState);
            subscribeToShard(splitState);
        }
    }

    private void subscribeToShard(KinesisShardSplitState splitState) {

        FanOutShardSubscriber fanOutShardSubscriber = new FanOutShardSubscriber(
            "consumerArn",
            splitState.getShardId(),
            kinesis,
            consumingQueue,
            () -> running.get());
        fanOutShardSubscriber.openSubscriptionToShard(splitState.getNextStartingPosition());
        shardSubscriberLookup.put(splitState.getShardId(), fanOutShardSubscriber);
    }

    @Override
    public void wakeUp() {
        // Do nothing because we don't have any sleep mechanism
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        kinesis.close();
    }

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
            return isComplete ? ImmutableSet.of(splitId) : Collections.emptySet();
        }
    }
}
