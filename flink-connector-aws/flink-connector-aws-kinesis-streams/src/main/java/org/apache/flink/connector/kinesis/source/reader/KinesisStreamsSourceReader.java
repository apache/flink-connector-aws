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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.Map;

/**
 * Coordinates the reading from assigned splits. Runs on the TaskManager.
 *
 * @param <T> the data type emitted by the Kinesis stream source
 */
@Internal
public class KinesisStreamsSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Record, T, KinesisShardSplit, KinesisShardSplitState> {

    public KinesisStreamsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Record>> elementsQueue,
            SingleThreadFetcherManager<Record, KinesisShardSplit> splitFetcherManager,
            RecordEmitter<Record, T, KinesisShardSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, KinesisShardSplitState> finishedSplitIds) {
        // no-op. We don't need to do anything on finished split now
    }

    @Override
    protected KinesisShardSplitState initializedState(KinesisShardSplit split) {
        return new KinesisShardSplitState(split);
    }

    @Override
    protected KinesisShardSplit toSplitType(String splitId, KinesisShardSplitState splitState) {
        return splitState.getKinesisShardSplit();
    }
}
