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

package org.apache.flink.connector.kinesis.source.reader.polling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.reader.KinesisShardSplitReaderBase;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_GET_RECORDS_IDLE_SOURCE_INTERVAL;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_GET_RECORDS_INTERVAL;

/**
 * An implementation of the KinesisShardSplitReader that periodically polls the Kinesis stream to
 * retrieve records.
 */
@Internal
public class PollingKinesisShardSplitReader extends KinesisShardSplitReaderBase {
    private final StreamProxy kinesis;
    private final Configuration configuration;
    private final int maxRecordsToGet;

    private final long getRecordsIntervalMillis;

    private final long idleSourceGetRecordsIntervalMillis;

    private long scheduledGetRecordTimeMillis = 0;

    private static final Logger LOG =
            LoggerFactory.getLogger(PollingKinesisShardSplitReader.class);

    public PollingKinesisShardSplitReader(
            StreamProxy kinesisProxy,
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            Configuration configuration) {
        super(shardMetricGroupMap);
        this.kinesis = kinesisProxy;
        this.configuration = configuration;
        this.maxRecordsToGet = configuration.get(KinesisSourceConfigOptions.SHARD_GET_RECORDS_MAX);
        this.getRecordsIntervalMillis = configuration.get(SHARD_GET_RECORDS_INTERVAL).toMillis();
        this.idleSourceGetRecordsIntervalMillis = configuration.get(SHARD_GET_RECORDS_IDLE_SOURCE_INTERVAL).toMillis();
    }

    @Override
    protected RecordBatch fetchRecords(KinesisShardSplitState splitState) throws IOException {
        sleepUntilScheduledGetRecordTime();

        GetRecordsResponse getRecordsResponse =
                kinesis.getRecords(
                        splitState.getStreamArn(),
                        splitState.getShardId(),
                        splitState.getNextStartingPosition(),
                        this.maxRecordsToGet);

        scheduleNextGetRecord(getRecordsResponse);

        boolean isCompleted = getRecordsResponse.nextShardIterator() == null;
        return new RecordBatch(
                getRecordsResponse.records(), getRecordsResponse.millisBehindLatest(), isCompleted);
    }

    private void sleepUntilScheduledGetRecordTime() throws IOException {
        long millisToNextGetRecord = scheduledGetRecordTimeMillis - System.currentTimeMillis();
        if (millisToNextGetRecord > 0) {
            try {
                Thread.sleep(millisToNextGetRecord);
            } catch (InterruptedException e) {
                throw new IOException("Sleep was interrupted while waiting for next scheduled GetRecord ", e);
            }
        }
    }

    private void scheduleNextGetRecord(GetRecordsResponse getRecordsResponse) {
        if (getRecordsResponse.records().isEmpty()) {
            scheduledGetRecordTimeMillis = System.currentTimeMillis() + idleSourceGetRecordsIntervalMillis;
            LOG.info("Got empty list from GetRecords, scheduling next get record to ", new Date(scheduledGetRecordTimeMillis));
            if (LOG.isWarnEnabled()) {
                LOG.warn("Got empty list from GetRecords, scheduling next get record to ", new Date(scheduledGetRecordTimeMillis));
            }
        } else {
            scheduledGetRecordTimeMillis = System.currentTimeMillis() + getRecordsIntervalMillis;
        }
    }

    @Override
    public void close() throws Exception {
        kinesis.close();
    }
}
