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

package org.apache.flink.connector.dynamodb.source.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.source.enumerator.tracker.SplitGraphInconsistencyTracker;
import org.apache.flink.connector.dynamodb.source.enumerator.tracker.SplitTracker;

import java.time.Duration;
import java.time.Instant;

/** Utility methods for properties of DynamoDB streams shards. */
@Internal
public class ShardUtils {
    /**
     * Maximum retention period for shards to be stored in {@link SplitTracker}. We keep this 48
     * hours to avoid corner cases during expiry of DDB Streams shards which can lead to
     * DescribeStream returning the shard, shard being finished processing and {@link SplitTracker}
     * tracking them again leading to reprocessing.
     */
    private static final Duration DDB_STREAMS_MAX_RETENTION_PERIOD = Duration.ofHours(48);

    /**
     * Maximum retention period for shards to be stored in {@link SplitGraphInconsistencyTracker}.
     * DDB Streams records are expired after 24 hours. We do not need to resolve inconsistencies
     * during shard expiry time.
     */
    private static final Duration DDB_STREAMS_MAX_RETENTION_PERIOD_FOR_RESOLVING_INCONSISTENCIES =
            Duration.ofHours(25);

    private static final String SHARD_ID_SEPARATOR = "-";
    /**
     * This method extracts the shard creation timestamp from the shardId.
     *
     * @param shardId
     * @return instant on which shard was created.
     */
    public static Instant getShardCreationTime(String shardId) {
        return Instant.ofEpochMilli(Long.parseLong(shardId.split(SHARD_ID_SEPARATOR)[1]));
    }

    /**
     * Returns true if the shard is older than what we want to store in {@link SplitTracker}.
     *
     * @param shardId
     */
    public static boolean isShardOlderThanRetentionPeriod(String shardId) {
        return Instant.now()
                .isAfter(getShardCreationTime(shardId).plus(DDB_STREAMS_MAX_RETENTION_PERIOD));
    }

    /**
     * Returns true if the shard is older than what we want to store in {@link
     * SplitGraphInconsistencyTracker}.
     *
     * @param shardId
     */
    public static boolean isShardOlderThanInconsistencyDetectionRetentionPeriod(String shardId) {
        return Instant.now()
                .isAfter(
                        getShardCreationTime(shardId)
                                .plus(
                                        DDB_STREAMS_MAX_RETENTION_PERIOD_FOR_RESOLVING_INCONSISTENCIES));
    }
}
