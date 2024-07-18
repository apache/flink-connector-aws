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

import java.time.Duration;
import java.time.Instant;

/** Utility methods for properties of DynamoDB streams shards. */
@Internal
public class ShardUtils {
    private static final Duration DDB_STREAMS_MAX_RETENTION_PERIOD = Duration.ofHours(25);
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
     * Returns true if the shard has been created for more than 24 hours.
     *
     * @param shardId
     */
    public static boolean isShardOlderThanRetentionPeriod(String shardId) {
        return Instant.now()
                .isAfter(getShardCreationTime(shardId).plus(DDB_STREAMS_MAX_RETENTION_PERIOD));
    }
}
