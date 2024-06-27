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

package org.apache.flink.connector.kinesis.source.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

import java.time.Instant;

/** Starting position to perform list shard request. */
@Internal
public class ListShardsStartingPosition {
    private final ShardFilter shardFilter;

    private ListShardsStartingPosition(ShardFilter shardFilter) {
        this.shardFilter = shardFilter;
    }

    /** Returns shard filter used to perform listShard request. */
    public ShardFilter getShardFilter() {
        return shardFilter;
    }

    /** Used to get shards that were active at or after specified timestamp. */
    public static ListShardsStartingPosition fromTimestamp(Instant timestamp) {
        Preconditions.checkNotNull(timestamp, "timestamp cannot be null");

        return new ListShardsStartingPosition(
                ShardFilter.builder()
                        .type(ShardFilterType.FROM_TIMESTAMP)
                        .timestamp(timestamp)
                        .build());
    }

    /** Used to get shards after specified shard id. */
    public static ListShardsStartingPosition fromShardId(String exclusiveStartShardId) {
        Preconditions.checkNotNull(exclusiveStartShardId, "exclusiveStartShardId cannot be null");

        return new ListShardsStartingPosition(
                ShardFilter.builder()
                        .type(ShardFilterType.AFTER_SHARD_ID)
                        .shardId(exclusiveStartShardId)
                        .build());
    }

    /** Used to get all shards starting from TRIM_HORIZON. */
    public static ListShardsStartingPosition fromStart() {
        return new ListShardsStartingPosition(
                ShardFilter.builder().type(ShardFilterType.FROM_TRIM_HORIZON).build());
    }
}
