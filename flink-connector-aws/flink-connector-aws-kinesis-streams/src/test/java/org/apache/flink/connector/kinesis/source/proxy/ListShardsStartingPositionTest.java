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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class ListShardsStartingPositionTest {
    @Test
    void testShardPositionFromTimestamp() {
        Instant timestamp = Instant.ofEpochMilli(1720543032243L);
        ListShardsStartingPosition startingPosition =
                ListShardsStartingPosition.fromTimestamp(timestamp);

        ShardFilter expected =
                ShardFilter.builder()
                        .type(ShardFilterType.FROM_TIMESTAMP)
                        .timestamp(timestamp)
                        .build();

        assertThat(startingPosition.getShardFilter()).isEqualTo(expected);
    }

    @Test
    void testShardPositionFromTimestampShouldFailOnNullTimestamp() {
        assertThatThrownBy(() -> ListShardsStartingPosition.fromTimestamp(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testShardPositionFromShardId() {
        String shardId = "shard-00000000002";
        ListShardsStartingPosition startingPosition =
                ListShardsStartingPosition.fromShardId(shardId);

        ShardFilter expected =
                ShardFilter.builder().type(ShardFilterType.AFTER_SHARD_ID).shardId(shardId).build();

        assertThat(startingPosition.getShardFilter()).isEqualTo(expected);
    }

    @Test
    void testShardPositionFromShardIdShouldFailOnNullTimestamp() {
        assertThatThrownBy(() -> ListShardsStartingPosition.fromShardId(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testShardPositionFromStart() {
        ListShardsStartingPosition startingPosition = ListShardsStartingPosition.fromStart();

        ShardFilter expected =
                ShardFilter.builder().type(ShardFilterType.FROM_TRIM_HORIZON).build();

        assertThat(startingPosition.getShardFilter()).isEqualTo(expected);
    }
}
