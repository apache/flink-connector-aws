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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Unit tests for {@link ShardUtils} class. */
public class ShardUtilsTest {
    @Test
    void testGetShardCreationTime() {
        long currentTime = Instant.now().toEpochMilli();
        String shardId = "shardId-" + currentTime;
        assertThat(ShardUtils.getShardCreationTime(shardId).toEpochMilli()).isEqualTo(currentTime);
    }

    @Test
    void testIsShardOlderThanRetentionPeriod() {
        Instant currentTime = Instant.now();
        String oldShardId = "shardId-" + currentTime.minus(Duration.ofHours(26)).toEpochMilli();
        String newShardId = "shardId-" + currentTime.toEpochMilli();
        assertThat(ShardUtils.isShardOlderThanRetentionPeriod(oldShardId)).isTrue();
        assertThat(ShardUtils.isShardOlderThanRetentionPeriod(newShardId)).isFalse();
    }
}
