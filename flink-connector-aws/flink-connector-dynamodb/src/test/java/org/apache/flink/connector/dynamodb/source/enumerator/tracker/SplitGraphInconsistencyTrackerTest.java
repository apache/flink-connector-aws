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

package org.apache.flink.connector.dynamodb.source.enumerator.tracker;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShard;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@link SplitGraphInconsistencyTracker} class to verify that it correctly discovers
 * inconsistencies within the shard graph.
 */
public class SplitGraphInconsistencyTrackerTest {
    @Test
    public void testSplitGraphInconsistencyTrackerHappyCase() {
        List<Shard> shards =
                Arrays.asList(
                        // shards which don't have a parent
                        generateShard(0, "1400", "1700", null),
                        generateShard(1, "1500", "1800", null),
                        // shards produced by rotation of parents
                        generateShard(2, "1710", null, generateShardId(0)),
                        generateShard(3, "1520", null, generateShardId(1)));
        SplitGraphInconsistencyTracker splitGraphInconsistencyTracker =
                new SplitGraphInconsistencyTracker();
        splitGraphInconsistencyTracker.addNodes(shards);
        assertThat(splitGraphInconsistencyTracker.inconsistencyDetected()).isFalse();
        assertThat(splitGraphInconsistencyTracker.getNodes())
                .containsExactlyInAnyOrderElementsOf(shards);
        assertThat(splitGraphInconsistencyTracker.getLatestLeafNode())
                .isEqualTo(generateShardId(3));
    }

    @Test
    public void testSplitGraphInconsistencyTrackerWhenThereIsNoShardTracked() {
        SplitGraphInconsistencyTracker splitGraphInconsistencyTracker =
                new SplitGraphInconsistencyTracker();
        assertThat(splitGraphInconsistencyTracker.inconsistencyDetected()).isFalse();
        assertThat(splitGraphInconsistencyTracker.getLatestLeafNode()).isEqualTo(null);
    }

    @Test
    public void
            testSplitGraphInconsistencyTrackerDetectsInconsistenciesAndDoesNotDeleteClosedLeafNode() {
        long firstShardIdTimestamp = Instant.now().toEpochMilli();
        long secondShardIdTimestamp = Instant.now().minus(Duration.ofHours(2)).toEpochMilli();
        List<Shard> shards =
                Arrays.asList(
                        // shards which don't have a parent
                        generateShard(firstShardIdTimestamp, "1400", "1700", null),
                        generateShard(secondShardIdTimestamp, "1500", "1800", null),
                        // shards produced by rotation of parents
                        generateShard(2, "1710", null, generateShardId(firstShardIdTimestamp)));
        SplitGraphInconsistencyTracker splitGraphInconsistencyTracker =
                new SplitGraphInconsistencyTracker();
        splitGraphInconsistencyTracker.addNodes(shards);
        assertThat(splitGraphInconsistencyTracker.inconsistencyDetected()).isTrue();
        assertThat(splitGraphInconsistencyTracker.getEarliestClosedLeafNode())
                .isEqualTo(generateShardId(secondShardIdTimestamp));
        assertThat(splitGraphInconsistencyTracker.getNodes())
                .containsExactlyInAnyOrderElementsOf(shards);
        assertThat(splitGraphInconsistencyTracker.getLatestLeafNode())
                .isEqualTo(generateShardId(secondShardIdTimestamp));
    }

    @Test
    public void testSplitGraphInconsistencyTrackerDetectsInconsistenciesAndDeletesClosedLeafNode() {
        long firstShardIdTimestamp = Instant.now().toEpochMilli();
        long secondShardIdTimestamp = Instant.now().minus(Duration.ofHours(26)).toEpochMilli();
        List<Shard> shards =
                Arrays.asList(
                        // shards which don't have a parent
                        generateShard(firstShardIdTimestamp, "1400", "1700", null),
                        generateShard(secondShardIdTimestamp, "1500", "1800", null),
                        // shards produced by rotation of parents
                        generateShard(2, "1710", null, generateShardId(firstShardIdTimestamp)));
        SplitGraphInconsistencyTracker splitGraphInconsistencyTracker =
                new SplitGraphInconsistencyTracker();
        splitGraphInconsistencyTracker.addNodes(shards);
        assertThat(splitGraphInconsistencyTracker.inconsistencyDetected()).isFalse();
        assertThat(splitGraphInconsistencyTracker.getNodes())
                .containsExactlyInAnyOrder(shards.get(0), shards.get(2));
        assertThat(splitGraphInconsistencyTracker.getLatestLeafNode())
                .isEqualTo(generateShardId(secondShardIdTimestamp));
    }
}
