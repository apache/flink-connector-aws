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

package org.apache.flink.connector.kinesis.source.split;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class KinesisShardSplitStateTest {

    @Test
    void testGetKinesisShardSplitDelegatesToInitialSplit() throws Exception {
        KinesisShardSplit split = getTestSplit();
        KinesisShardSplitState splitState = new KinesisShardSplitState(split);

        assertThat(splitState.getKinesisShardSplit()).usingRecursiveComparison().isEqualTo(split);
    }

    @Test
    void testGetKinesisShardSplitUsesLatestStartingPosition() throws Exception {
        KinesisShardSplitState splitState = new KinesisShardSplitState(getTestSplit());

        StartingPosition nextStartingPosition = StartingPosition.fromTimestamp(Instant.now());
        splitState.setNextStartingPosition(nextStartingPosition);

        assertThat(splitState.getKinesisShardSplit().getStartingPosition())
                .usingRecursiveComparison()
                .isEqualTo(nextStartingPosition);
    }

    @Test
    void testSplitIdDelegatesToInitialSplit() throws Exception {
        KinesisShardSplit split = getTestSplit();
        KinesisShardSplitState splitState = new KinesisShardSplitState(split);

        assertThat(splitState.getSplitId()).hasToString(split.splitId());
    }

    @Test
    void testGetStreamArnDelegatesToInitialSplit() throws Exception {
        KinesisShardSplit split = getTestSplit();
        KinesisShardSplitState splitState = new KinesisShardSplitState(split);

        assertThat(splitState.getStreamArn()).hasToString(split.getStreamArn());
    }

    @Test
    void testGetShardIdDelegatesToInitialSplit() throws Exception {
        KinesisShardSplit split = getTestSplit();
        KinesisShardSplitState splitState = new KinesisShardSplitState(split);

        assertThat(splitState.getShardId()).hasToString(split.getShardId());
    }

    @Test
    void testGetAndSetNextStartingPosition() throws Exception {
        KinesisShardSplit split = getTestSplit();
        KinesisShardSplitState splitState = new KinesisShardSplitState(split);

        assertThat(splitState.getNextStartingPosition()).isEqualTo(split.getStartingPosition());

        StartingPosition newStartingPosition = StartingPosition.fromTimestamp(Instant.now());
        splitState.setNextStartingPosition(newStartingPosition);

        assertThat(splitState.getNextStartingPosition())
                .usingRecursiveComparison()
                .isEqualTo(newStartingPosition);
    }

    @Test
    void testGetAndSetShardIterator() throws Exception {
        KinesisShardSplit split = getTestSplit();
        KinesisShardSplitState splitState = new KinesisShardSplitState(split);

        assertThat(splitState.getNextShardIterator()).isNull();

        String newShardIterator = "some-shard-iterator";
        splitState.setNextShardIterator(newShardIterator);

        assertThat(splitState.getNextShardIterator()).hasToString(newShardIterator);
    }
}
