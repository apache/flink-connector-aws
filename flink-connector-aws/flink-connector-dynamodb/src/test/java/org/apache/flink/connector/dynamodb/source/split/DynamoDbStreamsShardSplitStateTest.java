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

package org.apache.flink.connector.dynamodb.source.split;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class DynamoDbStreamsShardSplitStateTest {

    @Test
    void testGetDynamoDbStreamsShardSplitDelegatesToInitialSplit() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        DynamoDbStreamsShardSplitState splitState = new DynamoDbStreamsShardSplitState(split);

        assertThat(splitState.getDynamoDbStreamsShardSplit())
                .usingRecursiveComparison()
                .isEqualTo(split);
    }

    @Test
    void testGetDynamoDbStreamsShardSplitUsesLatestStartingPosition() throws Exception {
        DynamoDbStreamsShardSplitState splitState =
                new DynamoDbStreamsShardSplitState(getTestSplit());

        StartingPosition nextStartingPosition = StartingPosition.latest();
        splitState.setNextStartingPosition(nextStartingPosition);

        assertThat(splitState.getDynamoDbStreamsShardSplit().getStartingPosition())
                .usingRecursiveComparison()
                .isEqualTo(nextStartingPosition);
    }

    @Test
    void testSplitIdDelegatesToInitialSplit() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        DynamoDbStreamsShardSplitState splitState = new DynamoDbStreamsShardSplitState(split);

        assertThat(splitState.getSplitId()).hasToString(split.splitId());
    }

    @Test
    void testGetStreamArnDelegatesToInitialSplit() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        DynamoDbStreamsShardSplitState splitState = new DynamoDbStreamsShardSplitState(split);

        assertThat(splitState.getStreamArn()).hasToString(split.getStreamArn());
    }

    @Test
    void testGetShardIdDelegatesToInitialSplit() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        DynamoDbStreamsShardSplitState splitState = new DynamoDbStreamsShardSplitState(split);

        assertThat(splitState.getShardId()).hasToString(split.getShardId());
    }

    @Test
    void testGetAndSetNextStartingPosition() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        DynamoDbStreamsShardSplitState splitState = new DynamoDbStreamsShardSplitState(split);

        assertThat(splitState.getNextStartingPosition()).isEqualTo(split.getStartingPosition());

        StartingPosition newStartingPosition = StartingPosition.latest();
        splitState.setNextStartingPosition(newStartingPosition);

        assertThat(splitState.getNextStartingPosition())
                .usingRecursiveComparison()
                .isEqualTo(newStartingPosition);
    }

    @Test
    void testGetAndSetShardIterator() throws Exception {
        DynamoDbStreamsShardSplit split = getTestSplit();
        DynamoDbStreamsShardSplitState splitState = new DynamoDbStreamsShardSplitState(split);

        assertThat(splitState.getNextShardIterator()).isNull();

        String newShardIterator = "some-shard-iterator";
        splitState.setNextShardIterator(newShardIterator);

        assertThat(splitState.getNextShardIterator()).hasToString(newShardIterator);
    }
}
