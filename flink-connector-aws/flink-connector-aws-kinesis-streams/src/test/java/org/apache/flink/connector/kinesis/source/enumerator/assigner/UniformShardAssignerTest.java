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

package org.apache.flink.connector.kinesis.source.enumerator.assigner;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardAssigner;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestReaderInfo;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class UniformShardAssignerTest {

    @ParameterizedTest
    @MethodSource("hashKeyRangeDataProvider")
    void testSplitAssignedByHashKeyRange(
            BigInteger startingHashKey,
            BigInteger endingHashKey,
            int nSubtasks,
            int expectedSubtask) {
        // Given a split with a given hash-key range
        final KinesisShardSplit split = getTestSplit(startingHashKey, endingHashKey);
        final TestShardAssignerContext assignerContext = new TestShardAssignerContext();
        // Given a distribution of subtasks with varying busyness
        for (int i = 0; i < nSubtasks; i++) {
            createReaderWithAssignedSplits(assignerContext, i, nSubtasks - i);
        }

        // When assigned a subtask
        KinesisShardAssigner assigner = new UniformShardAssigner();

        // Then expected subtask is assigned
        assertThat(assigner.assign(split, assignerContext)).isEqualTo(expectedSubtask);
    }

    static Stream<Arguments> hashKeyRangeDataProvider() {
        BigInteger two = BigInteger.valueOf(2);
        BigInteger three = BigInteger.valueOf(3);
        // Split the hash key range into thirds
        BigInteger maxHashKey = two.pow(128).subtract(BigInteger.ONE);
        BigInteger[] rangeBoundaries = {
            BigInteger.ZERO,
            maxHashKey.divide(three),
            maxHashKey.divide(three).multiply(two),
            maxHashKey
        };
        return Stream.of(
                // Test that each shard will be distributed evenly
                Arguments.of(rangeBoundaries[0], rangeBoundaries[1], 3, 0),
                Arguments.of(rangeBoundaries[1], rangeBoundaries[2], 3, 1),
                Arguments.of(rangeBoundaries[2], rangeBoundaries[3], 3, 2),
                // Edge case: Hash key range from 0 to 0
                Arguments.of(BigInteger.ZERO, BigInteger.ZERO, 3, 0),
                // Edge case: Negative hash key range should be treated as positive.
                // This is possible if restoring from old state
                Arguments.of(BigInteger.ONE.negate(), BigInteger.ZERO, 3, 0));
    }

    @Test
    void testNoRegisteredReaders() {
        final KinesisShardSplit split = getTestSplit();
        final TestShardAssignerContext assignerContext = new TestShardAssignerContext();

        // Given no registered readers

        // When assigned a subtask
        KinesisShardAssigner assigner = new UniformShardAssigner();

        // Then Exception is thrown
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> assigner.assign(split, assignerContext))
                .withMessageContaining(
                        "Expected at least one registered reader. Unable to assign split.");
    }

    private void createReaderWithAssignedSplits(
            TestShardAssignerContext testShardAssignerContext,
            int subtaskId,
            int numAssignedSplits) {
        testShardAssignerContext.registeredReaders.put(subtaskId, getTestReaderInfo(subtaskId));

        if (!testShardAssignerContext.splitAssignment.containsKey(subtaskId)) {
            testShardAssignerContext.splitAssignment.put(subtaskId, new HashSet<>());
        }
        for (int i = 0; i < numAssignedSplits; i++) {
            testShardAssignerContext
                    .splitAssignment
                    .get(subtaskId)
                    .add(getTestSplit(String.valueOf(i)));
        }
    }

    private static class TestShardAssignerContext implements KinesisShardAssigner.Context {
        private final Map<Integer, Set<KinesisShardSplit>> splitAssignment = new HashMap<>();
        private final Map<Integer, List<KinesisShardSplit>> pendingSplitAssignments =
                new HashMap<>();
        private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();

        @Override
        public Map<Integer, Set<KinesisShardSplit>> getCurrentSplitAssignment() {
            return splitAssignment;
        }

        @Override
        public Map<Integer, ReaderInfo> getRegisteredReaders() {
            return registeredReaders;
        }

        @Override
        public Map<Integer, List<KinesisShardSplit>> getPendingSplitAssignments() {
            return pendingSplitAssignments;
        }
    }
}
