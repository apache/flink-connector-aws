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

package org.apache.flink.connector.dynamodb.source.enumerator.assigner;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsShardAssigner;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestReaderInfo;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class UniformShardAssignerTest {

    @Test
    void testAssignedToLeastBusySubtask() {
        final DynamoDbStreamsShardSplit split = getTestSplit();
        final TestShardAssignerContext assignerContext = new TestShardAssignerContext();

        // Given a distribution of subtasks with varying busyness
        createReaderWithAssignedSplits(assignerContext, 1, 3);
        createReaderWithAssignedSplits(assignerContext, 2, 2);
        createReaderWithAssignedSplits(assignerContext, 3, 1);

        // When assigned a subtask
        DynamoDbStreamsShardAssigner assigner = new UniformShardAssigner();

        // Then least busy subtask is chosen
        assertThat(assigner.assign(split, assignerContext)).isEqualTo(3);
    }

    @Test
    void testAssignedToLeastBusySubtaskConsideringPendingAssignments() {
        final DynamoDbStreamsShardSplit split = getTestSplit();
        final TestShardAssignerContext assignerContext = new TestShardAssignerContext();

        // Given a distribution of subtasks with same busyness
        createReaderWithAssignedSplits(assignerContext, 1, 1);
        createReaderWithAssignedSplits(assignerContext, 2, 1);
        createReaderWithAssignedSplits(assignerContext, 3, 1);
        // Given a pending distribution of subtasks with varying busyness
        addPendingSplits(assignerContext, 1, 3);
        addPendingSplits(assignerContext, 2, 1);
        addPendingSplits(assignerContext, 3, 3);

        // When assigned a subtask
        DynamoDbStreamsShardAssigner assigner = new UniformShardAssigner();

        // Then least busy subtask is chosen
        assertThat(assigner.assign(split, assignerContext)).isEqualTo(2);
    }

    @Test
    void testOnlyRegisteredReaders() {
        final DynamoDbStreamsShardSplit split = getTestSplit();
        final TestShardAssignerContext assignerContext = new TestShardAssignerContext();

        // Given a few registered readers
        assignerContext.registeredReaders.put(1, getTestReaderInfo(1));
        assignerContext.registeredReaders.put(2, getTestReaderInfo(2));
        assignerContext.registeredReaders.put(3, getTestReaderInfo(3));

        // When assigned a subtask
        DynamoDbStreamsShardAssigner assigner = new UniformShardAssigner();
        // Then Exception is thrown
        assertThat(assigner.assign(split, assignerContext)).isIn(1, 2, 3);
    }

    @Test
    void testNoRegisteredReaders() {
        final DynamoDbStreamsShardSplit split = getTestSplit();
        final TestShardAssignerContext assignerContext = new TestShardAssignerContext();

        // Given no registered readers

        // When assigned a subtask
        DynamoDbStreamsShardAssigner assigner = new UniformShardAssigner();

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

    private void addPendingSplits(
            TestShardAssignerContext testShardAssignerContext,
            int subtaskId,
            int numAssignedSplits) {
        if (!testShardAssignerContext.pendingSplitAssignments.containsKey(subtaskId)) {
            testShardAssignerContext.pendingSplitAssignments.put(subtaskId, new ArrayList<>());
        }
        for (int i = 0; i < numAssignedSplits; i++) {
            testShardAssignerContext
                    .pendingSplitAssignments
                    .get(subtaskId)
                    .add(getTestSplit(String.valueOf(i)));
        }
    }

    private static class TestShardAssignerContext implements DynamoDbStreamsShardAssigner.Context {
        private final Map<Integer, Set<DynamoDbStreamsShardSplit>> splitAssignment =
                new HashMap<>();
        private final Map<Integer, List<DynamoDbStreamsShardSplit>> pendingSplitAssignments =
                new HashMap<>();
        private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();

        @Override
        public Map<Integer, Set<DynamoDbStreamsShardSplit>> getCurrentSplitAssignment() {
            return splitAssignment;
        }

        @Override
        public Map<Integer, ReaderInfo> getRegisteredReaders() {
            return registeredReaders;
        }

        @Override
        public Map<Integer, List<DynamoDbStreamsShardSplit>> getPendingSplitAssignments() {
            return pendingSplitAssignments;
        }
    }
}
