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

package org.apache.flink.connector.kinesis.source.enumerator.tracker;

import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardSplitWithAssignmentStatus;
import org.apache.flink.connector.kinesis.source.enumerator.SplitAssignmentStatus;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class SplitTrackerTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSplitWithoutParentAreAvailableToAssign(boolean preserveShardOrdering) {
        SplitTracker splitTracker = new SplitTracker(preserveShardOrdering);

        KinesisShardSplit split = getTestSplit(generateShardId(1), Collections.emptySet());

        splitTracker.addSplits(Collections.singletonList(split));

        List<KinesisShardSplit> pendingSplits = splitTracker.splitsAvailableForAssignment();

        assertThat(pendingSplits).containsExactly(split);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStateSnapshot(boolean preserveShardOrdering) {
        List<KinesisShardSplit> assignedSplits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()));
        List<KinesisShardSplit> unassignedSplits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(3), Collections.emptySet()),
                        // Shards produced by splitting parent shard
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(1))),
                        getTestSplit(generateShardId(5), Collections.singleton(generateShardId(1))),
                        // Shard produced by merging 2 parent shards
                        getTestSplit(
                                generateShardId(6),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(2), generateShardId(3)))));

        List<KinesisShardSplitWithAssignmentStatus> assignedSplitsWithStatus =
                assignedSplits.stream()
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.ASSIGNED))
                        .collect(Collectors.toList());
        List<KinesisShardSplitWithAssignmentStatus> unassignedSplitsWithStatus =
                unassignedSplits.stream()
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED))
                        .collect(Collectors.toList());
        List<KinesisShardSplitWithAssignmentStatus> expectedState =
                new ArrayList<>(assignedSplitsWithStatus);
        expectedState.addAll(unassignedSplitsWithStatus);

        SplitTracker splitTracker = new SplitTracker(preserveShardOrdering);

        splitTracker.addSplits(assignedSplits);
        splitTracker.addSplits(unassignedSplits);
        splitTracker.markAsAssigned(assignedSplits);

        // Verify that produced state is the same
        assertThat(splitTracker.snapshotState(0))
                .containsExactlyInAnyOrderElementsOf(expectedState);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSplitSnapshotStateDoesNotIncludeFinishedSplits(boolean preserveShardOrdering) {
        List<KinesisShardSplit> splits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()),
                        getTestSplit(generateShardId(3), Collections.emptySet()));

        List<KinesisShardSplitWithAssignmentStatus> expectedSplitState =
                splits.stream()
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.ASSIGNED))
                        .collect(Collectors.toList());

        SplitTracker splitTracker = new SplitTracker(preserveShardOrdering);
        splitTracker.addSplits(splits);
        splitTracker.markAsAssigned(splits);

        String splitIdToFinish = expectedSplitState.get(0).split().splitId();
        splitTracker.markAsFinished(Collections.singletonList(splitIdToFinish));

        // Verify that state does not contain finished split
        assertThat(splitTracker.snapshotState(0))
                .doesNotContain(expectedSplitState.get(0))
                .containsExactlyInAnyOrder(expectedSplitState.get(1), expectedSplitState.get(2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStateRestore(boolean preserveShardOrdering) {
        List<KinesisShardSplit> splits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()),
                        getTestSplit(generateShardId(3), Collections.emptySet()),
                        // Shards produced by splitting parent shard
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(1))),
                        getTestSplit(generateShardId(5), Collections.singleton(generateShardId(1))),
                        // Shard produced by merging 2 parent shards
                        getTestSplit(
                                generateShardId(6),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(2), generateShardId(3)))));

        Stream<KinesisShardSplitWithAssignmentStatus> assignedSplits =
                splits.subList(0, 3).stream()
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.ASSIGNED));
        Stream<KinesisShardSplitWithAssignmentStatus> unassignedSplits =
                splits.subList(3, splits.size()).stream()
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED));
        List<KinesisShardSplitWithAssignmentStatus> initialState =
                Stream.concat(assignedSplits, unassignedSplits).collect(Collectors.toList());

        SplitTracker splitTracker = new SplitTracker(preserveShardOrdering, initialState);

        // Verify that produced state is the same
        assertThat(splitTracker.snapshotState(0)).containsExactlyInAnyOrderElementsOf(initialState);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMarkUnknownSplitAsAssignedIsNoop(boolean preserveShardOrdering) {
        List<KinesisShardSplit> splits =
                Arrays.asList(
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(1))),
                        getTestSplit(generateShardId(5), Collections.singleton(generateShardId(1))),
                        getTestSplit(
                                generateShardId(6),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(2), generateShardId(3)))));
        List<KinesisShardSplitWithAssignmentStatus> initialState =
                splits.stream()
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED))
                        .collect(Collectors.toList());

        SplitTracker splitTracker = new SplitTracker(preserveShardOrdering, initialState);
        List<KinesisShardSplit> splitsToAssign =
                Collections.singletonList(getTestSplit(generateShardId(0)));

        assertThatNoException().isThrownBy(() -> splitTracker.markAsAssigned(splitsToAssign));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMarkUnknownSplitAsFinishedIsNoop(boolean preserveShardOrdering) {
        List<KinesisShardSplit> splits =
                Arrays.asList(
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(1))),
                        getTestSplit(generateShardId(5), Collections.singleton(generateShardId(1))),
                        getTestSplit(
                                generateShardId(6),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(2), generateShardId(3)))));
        List<KinesisShardSplitWithAssignmentStatus> initialState =
                splits.stream()
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED))
                        .collect(Collectors.toList());

        SplitTracker splitTracker = new SplitTracker(preserveShardOrdering, initialState);
        List<String> splitIdsToFinish = Collections.singletonList(generateShardId(0));

        assertThatNoException().isThrownBy(() -> splitTracker.markAsFinished(splitIdsToFinish));
    }

    // -----------------------------------------------------------------------------------------
    // Shard ordering disabled
    // -----------------------------------------------------------------------------------------

    @Test
    public void testUnorderedAllUnassignedSplitsAvailableForAssignment() {
        List<KinesisShardSplit> splits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()),
                        getTestSplit(generateShardId(3), Collections.emptySet()),
                        // Shards produced by splitting parent shard
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(1))),
                        getTestSplit(generateShardId(5), Collections.singleton(generateShardId(1))),
                        // Shard produced by merging 2 parent shards
                        getTestSplit(
                                generateShardId(6),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(2), generateShardId(3)))));

        SplitTracker splitTracker = new SplitTracker(false);
        splitTracker.addSplits(splits);

        List<KinesisShardSplit> pendingSplits = splitTracker.splitsAvailableForAssignment();

        assertThat(pendingSplits).containsExactlyInAnyOrderElementsOf(splits);
    }

    @Test
    public void testUnorderedAssignedSplitsNoLongerReturnedAsAvailableToAssign() {
        List<KinesisShardSplit> assignedSplits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()),
                        getTestSplit(generateShardId(3), Collections.emptySet()));
        List<KinesisShardSplit> unassignedSplits =
                Arrays.asList(
                        // Shards produced by splitting parent shard
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(1))),
                        getTestSplit(generateShardId(5), Collections.singleton(generateShardId(1))),
                        // Shard produced by merging 2 parent shards
                        getTestSplit(
                                generateShardId(6),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(2), generateShardId(3)))));

        SplitTracker splitTracker = new SplitTracker(false);
        splitTracker.addSplits(assignedSplits);
        splitTracker.addSplits(unassignedSplits);

        splitTracker.markAsAssigned(assignedSplits);

        List<KinesisShardSplit> pendingSplits = splitTracker.splitsAvailableForAssignment();

        assertThat(pendingSplits).containsExactlyInAnyOrderElementsOf(unassignedSplits);
    }

    // -----------------------------------------------------------------------------------------
    // Shard ordering enabled
    // -----------------------------------------------------------------------------------------

    @Test
    public void testOrderedAllUnassignedSplitsWithoutParentsAvailableForAssignment() {
        List<KinesisShardSplit> splitsWithoutParents =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()),
                        getTestSplit(generateShardId(3), Collections.emptySet()));
        List<KinesisShardSplit> splitsWithParents =
                Arrays.asList(
                        // Shards produced by splitting parent shard
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(1))),
                        getTestSplit(generateShardId(5), Collections.singleton(generateShardId(1))),
                        // Shard produced by merging 2 parent shards
                        getTestSplit(
                                generateShardId(6),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(2), generateShardId(3)))));

        SplitTracker splitTracker = new SplitTracker(true);
        splitTracker.addSplits(splitsWithParents);
        splitTracker.addSplits(splitsWithoutParents);

        List<KinesisShardSplit> pendingSplits = splitTracker.splitsAvailableForAssignment();

        assertThat(pendingSplits).containsExactlyInAnyOrderElementsOf(splitsWithoutParents);
    }

    @Test
    public void testOrderedMarkingParentSplitAsFinishedMakesChildrenAvailableForAssignment() {
        List<KinesisShardSplit> splits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(0), Collections.emptySet()),
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()),
                        // Shards produced by splitting parent shard
                        getTestSplit(generateShardId(3), Collections.singleton(generateShardId(0))),
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(0))),
                        // Shard produced by merging 2 parent shards
                        getTestSplit(
                                generateShardId(5),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(1), generateShardId(2)))));

        SplitTracker splitTracker = new SplitTracker(true);
        splitTracker.addSplits(splits);

        splitTracker.markAsAssigned(splits.subList(0, 3));
        // All splits without parents were assigned, no eligible splits
        assertThat(splitTracker.splitsAvailableForAssignment()).isEmpty();

        // Split 0 has 2 children (shard split)
        splitTracker.markAsFinished(Collections.singletonList(splits.get(0).splitId()));
        assertThat(splitTracker.splitsAvailableForAssignment())
                .containsExactlyInAnyOrder(splits.get(3), splits.get(4));
    }

    @Test
    public void
            testOrderedAllParentSplitShouldBeMarkedAsFinishedForChildrenToBecomeAvailableForAssignment() {
        List<KinesisShardSplit> splits =
                Arrays.asList(
                        // Shards without parents
                        getTestSplit(generateShardId(0), Collections.emptySet()),
                        getTestSplit(generateShardId(1), Collections.emptySet()),
                        getTestSplit(generateShardId(2), Collections.emptySet()),
                        // Shards produced by splitting parent shard
                        getTestSplit(generateShardId(3), Collections.singleton(generateShardId(0))),
                        getTestSplit(generateShardId(4), Collections.singleton(generateShardId(0))),
                        // Shard produced by merging 2 parent shards
                        getTestSplit(
                                generateShardId(5),
                                new HashSet<>(
                                        Arrays.asList(generateShardId(1), generateShardId(2)))));

        SplitTracker splitTracker = new SplitTracker(true);
        splitTracker.addSplits(splits);

        splitTracker.markAsAssigned(splits.subList(0, 3));
        // All splits without parents were assigned, no eligible splits
        assertThat(splitTracker.splitsAvailableForAssignment()).isEmpty();

        // Splits 1 and 2 has 1 common child (shard merge)
        // Marking split 1 as finished would not make child available, because another parent is
        // still being read
        splitTracker.markAsFinished(Collections.singletonList(splits.get(1).splitId()));
        assertThat(splitTracker.splitsAvailableForAssignment()).isEmpty();

        // Marking split 2 as finished makes child split available since both parent are finished
        // now
        splitTracker.markAsFinished(Collections.singletonList(splits.get(2).splitId()));
        assertThat(splitTracker.splitsAvailableForAssignment()).containsExactly(splits.get(5));
    }
}
