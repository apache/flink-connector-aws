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

import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDBStreamsShardSplitWithAssignmentStatus;
import org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.OLD_SHARD_DURATION;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.STREAM_ARN;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShard;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class SplitTrackerTest {

    @Test
    public void testSplitsWithoutParentsAreAvailableToAssign() {
        SplitTracker splitTracker = new SplitTracker(STREAM_ARN, InitialPosition.TRIM_HORIZON);
        Shard shard = generateShard(0, "1200", null, null);
        splitTracker.addSplits(Collections.singletonList(shard));
        List<String> pendingSplitIds =
                splitTracker.splitsAvailableForAssignment().stream()
                        .map(split -> split.splitId())
                        .collect(Collectors.toList());
        assertThat(pendingSplitIds).containsExactly(shard.shardId());
    }

    @Test
    public void testStateSnapshot() {
        List<Shard> finishedShards =
                Arrays.asList(
                        generateShard(0, "1000", "1200", null),
                        generateShard(1, "1100", "1300", null));
        List<DynamoDbStreamsShardSplit> finishedSplits =
                finishedShards.stream().map(this::getSplitFromShard).collect(Collectors.toList());
        List<Shard> assignedShards =
                Arrays.asList(
                        generateShard(2, "1300", "1500", generateShardId(0)),
                        generateShard(3, "1500", "1700", generateShardId(1)));
        List<DynamoDbStreamsShardSplit> assignedSplits =
                assignedShards.stream().map(this::getSplitFromShard).collect(Collectors.toList());
        List<Shard> unassignedShards =
                Arrays.asList(
                        generateShard(4, "1600", null, null),
                        generateShard(5, "1700", null, generateShardId(2)),
                        generateShard(6, "1900", null, generateShardId(3)));
        List<DynamoDbStreamsShardSplit> unassignedSplits =
                unassignedShards.stream().map(this::getSplitFromShard).collect(Collectors.toList());

        List<DynamoDBStreamsShardSplitWithAssignmentStatus> expectedState =
                finishedSplits.stream()
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.FINISHED))
                        .collect(Collectors.toList());
        expectedState.addAll(
                assignedSplits.stream()
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.ASSIGNED))
                        .collect(Collectors.toList()));
        expectedState.addAll(
                unassignedSplits.stream()
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED))
                        .collect(Collectors.toList()));

        SplitTracker splitTracker = new SplitTracker(STREAM_ARN, InitialPosition.TRIM_HORIZON);
        splitTracker.addSplits(
                Stream.of(assignedShards, unassignedShards, finishedShards)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
        splitTracker.markAsAssigned(assignedSplits);
        splitTracker.markAsFinished(
                finishedShards.stream().map(Shard::shardId).collect(Collectors.toList()));

        // Verify that produced state is the same
        assertThat(splitTracker.snapshotState(0))
                .containsExactlyInAnyOrderElementsOf(expectedState);
    }

    @Test
    public void testStateRestore() {
        List<Shard> shards =
                Arrays.asList(
                        // Shards without parents
                        generateShard(1, "1200", "1300", null),
                        generateShard(2, "2200", null, null),
                        generateShard(3, "3200", null, null),
                        // Shards produced by splitting parent shard
                        generateShard(4, "1400", null, generateShardId(1)),
                        generateShard(5, "1500", null, generateShardId(1)));
        List<DynamoDbStreamsShardSplit> splits =
                shards.stream().map(this::getSplitFromShard).collect(Collectors.toList());

        Stream<DynamoDBStreamsShardSplitWithAssignmentStatus> assignedSplits =
                splits.subList(0, 3).stream()
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.ASSIGNED));
        Stream<DynamoDBStreamsShardSplitWithAssignmentStatus> unassignedSplits =
                splits.subList(3, splits.size()).stream()
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED));
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> initialState =
                Stream.concat(assignedSplits, unassignedSplits).collect(Collectors.toList());

        SplitTracker splitTracker =
                new SplitTracker(initialState, STREAM_ARN, InitialPosition.TRIM_HORIZON);

        // Verify that produced state is the same
        assertThat(splitTracker.snapshotState(0)).containsExactlyInAnyOrderElementsOf(initialState);
    }

    @Test
    public void testMarkUnknownSplitAsAssignedIsNoop() {
        List<Shard> shards =
                Arrays.asList(
                        generateShard(4, "4000", null, generateShardId(1)),
                        generateShard(5, "5000", null, generateShardId(1)),
                        generateShard(6, "6000", null, generateShardId(2)));
        List<DynamoDbStreamsShardSplit> splits =
                shards.stream().map(this::getSplitFromShard).collect(Collectors.toList());
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> initialState =
                splits.stream()
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED))
                        .collect(Collectors.toList());

        SplitTracker splitTracker =
                new SplitTracker(initialState, STREAM_ARN, InitialPosition.TRIM_HORIZON);
        List<DynamoDbStreamsShardSplit> splitsToAssign =
                Collections.singletonList(getTestSplit(generateShardId(0)));

        assertThatNoException().isThrownBy(() -> splitTracker.markAsAssigned(splitsToAssign));
    }

    @Test
    public void testMarkUnknownSplitAsFinishedIsNoop() {
        List<DynamoDbStreamsShardSplit> splits =
                Arrays.asList(
                        getTestSplit(generateShardId(4), generateShardId(1)),
                        getTestSplit(generateShardId(5), generateShardId(1)),
                        getTestSplit(generateShardId(6), generateShardId(3)));
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> initialState =
                splits.stream()
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED))
                        .collect(Collectors.toList());

        SplitTracker splitTracker =
                new SplitTracker(initialState, STREAM_ARN, InitialPosition.TRIM_HORIZON);
        List<String> splitIdsToFinish = Collections.singletonList(generateShardId(0));

        assertThatNoException().isThrownBy(() -> splitTracker.markAsFinished(splitIdsToFinish));
    }

    @Test
    public void testOrderedAllUnassignedSplitsWithoutParentsAvailableForAssignment() {
        List<Shard> shardsWithoutParents =
                Arrays.asList(
                        generateShard(1, "1000", "3000", null),
                        generateShard(2, "2000", null, null),
                        generateShard(3, "3000", null, null));

        List<DynamoDbStreamsShardSplit> splitsWithoutParents =
                shardsWithoutParents.stream()
                        .map(this::getSplitFromShard)
                        .collect(Collectors.toList());

        List<Shard> shardWithParents =
                Arrays.asList(
                        generateShard(4, "4000", null, generateShardId(1)),
                        generateShard(5, "5000", null, generateShardId(1)));

        SplitTracker splitTracker = new SplitTracker(STREAM_ARN, InitialPosition.TRIM_HORIZON);
        splitTracker.addSplits(
                Stream.of(shardWithParents, shardsWithoutParents)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));

        List<DynamoDbStreamsShardSplit> pendingSplits = splitTracker.splitsAvailableForAssignment();

        assertThat(pendingSplits).containsExactlyInAnyOrderElementsOf(splitsWithoutParents);
    }

    @Test
    public void testOrderedUnassignedChildSplitsParentsFinishedAvailableForAssignment() {
        List<Shard> finishedParentShards =
                Arrays.asList(
                        generateShard(1, "1000", "3000", null),
                        generateShard(2, "2000", "4000", null),
                        generateShard(3, "3000", "5000", null));
        List<Shard> shardsWithParents =
                Arrays.asList(
                        generateShard(4, "4000", null, generateShardId(1)),
                        generateShard(5, "5000", null, generateShardId(1)),
                        generateShard(6, "6000", null, generateShardId(2)),
                        generateShard(7, "7000", null, generateShardId(3)));
        List<Shard> shardsWithoutParents =
                Arrays.asList(
                        generateShard(8, "8000", null, null), generateShard(9, "9000", null, null));

        SplitTracker splitTracker = new SplitTracker(STREAM_ARN, InitialPosition.TRIM_HORIZON);
        splitTracker.addSplits(
                Stream.of(finishedParentShards, shardsWithParents, shardsWithParents)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
        splitTracker.markAsFinished(
                finishedParentShards.stream().map(Shard::shardId).collect(Collectors.toList()));

        List<DynamoDbStreamsShardSplit> unassignedChildSplits =
                splitTracker.getUnassignedChildSplits(
                        finishedParentShards.stream()
                                .map(Shard::shardId)
                                .collect(Collectors.toSet()));

        assertThat(unassignedChildSplits)
                .containsExactlyInAnyOrderElementsOf(
                        shardsWithParents.stream()
                                .map(this::getSplitFromShard)
                                .collect(Collectors.toList()));
        assertThat(unassignedChildSplits)
                .doesNotContainAnyElementsOf(
                        shardsWithoutParents.stream()
                                .map(this::getSplitFromShard)
                                .collect(Collectors.toList()));
    }

    @Test
    public void testOrderedUnassignedChildSplitsWithUnknownParentsFinishedAvailableForAssignment() {
        List<Shard> finishedParentShards =
                Arrays.asList(
                        generateShard(1, "1000", "3000", null),
                        generateShard(2, "2000", "4000", null),
                        generateShard(3, "3000", "5000", null));
        List<Shard> shardsWithParents =
                Arrays.asList(
                        generateShard(4, "4000", null, generateShardId(1)),
                        generateShard(5, "5000", null, generateShardId(1)),
                        generateShard(6, "6000", null, generateShardId(2)));
        List<Shard> shardsWithoutParents =
                Arrays.asList(
                        generateShard(8, "8000", null, null), generateShard(9, "9000", null, null));

        SplitTracker splitTracker = new SplitTracker(STREAM_ARN, InitialPosition.TRIM_HORIZON);
        splitTracker.addSplits(
                Stream.of(finishedParentShards, shardsWithParents, shardsWithParents)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
        splitTracker.markAsFinished(
                finishedParentShards.stream().map(Shard::shardId).collect(Collectors.toList()));

        List<DynamoDbStreamsShardSplit> unassignedChildSplits =
                splitTracker.getUnassignedChildSplits(
                        finishedParentShards.stream()
                                .map(Shard::shardId)
                                .collect(Collectors.toSet()));

        assertThat(unassignedChildSplits)
                .containsExactlyInAnyOrderElementsOf(
                        shardsWithParents.stream()
                                .map(this::getSplitFromShard)
                                .collect(Collectors.toList()));
        assertThat(unassignedChildSplits)
                .doesNotContainAnyElementsOf(
                        shardsWithoutParents.stream()
                                .map(this::getSplitFromShard)
                                .collect(Collectors.toList()));
    }

    @Test
    public void testOrderedMarkingParentSplitAsFinishedMakesChildrenAvailableForAssignment() {
        List<Shard> shards =
                Arrays.asList(
                        // Shards without parents
                        generateShard(0, "1000", "2000", null),
                        generateShard(1, "2000", null, null),
                        generateShard(2, "2200", null, null),
                        // Shards produced by splitting parent shard
                        generateShard(3, "2100", null, generateShardId(0)),
                        generateShard(4, "2400", null, generateShardId(0)));
        List<DynamoDbStreamsShardSplit> splits =
                shards.stream().map(this::getSplitFromShard).collect(Collectors.toList());

        SplitTracker splitTracker = new SplitTracker(STREAM_ARN, InitialPosition.TRIM_HORIZON);
        splitTracker.addSplits(shards);

        splitTracker.markAsAssigned(splits.subList(0, 3));
        // All splits without parents were assigned, no eligible splits
        assertThat(splitTracker.splitsAvailableForAssignment()).isEmpty();

        // Split 0 has 2 children (shard split)
        splitTracker.markAsFinished(Collections.singletonList(splits.get(0).splitId()));
        assertThat(splitTracker.splitsAvailableForAssignment())
                .containsExactlyInAnyOrder(splits.get(3), splits.get(4));
    }

    @Test
    public void testSplitTrackerCleansUpOlderFinished() {
        String firstShardId = "shardId-" + Instant.now().minus(OLD_SHARD_DURATION).toEpochMilli();
        String secondShardId =
                "shardId-" + Instant.now().minus(Duration.ofHours(23)).toEpochMilli();

        List<Shard> shards =
                Arrays.asList(
                        generateShard(firstShardId, "1000", "1500", null),
                        generateShard(1, "1200", "1300", null),
                        generateShard(2, "2000", null, null),
                        // shards produced by splitting parent shard
                        generateShard(3, "3100", null, generateShardId(0)),
                        generateShard(4, "4400", null, generateShardId(1)),
                        generateShard(secondShardId, "4100", null, firstShardId));

        SplitTracker splitTracker = new SplitTracker(STREAM_ARN, InitialPosition.TRIM_HORIZON);
        splitTracker.addSplits(shards);

        splitTracker.markAsFinished(Arrays.asList(firstShardId, secondShardId));
        splitTracker.cleanUpOldFinishedSplits(
                Stream.of(
                                generateShardId(1),
                                generateShardId(2),
                                generateShardId(3),
                                generateShardId(4))
                        .collect(Collectors.toSet()));

        // first shard was created 25 hours ago and has been finished so it should get cleaned up
        assertThat(splitTracker.getKnownSplitIds()).doesNotContain(firstShardId);

        // second shard has been finished but was created 23 hours ago, so it should not get cleaned
        // up.
        assertThat(splitTracker.getKnownSplitIds()).contains(secondShardId);
    }

    private DynamoDbStreamsShardSplit getSplitFromShard(Shard shard) {
        return new DynamoDbStreamsShardSplit(
                STREAM_ARN, shard.shardId(), StartingPosition.fromStart(), shard.parentShardId());
    }
}
