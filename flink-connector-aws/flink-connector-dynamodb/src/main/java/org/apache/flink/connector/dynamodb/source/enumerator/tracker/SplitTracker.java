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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDBStreamsShardSplitWithAssignmentStatus;
import org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.ShardUtils;

import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON;
import static org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus.ASSIGNED;
import static org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus.FINISHED;

/**
 * This class is used to track splits and will be used to assign any unassigned splits. It also
 * ensures that the parent-child shard ordering is maintained.
 */
@Internal
public class SplitTracker {
    private final Map<String, DynamoDbStreamsShardSplit> knownSplits = new ConcurrentHashMap<>();
    private final Set<String> assignedSplits = new HashSet<>();
    private final Set<String> finishedSplits = new HashSet<>();
    private final String streamArn;
    private final InitialPosition initialPosition;

    public SplitTracker(String streamArn, InitialPosition initialPosition) {
        this(Collections.emptyList(), streamArn, initialPosition);
    }

    public SplitTracker(
            List<DynamoDBStreamsShardSplitWithAssignmentStatus> initialState,
            String streamArn,
            DynamodbStreamsSourceConfigConstants.InitialPosition initialPosition) {
        this.streamArn = streamArn;
        this.initialPosition = initialPosition;
        initialState.forEach(
                splitWithStatus -> {
                    DynamoDbStreamsShardSplit currentSplit = splitWithStatus.split();
                    knownSplits.put(currentSplit.splitId(), currentSplit);

                    if (ASSIGNED.equals(splitWithStatus.assignmentStatus())) {
                        assignedSplits.add(splitWithStatus.split().splitId());
                    }
                    if (FINISHED.equals(splitWithStatus.assignmentStatus())) {
                        finishedSplits.add(splitWithStatus.split().splitId());
                    }
                });
    }

    /**
     * Add newly discovered splits to tracker.
     *
     * @param shardsToAdd collection of splits to add to tracking
     */
    public void addSplits(Collection<Shard> shardsToAdd) {
        for (Shard shard : shardsToAdd) {
            String shardId = shard.shardId();
            if (!knownSplits.containsKey(shardId)) {
                DynamoDbStreamsShardSplit newSplit = mapToSplit(shard, getStartingPosition(shard));
                knownSplits.put(shardId, newSplit);
            }
        }
    }

    /**
     * If there is no parent in knownSplits of the current shard, that means that the parent has
     * been read already, so we start the current shard with the specified initial position. We do
     * this because we can't really differentiate the case of read from an expired snapshot where
     * there is a node with parent which was trimmed but a grandparent vs a happy case where there
     * is a node with no parent. This means that for cases when we are reading from an expired
     * snapshot, we'll start the first node in the lineage with LATEST shard iterator type.
     *
     * @param shard Shard whose starting position has to be determined.
     */
    private InitialPosition getStartingPosition(Shard shard) {
        if (shard.parentShardId() == null) {
            return initialPosition;
        }
        if (!knownSplits.containsKey(shard.parentShardId())) {
            return initialPosition;
        }
        return TRIM_HORIZON;
    }

    private DynamoDbStreamsShardSplit mapToSplit(
            Shard shard, DynamodbStreamsSourceConfigConstants.InitialPosition initialPosition) {
        StartingPosition startingPosition;
        switch (initialPosition) {
            case LATEST:
                startingPosition = StartingPosition.latest();
                break;
            case TRIM_HORIZON:
            default:
                startingPosition = StartingPosition.fromStart();
        }
        return new DynamoDbStreamsShardSplit(
                streamArn, shard.shardId(), startingPosition, shard.parentShardId());
    }

    /**
     * Mark splits as assigned. Assigned splits will no longer be returned as pending splits.
     *
     * @param splitsToAssign collection of splits to mark as assigned
     */
    public void markAsAssigned(Collection<DynamoDbStreamsShardSplit> splitsToAssign) {
        splitsToAssign.forEach(split -> assignedSplits.add(split.splitId()));
    }

    /**
     * Mark splits as finished. Assigned splits will no longer be returned as pending splits.
     *
     * @param splitsToFinish collection of splits to mark as finished
     */
    public void markAsFinished(Collection<String> splitsToFinish) {
        splitsToFinish.forEach(
                splitId -> {
                    finishedSplits.add(splitId);
                    assignedSplits.remove(splitId);
                });
    }

    public boolean isAssigned(String splitId) {
        return assignedSplits.contains(splitId);
    }

    /**
     * Since we never put an inconsistent shard lineage to splitTracker, so if a shard's parent is
     * not there, that means that that should already be cleaned up.
     */
    public List<DynamoDbStreamsShardSplit> splitsAvailableForAssignment() {
        return knownSplits.values().stream()
                .filter(
                        split -> {
                            boolean splitIsNotAssigned = !isAssigned(split.splitId());
                            return splitIsNotAssigned
                                    && !isFinished(split.splitId())
                                    && verifyParentIsEitherFinishedOrCleanedUp(split);
                        })
                .collect(Collectors.toList());
    }

    public List<DynamoDBStreamsShardSplitWithAssignmentStatus> snapshotState(long checkpointId) {
        return knownSplits.values().stream()
                .map(
                        split -> {
                            SplitAssignmentStatus assignmentStatus =
                                    SplitAssignmentStatus.UNASSIGNED;
                            if (isAssigned(split.splitId())) {
                                assignmentStatus = ASSIGNED;
                            } else if (isFinished(split.splitId())) {
                                assignmentStatus = SplitAssignmentStatus.FINISHED;
                            }
                            return new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                    split, assignmentStatus);
                        })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public Set<String> getKnownSplitIds() {
        return knownSplits.keySet();
    }

    /**
     * finishedSplits needs to be cleaned up. The logic applied to cleaning up finished splits is
     * that if any split has been finished reading, its parent has been finished reading and it is
     * no longer present in the describestream response, it means that the split can be cleaned up.
     */
    public void cleanUpOldFinishedSplits(Set<String> discoveredSplitIds) {
        Set<String> finishedSplitsSnapshot = new HashSet<>(finishedSplits);
        for (String finishedSplitId : finishedSplitsSnapshot) {
            DynamoDbStreamsShardSplit finishedSplit = knownSplits.get(finishedSplitId);
            if (isSplitReadyToBeCleanedUp(finishedSplit, discoveredSplitIds)) {
                finishedSplits.remove(finishedSplitId);
                knownSplits.remove(finishedSplitId);
            }
        }
    }

    private boolean isSplitReadyToBeCleanedUp(
            DynamoDbStreamsShardSplit finishedSplit, Set<String> discoveredSplitIds) {
        String splitId = finishedSplit.splitId();
        boolean parentIsFinishedOrCleanedUp =
                verifyParentIsEitherFinishedOrCleanedUp(finishedSplit);
        boolean isAnOldSplit = ShardUtils.isShardOlderThanRetentionPeriod(splitId);
        return parentIsFinishedOrCleanedUp && !discoveredSplitIds.contains(splitId) && isAnOldSplit;
    }

    private boolean verifyParentIsEitherFinishedOrCleanedUp(DynamoDbStreamsShardSplit split) {
        if (split.getParentShardId() == null) {
            return true;
        }
        return !knownSplits.containsKey(split.getParentShardId())
                || isFinished(split.getParentShardId());
    }

    private boolean isFinished(String splitId) {
        return finishedSplits.contains(splitId);
    }
}
