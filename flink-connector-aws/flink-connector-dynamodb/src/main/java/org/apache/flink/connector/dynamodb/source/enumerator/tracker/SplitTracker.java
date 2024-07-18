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

import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON;
import static org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus.ASSIGNED;
import static org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus.FINISHED;

/** This class is used to track splits and will be used to assign any unassigned splits. */
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
        List<DynamoDbStreamsShardSplit> newSplitsToAdd =
                determineNewShardsToBePutForAssignment(shardsToAdd);
        newSplitsToAdd.forEach(
                split -> {
                    knownSplits.put(split.splitId(), split);
                });
    }

    private List<Shard> getOpenShards(Collection<Shard> shards) {
        List<Shard> openShards = new ArrayList<>();
        for (Shard shard : shards) {
            if (shard.sequenceNumberRange().endingSequenceNumber() == null) {
                openShards.add(shard);
            }
        }
        return openShards;
    }

    private Map<String, Shard> getShardIdToShardMap(Collection<Shard> shards) {
        Map<String, Shard> shardIdToShardMap = new HashMap<>();
        for (Shard shard : shards) {
            shardIdToShardMap.put(shard.shardId(), shard);
        }
        return shardIdToShardMap;
    }

    /**
     * This function finds the open shards returned from describeStream operation and adds them
     * along with their parents if parents are not already tracked to the tracked splits
     *
     * <p>This is needed because describestream has an inconsistency where for example if a shard s
     * is split into s1 and s2, in one describestream operation, its possible that only one of s1
     * and s2 is returned.
     *
     * <p>We will go up the shard lineage until we find a parent shard which is not yet tracked by
     * SplitTracker If no ancestor is tracked, the first ancestor will be read from the initial
     * position configured and all its descendants will be read from TRIM_HORIZON
     *
     * @param shards the shards returned from DescribeStream operation
     * @return list of {@link DynamoDbStreamsShardSplit} which will be put to tracked splits
     */
    private List<DynamoDbStreamsShardSplit> determineNewShardsToBePutForAssignment(
            Collection<Shard> shards) {
        Map<String, Shard> shardIdToShardMap = getShardIdToShardMap(shards);
        List<Shard> openShards = getOpenShards(shards);
        List<DynamoDbStreamsShardSplit> newSplitsToBeTracked = new ArrayList<>();
        Map<String, Boolean> memoizationContext = new HashMap<>();
        for (Shard openShard : openShards) {
            String shardId = openShard.shardId();
            if (!knownSplits.containsKey(shardId)) {
                boolean isDescendant =
                        checkIfShardIsDescendantAndAddAncestorsToBeTracked(
                                openShard.shardId(),
                                shardIdToShardMap,
                                newSplitsToBeTracked,
                                memoizationContext);
                if (isDescendant) {
                    newSplitsToBeTracked.add(mapToSplit(openShard, TRIM_HORIZON));
                } else {
                    newSplitsToBeTracked.add(mapToSplit(openShard, initialPosition));
                }
            }
        }
        return newSplitsToBeTracked;
    }

    /**
     * Check if any ancestor shard of the current shard has not been tracked yet. Take this example:
     * 0->3->8, 0->4->9, 1->5, 1->6, 2->7
     *
     * <p>At epoch 1, the lineage looked like this due to describestream inconsistency 0->3, 1->5,
     * 1->6, 2->7 knownSplits = 0,1,2,3,5,6,7 After a few describestream calls, at epoch 2, after
     * the whole lineage got discovered, since 4 was not tracked, we should start tracking 4 also.
     * knownSplits = 0,1,2,3,4,5,6,7,8,9.
     */
    private boolean checkIfShardIsDescendantAndAddAncestorsToBeTracked(
            String shardId,
            Map<String, Shard> shardIdToShardMap,
            List<DynamoDbStreamsShardSplit> newSplitsToBeTracked,
            Map<String, Boolean> memoizationContext) {
        Boolean previousValue = memoizationContext.get(shardId);
        if (previousValue != null) {
            return previousValue;
        }

        if (shardId != null && shardIdToShardMap.containsKey(shardId)) {
            if (knownSplits.containsKey(shardId)) {
                return true;
            } else {
                Shard shard = shardIdToShardMap.get(shardId);
                String parentShardId = shard.parentShardId();
                boolean isParentShardDescendant =
                        checkIfShardIsDescendantAndAddAncestorsToBeTracked(
                                parentShardId,
                                shardIdToShardMap,
                                newSplitsToBeTracked,
                                memoizationContext);
                if (shardIdToShardMap.containsKey(parentShardId)) {
                    if (!knownSplits.containsKey(parentShardId)) {
                        Shard parentShard = shardIdToShardMap.get(parentShardId);
                        if (isParentShardDescendant) {
                            newSplitsToBeTracked.add(mapToSplit(parentShard, TRIM_HORIZON));
                        } else {
                            newSplitsToBeTracked.add(mapToSplit(parentShard, initialPosition));
                        }
                        return true;
                    }
                }
            }
        }

        return false;
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

        Set<String> parentShardIds = new HashSet<>();
        if (shard.parentShardId() != null) {
            parentShardIds.add(shard.parentShardId());
        }
        return new DynamoDbStreamsShardSplit(
                streamArn, shard.shardId(), startingPosition, parentShardIds);
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
     * @param splitsToFinish collection of splits to mark as assigned
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
                                    && (verifyAllParentSplitsAreFinished(split)
                                            || verifyAllParentSplitsAreCleanedUp(split));
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
    public void cleanupGarbageSplits(Set<String> discoveredSplitIds) {
        Set<String> finishedSplitsSnapshot = new HashSet<>(finishedSplits);
        for (String finishedSplitId : finishedSplitsSnapshot) {
            DynamoDbStreamsShardSplit finishedSplit = knownSplits.get(finishedSplitId);
            if (verifyAllParentSplitsAreFinished(finishedSplit)
                    || verifyAllParentSplitsAreCleanedUp(finishedSplit)
                            && !discoveredSplitIds.contains(finishedSplitId)) {
                finishedSplits.remove(finishedSplitId);
                knownSplits.remove(finishedSplitId);
            }
        }
    }

    private boolean verifyAllParentSplitsAreCleanedUp(DynamoDbStreamsShardSplit split) {
        boolean allParentsCleanedUp = true;
        for (String parentSplitId : split.getParentShardIds()) {
            allParentsCleanedUp = allParentsCleanedUp && !knownSplits.containsKey(parentSplitId);
        }
        return allParentsCleanedUp;
    }

    private boolean verifyAllParentSplitsAreFinished(DynamoDbStreamsShardSplit split) {
        boolean allParentsAreFinished = true;
        for (String parentSplitId : split.getParentShardIds()) {
            allParentsAreFinished = allParentsAreFinished && isFinished(parentSplitId);
        }
        return allParentsAreFinished;
    }

    private boolean isFinished(String splitId) {
        return finishedSplits.contains(splitId);
    }
}
