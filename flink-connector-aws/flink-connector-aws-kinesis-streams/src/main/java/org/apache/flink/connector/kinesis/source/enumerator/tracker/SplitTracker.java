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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardSplitWithAssignmentStatus;
import org.apache.flink.connector.kinesis.source.enumerator.SplitAssignmentStatus;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** This class is used to track shard hierarchy. */
@Internal
public class SplitTracker {
    /**
     * Flag controlling if tracker should wait before all parent splits will be completed before
     * assigning split to readers.
     */
    private final boolean preserveShardOrdering;

    /** Map of all discovered splits that have not been completed. Split id is used as a key. */
    private final Map<String, KinesisShardSplit> knownSplits = new ConcurrentHashMap<>();

    /** Set of ids for currently assigned splits. */
    private final Set<String> assignedSplits = new HashSet<>();

    public SplitTracker(boolean preserveShardOrdering) {
        this(preserveShardOrdering, Collections.emptyList());
    }

    public SplitTracker(
            boolean preserveShardOrdering,
            List<KinesisShardSplitWithAssignmentStatus> initialState) {
        this.preserveShardOrdering = preserveShardOrdering;

        initialState.forEach(
                splitWithStatus -> {
                    knownSplits.put(splitWithStatus.split().splitId(), splitWithStatus.split());
                    if (SplitAssignmentStatus.ASSIGNED.equals(splitWithStatus.assignmentStatus())) {
                        assignedSplits.add(splitWithStatus.split().splitId());
                    }
                });
    }

    /**
     * Add newly discovered splits to tracker.
     *
     * @param splitsToAdd collection of splits to add to tracking
     */
    public void addSplits(Collection<KinesisShardSplit> splitsToAdd) {
        splitsToAdd.forEach(split -> knownSplits.put(split.splitId(), split));
    }

    /**
     * Mark splits as assigned. Assigned splits will no longer be returned as pending splits.
     *
     * @param splitsToAssign collection of splits to mark as assigned
     */
    public void markAsAssigned(Collection<KinesisShardSplit> splitsToAssign) {
        splitsToAssign.forEach(split -> assignedSplits.add(split.splitId()));
    }

    /**
     * Mark splits with specified ids as finished.
     *
     * @param finishedSplitIds collection of split ids to mark as finished
     */
    public void markAsFinished(Collection<String> finishedSplitIds) {
        finishedSplitIds.forEach(
                splitId -> {
                    assignedSplits.remove(splitId);
                    knownSplits.remove(splitId);
                });
    }

    /**
     * Checks if split with specified id had been assigned to the reader.
     *
     * @param splitId split id
     * @return {@code true} if split had been assigned, otherwise {@code false}
     */
    public boolean isAssigned(String splitId) {
        return assignedSplits.contains(splitId);
    }

    /**
     * Returns list of splits that can be assigned to readers. Does not include splits that are
     * already assigned or finished. If shard ordering is enabled, only splits with finished parents
     * will be returned.
     *
     * @return list of splits that can be assigned to readers.
     */
    public List<KinesisShardSplit> splitsAvailableForAssignment() {
        return knownSplits.values().stream()
                .filter(
                        split -> {
                            boolean splitIsNotAssigned = !isAssigned(split.splitId());
                            if (preserveShardOrdering) {
                                // Check if all parent splits were finished
                                return splitIsNotAssigned
                                        && verifyAllParentSplitsAreFinished(split);
                            } else {
                                return splitIsNotAssigned;
                            }
                        })
                .collect(Collectors.toList());
    }

    /**
     * Prepare split graph representation to store in state. Method returns only splits that are
     * currently assigned to readers or unassigned. Finished splits are not included in the result.
     *
     * @param checkpointId id of the checkpoint
     * @return list of splits with current assignment status
     */
    public List<KinesisShardSplitWithAssignmentStatus> snapshotState(long checkpointId) {
        return knownSplits.values().stream()
                .map(
                        split -> {
                            SplitAssignmentStatus assignmentStatus =
                                    isAssigned(split.splitId())
                                            ? SplitAssignmentStatus.ASSIGNED
                                            : SplitAssignmentStatus.UNASSIGNED;
                            return new KinesisShardSplitWithAssignmentStatus(
                                    split, assignmentStatus);
                        })
                .collect(Collectors.toList());
    }

    private boolean verifyAllParentSplitsAreFinished(KinesisShardSplit split) {
        boolean allParentsFinished = true;
        for (String parentSplitId : split.getParentShardIds()) {
            allParentsFinished = allParentsFinished && isFinished(parentSplitId);
        }

        return allParentsFinished;
    }

    private boolean isFinished(String splitId) {
        return !knownSplits.containsKey(splitId);
    }
}
