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

import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumerator;
import org.apache.flink.connector.dynamodb.source.util.ShardUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Class to track the state of shard graph created as part of describestream operation. This will
 * track for inconsistent shards returned due to describestream operation. Caller will have to call
 * describestream again with the chronologically first leaf node to resolve inconsistencies.
 */
public class SplitGraphInconsistencyTracker {
    private final TreeSet<String> closedLeafNodes;
    private final Map<String, Shard> nodes;
    private static final Logger LOG =
            LoggerFactory.getLogger(DynamoDbStreamsSourceEnumerator.class);

    public SplitGraphInconsistencyTracker() {
        nodes = new HashMap<>();
        closedLeafNodes = new TreeSet<>();
    }

    /**
     * Adds shards to the shard graph tracker. We first add shards and once shards are added, we
     * remove the parent shards from closedLeafNodes. We need 2x for loops here to make sure if the
     * shards are not ordered, parent and child can both reside in closedLeafNodes
     *
     * @param shards Shards to add in the tracker
     */
    public void addNodes(List<Shard> shards) {
        for (Shard shard : shards) {
            addNode(shard);
        }
        for (Shard shard : shards) {
            removeParentFromClosedLeafNodes(shard);
        }
    }

    private void removeParentFromClosedLeafNodes(Shard shard) {
        if (shard.parentShardId() != null) {
            closedLeafNodes.remove(shard.parentShardId());
        }
    }

    private void addNode(Shard shard) {
        nodes.put(shard.shardId(), shard);
        if (shard.sequenceNumberRange().endingSequenceNumber() != null) {
            closedLeafNodes.add(shard.shardId());
        }
    }

    /**
     * If any shard id is more than 24 hours old and it does not have a child, we can assume that
     * the child has been trimmed. This case is pretty common for large streams and it doesn't make
     * sense to halt the application for another expensive describestream operation.
     */
    public boolean inconsistencyDetected() {
        Set<String> closedLeafNodesCopy = new HashSet<>(closedLeafNodes);
        for (String closedLeafNodeId : closedLeafNodesCopy) {
            if (ShardUtils.isShardOlderThanInconsistencyDetectionRetentionPeriod(
                    closedLeafNodeId)) {
                LOG.warn(
                        "Shard id: {} has no child and has been created more than 24 hours ago. Not tracking it and its ancestors",
                        closedLeafNodeId);
                removeExpiredInconsistentLeaves(closedLeafNodeId);
            }
        }
        return !closedLeafNodes.isEmpty();
    }

    public String getEarliestClosedLeafNode() {
        return closedLeafNodes.first();
    }

    public Collection<Shard> getNodes() {
        return nodes.values();
    }

    private void removeExpiredInconsistentLeaves(String shardId) {
        String currentShardId = shardId;
        while (currentShardId != null && nodes.containsKey(currentShardId)) {
            Shard currentShard = nodes.remove(currentShardId);
            closedLeafNodes.remove(currentShardId);
            currentShardId = currentShard.parentShardId();
        }
    }
}
