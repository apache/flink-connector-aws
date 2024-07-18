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

import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Class to track the state of shard graph created as part of describestream operation. This will
 * track for inconsistent shards returned due to describestream operation. Caller will have to call
 * describestream again with the chronologically first leaf node to resolve inconsistencies.
 */
public class ShardGraphTracker {
    private final TreeSet<String> closedLeafNodes;
    private final Map<String, Shard> nodes;

    public ShardGraphTracker() {
        nodes = new HashMap<>();
        closedLeafNodes = new TreeSet<>();
    }

    public void addNodes(List<Shard> shards) {
        for (Shard shard : shards) {
            addNode(shard);
        }
    }

    private void addNode(Shard shard) {
        nodes.put(shard.shardId(), shard);
        if (shard.sequenceNumberRange().endingSequenceNumber() != null) {
            closedLeafNodes.add(shard.shardId());
        }
        if (shard.parentShardId() != null) {
            closedLeafNodes.remove(shard.parentShardId());
        }
    }

    public boolean inconsistencyDetected() {
        return !closedLeafNodes.isEmpty();
    }

    public String getEarliestClosedLeafNode() {
        return closedLeafNodes.first();
    }

    public Collection<Shard> getNodes() {
        return nodes.values();
    }
}
