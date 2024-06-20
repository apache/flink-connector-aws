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

package org.apache.flink.connector.dynamodb.source.enumerator;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Interface that controls the mapping of the split to a given subtask. */
@Experimental
public interface DynamoDbStreamsShardAssigner extends Serializable {

    /**
     * Controls the assignment of a given split to a specific subtask. The {@link Context} object
     * provides immutable information for the current status of the Flink cluster (e.g. number of
     * registered readers, current split assignments).
     *
     * @param split the split to assign
     * @param context immutable information about the Flink cluster
     * @return subtaskId to assign given split
     */
    int assign(final DynamoDbStreamsShardSplit split, final Context context);

    /**
     * Immutable information about the Flink cluster useful for deciding which subtask to assign a
     * given split.
     */
    @Experimental
    interface Context {
        /**
         * Provides the current split distribution between subtasks.
         *
         * @return current assignment of splits. Key is subtaskId, value is the currently assigned
         *     splits.
         */
        Map<Integer, Set<DynamoDbStreamsShardSplit>> getCurrentSplitAssignment();

        /**
         * Provides the pending split assignments in the current batch of splits.
         *
         * @return pending split assignments. Key is subtaskId, value is the currently assigned
         *     splits.
         */
        Map<Integer, List<DynamoDbStreamsShardSplit>> getPendingSplitAssignments();

        /**
         * Provides information about the currently registered readers on the Flink cluster.
         *
         * @return currently registered readers in the Flink cluster. Key is subtaskId, value is
         *     information about the registered reader.
         */
        Map<Integer, ReaderInfo> getRegisteredReaders();
    }
}
