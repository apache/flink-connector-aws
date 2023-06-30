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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardAssigner;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** An implementation of the {@link KinesisShardAssigner} that assigns splits uniformly. */
@Internal
public class UniformShardAssigner implements KinesisShardAssigner {
    @Override
    public int assign(KinesisShardSplit split, Context context) {
        int selectedSubtask = -1;
        int curMinAssignment = Integer.MAX_VALUE;
        Map<Integer, Set<KinesisShardSplit>> splitAssignment = context.getCurrentSplitAssignment();
        Map<Integer, List<KinesisShardSplit>> pendingSplitAssignments =
                context.getPendingSplitAssignments();

        for (int subtaskId : context.getRegisteredReaders().keySet()) {
            int subtaskAssignmentSize =
                    splitAssignment.getOrDefault(subtaskId, Collections.emptySet()).size()
                            + pendingSplitAssignments
                                    .getOrDefault(subtaskId, Collections.emptyList())
                                    .size();
            if (subtaskAssignmentSize < curMinAssignment) {
                curMinAssignment = subtaskAssignmentSize;
                selectedSubtask = subtaskId;
            }
        }

        Preconditions.checkArgument(
                selectedSubtask != -1,
                "Expected at least one registered reader. Unable to assign split.");
        return selectedSubtask;
    }
}
