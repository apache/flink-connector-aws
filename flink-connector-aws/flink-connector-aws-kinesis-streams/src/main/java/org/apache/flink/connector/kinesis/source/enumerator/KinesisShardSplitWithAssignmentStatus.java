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

package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;

import java.util.Objects;

/** Kinesis shard split with assignment status. */
@Internal
public class KinesisShardSplitWithAssignmentStatus {
    private final KinesisShardSplit kinesisShardSplit;
    private final SplitAssignmentStatus splitAssignmentStatus;

    public KinesisShardSplitWithAssignmentStatus(
            KinesisShardSplit kinesisShardSplit, SplitAssignmentStatus splitAssignmentStatus) {
        this.kinesisShardSplit = kinesisShardSplit;
        this.splitAssignmentStatus = splitAssignmentStatus;
    }

    public KinesisShardSplit split() {
        return kinesisShardSplit;
    }

    public SplitAssignmentStatus assignmentStatus() {
        return splitAssignmentStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KinesisShardSplitWithAssignmentStatus that = (KinesisShardSplitWithAssignmentStatus) o;
        return Objects.equals(kinesisShardSplit, that.kinesisShardSplit)
                && Objects.equals(splitAssignmentStatus, that.splitAssignmentStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kinesisShardSplit, splitAssignmentStatus);
    }

    @Override
    public String toString() {
        return "KinesisShardSplitWithAssignmentStatus{"
                + "kinesisShardSplit="
                + kinesisShardSplit
                + ", splitAssignmentStatus="
                + splitAssignmentStatus
                + '}';
    }
}
