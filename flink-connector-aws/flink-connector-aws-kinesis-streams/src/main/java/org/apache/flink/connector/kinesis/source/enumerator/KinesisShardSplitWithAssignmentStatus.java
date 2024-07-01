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
}
