package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.annotation.Internal;

/**
 * Assignment status of {@link org.apache.flink.connector.kinesis.source.split.KinesisShardSplit}.
 */
@Internal
public enum SplitAssignmentStatus {
    ASSIGNED(0),
    UNASSIGNED(1);

    private final int statusCode;

    SplitAssignmentStatus(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public static SplitAssignmentStatus fromStatusCode(int statusCode) {
        for (SplitAssignmentStatus status : SplitAssignmentStatus.values()) {
            if (status.getStatusCode() == statusCode) {
                return status;
            }
        }

        throw new IllegalArgumentException("Unknown status code: " + statusCode);
    }
}
