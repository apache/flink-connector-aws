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
