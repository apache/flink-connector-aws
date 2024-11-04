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

package org.apache.flink.connector.kinesis.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Instant;

/**
 * Util class with methods relating to {@link KinesisStreamsSource}'s internal representation of
 * {@link StartingPosition}.
 */
@Internal
public class StartingPositionUtil {

    private StartingPositionUtil() {
        // prevent initialization of util class.
    }

    /**
     * @param startingPosition {@link KinesisStreamsSource}'s internal representation of {@link
     *     StartingPosition}
     * @return AWS SDK's representation of {@link StartingPosition}.
     */
    public static software.amazon.awssdk.services.kinesis.model.StartingPosition
            toSdkStartingPosition(StartingPosition startingPosition) {
        ShardIteratorType shardIteratorType = startingPosition.getShardIteratorType();
        Object startingMarker = startingPosition.getStartingMarker();

        software.amazon.awssdk.services.kinesis.model.StartingPosition.Builder builder =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder()
                        .type(shardIteratorType);

        switch (shardIteratorType) {
            case LATEST:
            case TRIM_HORIZON:
                return builder.type(shardIteratorType).build();
            case AT_TIMESTAMP:
                Preconditions.checkArgument(
                        startingMarker instanceof Instant,
                        "Invalid StartingPosition. When ShardIteratorType is AT_TIMESTAMP, startingMarker must be an Instant.");
                return builder.timestamp((Instant) startingMarker).build();
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                Preconditions.checkArgument(
                        startingMarker instanceof String,
                        "Invalid StartingPosition. When ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER, startingMarker must be a String.");
                return builder.sequenceNumber((String) startingMarker).build();
        }
        throw new IllegalArgumentException("Unsupported shardIteratorType " + shardIteratorType);
    }
}
