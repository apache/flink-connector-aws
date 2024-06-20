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

package org.apache.flink.connector.dynamodb.source.split;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

import javax.annotation.Nullable;

import java.util.Objects;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.LATEST;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

/** Data class indicating the starting position for reading a given shard. */
@Internal
public final class StartingPosition {

    private final ShardIteratorType shardIteratorType;
    private final Object startingMarker;

    StartingPosition(ShardIteratorType shardIteratorType, Object startingMarker) {
        this.shardIteratorType = shardIteratorType;
        this.startingMarker = startingMarker;
    }

    public ShardIteratorType getShardIteratorType() {
        return shardIteratorType;
    }

    @Nullable
    public Object getStartingMarker() {
        return startingMarker;
    }

    public static StartingPosition continueFromSequenceNumber(final String sequenceNumber) {
        return new StartingPosition(AFTER_SEQUENCE_NUMBER, sequenceNumber);
    }

    public static StartingPosition fromStart() {
        return new StartingPosition(TRIM_HORIZON, null);
    }

    public static StartingPosition latest() {
        return new StartingPosition(LATEST, null);
    }

    @Override
    public String toString() {
        return "StartingPosition{"
                + "shardIteratorType="
                + shardIteratorType
                + ", startingMarker="
                + startingMarker
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartingPosition that = (StartingPosition) o;
        return shardIteratorType == that.shardIteratorType
                && Objects.equals(startingMarker, that.startingMarker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardIteratorType, startingMarker);
    }
}
