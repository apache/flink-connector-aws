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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumerator;

import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Contains information about the kinesis stream and shard. Serves as a communication mechanism
 * between the {@link KinesisStreamsSourceEnumerator} and the {@link SplitReader}. Information
 * provided here should be immutable. This class is stored in state, so any changes need to be
 * backwards compatible.
 */
@Internal
public final class KinesisShardSplit implements SourceSplit {

    private final String streamArn;
    private final String shardId;
    private final StartingPosition startingPosition;

    public KinesisShardSplit(String streamArn, String shardId, StartingPosition startingPosition) {
        Preconditions.checkNotNull(streamArn, "streamArn cannot be null");
        Preconditions.checkNotNull(shardId, "shardId cannot be null");
        Preconditions.checkNotNull(startingPosition, "startingPosition cannot be null");

        this.streamArn = streamArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
    }

    @Override
    public String splitId() {
        return shardId;
    }

    public String getStreamArn() {
        return streamArn;
    }

    public String getShardId() {
        return shardId;
    }

    public StartingPosition getStartingPosition() {
        return startingPosition;
    }

    @Override
    public String toString() {
        return "KinesisShardSplit{"
                + "streamArn='"
                + streamArn
                + '\''
                + ", shardId='"
                + shardId
                + '\''
                + ", startingPosition="
                + startingPosition
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
        KinesisShardSplit that = (KinesisShardSplit) o;
        return Objects.equals(streamArn, that.streamArn)
                && Objects.equals(shardId, that.shardId)
                && Objects.equals(startingPosition, that.startingPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamArn, shardId, startingPosition);
    }
}
