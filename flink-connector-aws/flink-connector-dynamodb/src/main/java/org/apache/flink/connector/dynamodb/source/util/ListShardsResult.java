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

package org.apache.flink.connector.dynamodb.source.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.source.proxy.DynamoDbStreamsProxy;

import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Class to store the shards and status of the stream from the response of DescribeStream for
 * DynamoDB Source coming from {@link DynamoDbStreamsProxy}.
 */
@Internal
public class ListShardsResult {
    private final List<Shard> shards;
    private StreamStatus streamStatus;
    private boolean inconsistencyDetected;

    public ListShardsResult() {
        this.shards = new ArrayList<>();
        this.streamStatus = StreamStatus.ENABLED;
        this.inconsistencyDetected = false;
    }

    public void addShards(List<Shard> shardList) {
        this.shards.addAll(shardList);
    }

    public void setStreamStatus(StreamStatus streamStatus) {
        this.streamStatus = streamStatus;
    }

    public void setInconsistencyDetected(boolean inconsistencyDetected) {
        this.inconsistencyDetected = inconsistencyDetected;
    }

    public List<Shard> getShards() {
        return this.shards;
    }

    public StreamStatus getStreamStatus() {
        return this.streamStatus;
    }

    public boolean getInconsistencyDetected() {
        return this.inconsistencyDetected;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ListShardsResult that = (ListShardsResult) o;
        return Objects.equals(shards, that.shards)
                && Objects.equals(streamStatus, that.getStreamStatus())
                && Objects.equals(inconsistencyDetected, that.inconsistencyDetected);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shards, streamStatus, inconsistencyDetected);
    }

    @Override
    public String toString() {
        return "ListShardsResult{"
                + "shards="
                + shards
                + ", streamStatus="
                + streamStatus.toString()
                + ", inconsistencyDetected="
                + inconsistencyDetected
                + "}";
    }
}
