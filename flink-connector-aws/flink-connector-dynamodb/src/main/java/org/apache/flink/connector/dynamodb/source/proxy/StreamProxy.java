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

package org.apache.flink.connector.dynamodb.source.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.ListShardsResult;

import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.ShardFilter;

import javax.annotation.Nullable;

import java.io.Closeable;

/** Interface for a StreamProxy to interact with Streams service in a given region. */
@Internal
public interface StreamProxy extends Closeable {
    /**
     * Obtains the shards associated with a given stream.
     *
     * @param streamArn the ARN of the stream
     * @param lastSeenShardId the last seen shard Id. Used for reducing the number of results
     *     returned.
     * @return shard list
     */
    ListShardsResult listShards(String streamArn, @Nullable String lastSeenShardId);

    /**
     * Obtains the child shards of a given shard, filtered by the {@link ShardFilter}.
     *
     * @param streamArn the ARN of the stream
     * @param shardFilter the filter to apply
     * @return shard list
     */
    ListShardsResult listShardsWithFilter(String streamArn, ShardFilter shardFilter);

    /**
     * Retrieves records from the stream.
     *
     * @param streamArn the ARN of the stream
     * @param shardId the shard to subscribe from
     * @param startingPosition the starting position to read from
     * @return the response with records. Includes both the returned records and the subsequent
     *     shard iterator to use.
     */
    GetRecordsResponse getRecords(
            String streamArn, String shardId, StartingPosition startingPosition);
}
