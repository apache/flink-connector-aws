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

package org.apache.flink.connector.kinesis.source.util;

import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Provides {@link StreamProxy} with mocked Kinesis Streams behavior. */
public class KinesisStreamProxyProvider {

    public static TestKinesisStreamProxy getTestStreamProxy() {
        return new TestKinesisStreamProxy();
    }

    /**
     * An implementation of the {@link StreamProxy} that allows control over shard and record
     * behavior.
     */
    public static class TestKinesisStreamProxy implements StreamProxy {

        // List shards configuration
        private final List<Shard> shards = new ArrayList<>();
        private Supplier<Exception> listShardsExceptionSupplier;
        private boolean shouldRespectLastSeenShardId = true;
        private String lastProvidedLastSeenShardId;

        // GetRecords configuration
        private final Map<ShardHandle, Deque<List<Record>>> storedRecords = new HashMap<>();
        private boolean shouldCompleteNextShard = false;
        private boolean closed = false;

        @Override
        public List<Shard> listShards(String streamArn, @Nullable String lastSeenShardId) {
            this.lastProvidedLastSeenShardId = lastSeenShardId;

            if (listShardsExceptionSupplier != null) {
                try {
                    throw listShardsExceptionSupplier.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            List<Shard> results = new ArrayList<>();
            for (Shard shard : shards) {
                if (shouldRespectLastSeenShardId && shard.shardId().equals(lastSeenShardId)) {
                    results.clear();
                    continue;
                }
                results.add(shard);
            }
            return results;
        }

        @Override
        public GetRecordsResponse getRecords(
                String streamArn, String shardId, StartingPosition startingPosition) {
            ShardHandle shardHandle = new ShardHandle(streamArn, shardId);

            List<Record> records = null;
            if (storedRecords.containsKey(shardHandle)) {
                records = storedRecords.get(shardHandle).poll();
            }

            return GetRecordsResponse.builder()
                    .records(records)
                    .nextShardIterator(shouldCompleteNextShard ? null : "some-shard-iterator")
                    .build();
        }

        public String getLastProvidedLastSeenShardId() {
            return lastProvidedLastSeenShardId;
        }

        public void addShards(String... shardIds) {
            for (String shardId : shardIds) {
                shards.add(Shard.builder().shardId(shardId).build());
            }
        }

        public void setListShardsExceptionSupplier(Supplier<Exception> exceptionSupplier) {
            listShardsExceptionSupplier = exceptionSupplier;
        }

        public void setShouldRespectLastSeenShardId(boolean shouldRespectLastSeenShardId) {
            this.shouldRespectLastSeenShardId = shouldRespectLastSeenShardId;
        }

        public void addRecords(String streamArn, String shardId, List<Record> records) {
            Deque<List<Record>> recordsQueue = new ArrayDeque<>();
            recordsQueue.add(records);
            storedRecords.merge(
                    new ShardHandle(streamArn, shardId), recordsQueue, this::mergeQueues);
        }

        private <T> Deque<T> mergeQueues(Deque<T> q1, Deque<T> q2) {
            return Stream.of(q1, q2)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toCollection(ArrayDeque::new));
        }

        public void setShouldCompleteNextShard(boolean shouldCompleteNextShard) {
            this.shouldCompleteNextShard = shouldCompleteNextShard;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        private static class ShardHandle {
            private final String streamArn;
            private final String shardId;

            public ShardHandle(String streamArn, String shardId) {
                this.streamArn = streamArn;
                this.shardId = shardId;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                ShardHandle that = (ShardHandle) o;

                return new EqualsBuilder()
                        .append(streamArn, that.streamArn)
                        .append(shardId, that.shardId)
                        .isEquals();
            }

            @Override
            public int hashCode() {
                return new HashCodeBuilder(17, 37).append(streamArn).append(shardId).toHashCode();
            }
        }
    }
}
