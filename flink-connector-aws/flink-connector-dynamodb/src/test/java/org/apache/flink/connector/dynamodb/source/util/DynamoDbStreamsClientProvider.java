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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsServiceClientConfiguration;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;

/** Provides {@link DynamoDbStreamsClient} with mocked DynamoDbStreams behavior. */
public class DynamoDbStreamsClientProvider {

    /**
     * An implementation of the {@link DynamoDbStreamsClient} that allows control over DynamoDb Service
     * responses.
     */
    public static class TestingDynamoDbStreamsClient implements DynamoDbStreamsClient {

        private Deque<DescribeStreamItem> describeStreamItems = new ArrayDeque<>();
        private Deque<String> shardIterators = new ArrayDeque<>();
        private Consumer<GetShardIteratorRequest> getShardIteratorValidation;
        private GetRecordsResponse getRecordsResponse;
        private Consumer<GetRecordsRequest> getRecordsValidation;
        private boolean closed = false;

        @Override
        public String serviceName() {
            return "dynamodb";
        }

        @Override
        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        public void setNextShardIterator(String shardIterator) {
            this.shardIterators.add(shardIterator);
        }

        public void setShardIteratorValidation(Consumer<GetShardIteratorRequest> validation) {
            this.getShardIteratorValidation = validation;
        }

        @Override
        public GetShardIteratorResponse getShardIterator(
                GetShardIteratorRequest getShardIteratorRequest)
                throws AwsServiceException, SdkClientException {
            getShardIteratorValidation.accept(getShardIteratorRequest);
            return GetShardIteratorResponse.builder().shardIterator(shardIterators.poll()).build();
        }

        public void setDescribeStreamResponse(List<DescribeStreamItem> items) {
            describeStreamItems.addAll(items);
        }

        @Override
        public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest)
                throws AwsServiceException, SdkClientException {

            DescribeStreamItem item = describeStreamItems.pop();

            item.validation.accept(describeStreamRequest);
            return DescribeStreamResponse.builder()
                    .streamDescription(
                            StreamDescription.builder()
                                    .shards(item.shards)
                                    .streamStatus(item.streamStatus)
                                    .streamArn(item.streamArn)
                                    .build()
                    )
                    .build();
        }

        public void setGetRecordsResponse(GetRecordsResponse getRecordsResponse) {
            this.getRecordsResponse = getRecordsResponse;
        }

        public void setGetRecordsValidation(Consumer<GetRecordsRequest> validation) {
            this.getRecordsValidation = validation;
        }

        @Override
        public GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest)
                throws AwsServiceException, SdkClientException {
            getRecordsValidation.accept(getRecordsRequest);
            return getRecordsResponse;
        }

        @Override
        public DynamoDbStreamsServiceClientConfiguration serviceClientConfiguration() {
            // This is not used
            return null;
        }
    }

    /** Data class to provide a mocked response to ListShards() calls. */
    public static class DescribeStreamItem {
        private final Consumer<DescribeStreamRequest> validation;
        private final List<Shard> shards;
        private final String nextToken;
        private final StreamStatus streamStatus;
        private final String streamArn;

        private DescribeStreamItem(
                Consumer<DescribeStreamRequest> validation, List<Shard> shards,
                String nextToken, StreamStatus streamStatus, String streamArn) {
            this.validation = validation;
            this.shards = shards;
            this.nextToken = nextToken;
            this.streamStatus = streamStatus;
            this.streamArn = streamArn;
        }

        public static Builder builder() {
            return new Builder();
        }

        /** Builder for {@link DescribeStreamItem}. */
        public static class Builder {
            private Consumer<DescribeStreamRequest> validation;
            private List<Shard> shards;
            private String nextToken;
            private StreamStatus streamStatus;
            private String streamArn;

            public Builder validation(Consumer<DescribeStreamRequest> validation) {
                this.validation = validation;
                return this;
            }

            public Builder shards(List<Shard> shards) {
                this.shards = shards;
                return this;
            }

            public Builder nextToken(String nextToken) {
                this.nextToken = nextToken;
                return this;
            }

            public Builder streamStatus(StreamStatus streamStatus) {
                this.streamStatus = streamStatus;
                return this;
            }

            public Builder streamArn(String streamArn) {
                this.streamArn = streamArn;
                return this;
            }

            public DescribeStreamItem build() {
                return new DescribeStreamItem(validation, shards, nextToken, streamStatus, streamArn);
            }
        }
    }
}
