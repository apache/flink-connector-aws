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

import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsClientProvider.TestingDynamoDbStreamsClient;

import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsServiceClientConfiguration;

import java.util.concurrent.CompletableFuture;

/** Provides {@link DynamoDbStreamsAsyncClient} with mocked DynamoDbStreams behavior. */
public class DynamoDbStreamsAsyncClientProvider {

    /**
     * An implementation of the {@link DynamoDbStreamsAsyncClient} that runs each call of a {@link
     * TestingDynamoDbStreamsClient} on another thread, as the AWS SDK does. An exception from the
     * blocking client fails the future with a {@link java.util.concurrent.CompletionException}.
     */
    public static class TestingDynamoDbStreamsAsyncClient implements DynamoDbStreamsAsyncClient {
        private final TestingDynamoDbStreamsClient blockingClient;
        private boolean closed = false;

        public TestingDynamoDbStreamsAsyncClient(TestingDynamoDbStreamsClient blockingClient) {
            this.blockingClient = blockingClient;
        }

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

        @Override
        public CompletableFuture<GetShardIteratorResponse> getShardIterator(
                GetShardIteratorRequest getShardIteratorRequest) {
            return CompletableFuture.supplyAsync(
                    () -> blockingClient.getShardIterator(getShardIteratorRequest));
        }

        @Override
        public CompletableFuture<GetRecordsResponse> getRecords(
                GetRecordsRequest getRecordsRequest) {
            return CompletableFuture.supplyAsync(
                    () -> blockingClient.getRecords(getRecordsRequest));
        }

        @Override
        public CompletableFuture<DescribeStreamResponse> describeStream(
                DescribeStreamRequest describeStreamRequest) {
            return CompletableFuture.supplyAsync(
                    () -> blockingClient.describeStream(describeStreamRequest));
        }

        @Override
        public DynamoDbStreamsServiceClientConfiguration serviceClientConfiguration() {
            // This is not used
            return null;
        }
    }
}
