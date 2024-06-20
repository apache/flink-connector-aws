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
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsServiceClientConfiguration;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

/** Provides {@link DynamoDbStreamsClient} with mocked DynamoDbStreams behavior. */
public class DynamoDbStreamsClientProvider {

    /**
     * An implementation of the {@link DynamoDbStreamsClient} that allows control over DynamoDb
     * Service responses.
     */
    public static class TestingDynamoDbStreamsClient implements DynamoDbStreamsClient {
        private Deque<String> shardIterators = new ArrayDeque<>();
        private Consumer<GetShardIteratorRequest> getShardIteratorValidation;
        private GetRecordsResponse getRecordsResponse;
        private DescribeStreamResponse describeStreamResponse;
        private Consumer<GetRecordsRequest> getRecordsValidation;
        private Consumer<DescribeStreamRequest> describeStreamValidation;
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

        @Override
        public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest)
                throws AwsServiceException, SdkClientException {

            describeStreamValidation.accept(describeStreamRequest);
            return describeStreamResponse;
        }

        public void setDescribeStreamValidation(Consumer<DescribeStreamRequest> validation) {
            this.describeStreamValidation = validation;
        }

        public void setDescribeStreamResponse(DescribeStreamResponse describeStreamResponse) {
            this.describeStreamResponse = describeStreamResponse;
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
}
