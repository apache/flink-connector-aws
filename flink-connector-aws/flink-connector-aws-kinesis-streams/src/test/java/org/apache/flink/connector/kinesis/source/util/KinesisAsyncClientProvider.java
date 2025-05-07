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

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.concurrent.CompletableFuture;

/** Provides {@link KinesisClient} with mocked Kinesis Stream behavior. */
public class KinesisAsyncClientProvider {

    /**
     * An implementation of the {@link KinesisClient} that allows control over Kinesis Service
     * responses.
     */
    public static class TestingAsyncKinesisClient implements KinesisAsyncClient {

        public static final CompletableFuture<Void> SUBSCRIBE_TO_SHARD_RESPONSE_FUTURE =
                new CompletableFuture<>();

        private boolean closed = false;
        private SubscribeToShardRequest subscribeToShardRequest;
        private SubscribeToShardResponseHandler subscribeToShardResponseHandler;

        @Override
        public String serviceName() {
            return "kinesis";
        }

        @Override
        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public CompletableFuture<Void> subscribeToShard(
                SubscribeToShardRequest subscribeToShardRequest,
                SubscribeToShardResponseHandler asyncResponseHandler) {
            this.subscribeToShardRequest = subscribeToShardRequest;
            this.subscribeToShardResponseHandler = asyncResponseHandler;
            return SUBSCRIBE_TO_SHARD_RESPONSE_FUTURE;
        }

        public SubscribeToShardRequest getSubscribeToShardRequest() {
            return subscribeToShardRequest;
        }

        public SubscribeToShardResponseHandler getSubscribeToShardResponseHandler() {
            return subscribeToShardResponseHandler;
        }
    }
}
