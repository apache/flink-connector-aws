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

package org.apache.flink.connector.kinesis.source.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Implementation of async stream proxy for the Kinesis client. */
@Internal
public class KinesisAsyncStreamProxy implements AsyncStreamProxy {
    private final KinesisAsyncClient kinesisAsyncClient;
    private final SdkAsyncHttpClient asyncHttpClient;

    public KinesisAsyncStreamProxy(
            KinesisAsyncClient kinesisAsyncClient, SdkAsyncHttpClient asyncHttpClient) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(
            String consumerArn,
            String shardId,
            StartingPosition startingPosition,
            SubscribeToShardResponseHandler responseHandler) {
        SubscribeToShardRequest request =
                SubscribeToShardRequest.builder()
                        .consumerARN(consumerArn)
                        .shardId(shardId)
                        .startingPosition(startingPosition.getSdkStartingPosition())
                        .build();
        return kinesisAsyncClient.subscribeToShard(request, responseHandler);
    }

    @Override
    public void close() throws IOException {
        kinesisAsyncClient.close();
        asyncHttpClient.close();
    }
}
