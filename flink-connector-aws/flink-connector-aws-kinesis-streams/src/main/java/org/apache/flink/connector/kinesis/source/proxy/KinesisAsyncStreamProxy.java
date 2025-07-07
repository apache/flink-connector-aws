/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.connector.kinesis.source.split.StartingPositionUtil.toSdkStartingPosition;

/** Implementation of async stream proxy for the Kinesis client. */
@Internal
public class KinesisAsyncStreamProxy implements AsyncStreamProxy {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisAsyncStreamProxy.class);
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5000;
    private final long shutdownTimeoutMillis;

    private final KinesisAsyncClient kinesisAsyncClient;
    private final SdkAsyncHttpClient asyncHttpClient;

    public KinesisAsyncStreamProxy(
            KinesisAsyncClient kinesisAsyncClient, SdkAsyncHttpClient asyncHttpClient) {
        this(kinesisAsyncClient, asyncHttpClient, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    public KinesisAsyncStreamProxy(
            KinesisAsyncClient kinesisAsyncClient, SdkAsyncHttpClient asyncHttpClient, long shutdownTimeoutMillis) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.asyncHttpClient = asyncHttpClient;
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
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
                        .startingPosition(toSdkStartingPosition(startingPosition))
                        .build();
        return kinesisAsyncClient.subscribeToShard(request, responseHandler);
    }

    /**
     * Gracefully closes the Kinesis clients with a timeout.
     *
     * @param timeoutMillis maximum time to wait for clients to close
     */
    public void gracefulClose(long timeoutMillis) {
        try {
            LOG.debug("Closing Kinesis clients with timeout of {} ms", timeoutMillis);

            // Close the Kinesis client first with half the timeout
            long kinesisClientTimeout = timeoutMillis / 2;
            LOG.debug("Closing KinesisAsyncClient with timeout of {} ms", kinesisClientTimeout);
            try {
                CompletableFuture<Void> kinesisClientFuture = CompletableFuture.runAsync(() -> kinesisAsyncClient.close());
                kinesisClientFuture.get(kinesisClientTimeout, TimeUnit.MILLISECONDS);
                LOG.debug("Successfully closed KinesisAsyncClient");
            } catch (TimeoutException e) {
                LOG.warn("Closing KinesisAsyncClient timed out after {} ms", kinesisClientTimeout);
            } catch (Exception e) {
                LOG.warn("Error while closing KinesisAsyncClient", e);
            }

            // Then close the HTTP client with the remaining timeout
            long httpClientTimeout = timeoutMillis - kinesisClientTimeout;
            LOG.debug("Closing SdkAsyncHttpClient with timeout of {} ms", httpClientTimeout);
            try {
                CompletableFuture<Void> httpClientFuture = CompletableFuture.runAsync(() -> asyncHttpClient.close());
                httpClientFuture.get(httpClientTimeout, TimeUnit.MILLISECONDS);
                LOG.debug("Successfully closed SdkAsyncHttpClient");
            } catch (TimeoutException e) {
                LOG.warn("Closing SdkAsyncHttpClient timed out after {} ms", httpClientTimeout);
            } catch (Exception e) {
                LOG.warn("Error while closing SdkAsyncHttpClient", e);
            }

            LOG.debug("Completed graceful shutdown of Kinesis clients");
        } catch (Exception e) {
            LOG.warn("Error during graceful shutdown of Kinesis clients", e);
        }
    }

    @Override
    public void close() throws IOException {
        gracefulClose(shutdownTimeoutMillis);
    }
}
