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

package org.apache.flink.connector.dynamodb.source;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the client that {@link DynamoDbStreamsSource} creates for non-blocking calls. */
class DynamoDbStreamsSourceAsyncClientTest {

    @Test
    void testCreateAsyncClientUsesRegionAndEndpointOverride() {
        Properties clientProperties = new Properties();
        clientProperties.setProperty(AWSConfigConstants.AWS_REGION, "eu-west-1");
        clientProperties.setProperty(AWSConfigConstants.AWS_ENDPOINT, "https://localhost:4566");

        try (SdkAsyncHttpClient asyncHttpClient = DynamoDbStreamsSource.createAsyncHttpClient();
                DynamoDbStreamsAsyncClient asyncClient =
                        DynamoDbStreamsSource.createAsyncClient(
                                clientProperties,
                                asyncHttpClient,
                                SdkDefaultRetryStrategy.defaultRetryStrategy())) {
            assertThat(asyncClient.serviceClientConfiguration().region())
                    .isEqualTo(Region.EU_WEST_1);
            assertThat(asyncClient.serviceClientConfiguration().endpointOverride())
                    .contains(URI.create("https://localhost:4566"));
        }
    }

    @Test
    void testCreateAsyncClientWithoutEndpointOverride() {
        Properties clientProperties = new Properties();
        clientProperties.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");

        try (SdkAsyncHttpClient asyncHttpClient = DynamoDbStreamsSource.createAsyncHttpClient();
                DynamoDbStreamsAsyncClient asyncClient =
                        DynamoDbStreamsSource.createAsyncClient(
                                clientProperties,
                                asyncHttpClient,
                                SdkDefaultRetryStrategy.defaultRetryStrategy())) {
            assertThat(asyncClient.serviceClientConfiguration().endpointOverride()).isEmpty();
        }
    }

    /**
     * Sends one GetRecords call through the real Netty client to a local HTTP/1.1 server. DynamoDB
     * Streams endpoints use only HTTP/1.1, so a client that used HTTP/2 would fail every call.
     */
    @Test
    void testAsyncClientCallsHttp11Endpoint() throws Exception {
        List<String> protocols = new CopyOnWriteArrayList<>();
        byte[] body =
                "{\"Records\":[],\"NextShardIterator\":\"next-shard-iterator\"}"
                        .getBytes(StandardCharsets.UTF_8);
        HttpServer server =
                HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    protocols.add(exchange.getProtocol());
                    exchange.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.0");
                    exchange.sendResponseHeaders(200, body.length);
                    try (OutputStream responseBody = exchange.getResponseBody()) {
                        responseBody.write(body);
                    }
                });
        server.start();

        Properties clientProperties = new Properties();
        clientProperties.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        clientProperties.setProperty(
                AWSConfigConstants.AWS_ENDPOINT,
                "http://127.0.0.1:" + server.getAddress().getPort());
        clientProperties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        clientProperties.setProperty(
                AWSConfigConstants.accessKeyId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
                "accessKeyId");
        clientProperties.setProperty(
                AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
                "secretAccessKey");

        try (SdkAsyncHttpClient asyncHttpClient = DynamoDbStreamsSource.createAsyncHttpClient();
                DynamoDbStreamsAsyncClient asyncClient =
                        DynamoDbStreamsSource.createAsyncClient(
                                clientProperties,
                                asyncHttpClient,
                                SdkDefaultRetryStrategy.defaultRetryStrategy())) {
            GetRecordsResponse response =
                    asyncClient
                            .getRecords(request -> request.shardIterator("shard-iterator"))
                            .get();

            assertThat(response.nextShardIterator()).isEqualTo("next-shard-iterator");
            assertThat(protocols).containsExactly("HTTP/1.1");
        } finally {
            server.stop(0);
        }
    }
}
