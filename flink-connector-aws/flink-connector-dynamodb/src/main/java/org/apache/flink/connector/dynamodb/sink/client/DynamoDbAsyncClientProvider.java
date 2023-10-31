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

package org.apache.flink.connector.dynamodb.sink.client;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.dynamodb.sink.DynamoDbConfigConstants;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.Properties;

/** Provides a {@link DynamoDbAsyncClient}. */
@Internal
public class DynamoDbAsyncClientProvider implements SdkClientProvider<DynamoDbAsyncClient> {

    private final SdkAsyncHttpClient httpClient;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public DynamoDbAsyncClientProvider(Properties clientProperties) {
        this.httpClient =
                AWSGeneralUtil.createAsyncHttpClient(overrideClientProperties(clientProperties));
        this.dynamoDbAsyncClient = buildClient(clientProperties, httpClient);
    }

    @Override
    public DynamoDbAsyncClient getClient() {
        return dynamoDbAsyncClient;
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(httpClient, dynamoDbAsyncClient);
    }

    private Properties overrideClientProperties(Properties dynamoDbClientProperties) {
        Properties overridenProperties = new Properties();
        overridenProperties.putAll(dynamoDbClientProperties);

        // Specify HTTP1_1 protocol since DynamoDB endpoint doesn't support HTTP2
        overridenProperties.putIfAbsent(AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP1_1");
        return overridenProperties;
    }

    private DynamoDbAsyncClient buildClient(
            Properties dynamoDbClientProperties, SdkAsyncHttpClient httpClient) {
        AWSGeneralUtil.validateAwsCredentials(dynamoDbClientProperties);

        return AWSClientUtil.createAwsAsyncClient(
                dynamoDbClientProperties,
                httpClient,
                DynamoDbAsyncClient.builder(),
                DynamoDbConfigConstants.BASE_DYNAMODB_USER_AGENT_PREFIX_FORMAT,
                DynamoDbConfigConstants.DYNAMODB_CLIENT_USER_AGENT_PREFIX);
    }
}
