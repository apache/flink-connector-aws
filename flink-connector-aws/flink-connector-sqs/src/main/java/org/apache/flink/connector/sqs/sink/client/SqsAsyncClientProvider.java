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

package org.apache.flink.connector.sqs.sink.client;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.sqs.sink.SqsConfigConstants;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Properties;

/** Provides a {@link SqsAsyncClient}. */
@Internal
public class SqsAsyncClientProvider implements SdkClientProvider<SqsAsyncClient> {

    private final SdkAsyncHttpClient httpClient;
    private final SqsAsyncClient sqsAsyncClient;

    public SqsAsyncClientProvider(Properties clientProperties) {
        this.httpClient = AWSGeneralUtil.createAsyncHttpClient(clientProperties);
        this.sqsAsyncClient = buildClient(clientProperties, httpClient);
    }

    @Override
    public SqsAsyncClient getClient() {
        return sqsAsyncClient;
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(httpClient, sqsAsyncClient);
    }

    private SqsAsyncClient buildClient(
            Properties sqsClientProperties, SdkAsyncHttpClient httpClient) {
        AWSGeneralUtil.validateAwsCredentials(sqsClientProperties);

        return AWSClientUtil.createAwsAsyncClient(
                sqsClientProperties,
                httpClient,
                SqsAsyncClient.builder(),
                SqsConfigConstants.BASE_SQS_USER_AGENT_PREFIX_FORMAT.key(),
                SqsConfigConstants.SQS_CLIENT_USER_AGENT_PREFIX.key());
    }
}
