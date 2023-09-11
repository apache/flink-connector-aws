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

package org.apache.flink.connector.kinesis.testutils;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;

import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;

/** Utility class for configuring end-to-end tests. */
public class AWSEndToEndTestUtils {
    private static final Region TEST_REGION = Region.AP_SOUTHEAST_1;

    private AWSEndToEndTestUtils() {
        // Private constructor to prevent initialization.
    }

    public static <
                    S extends SdkClient,
                    T extends
                            AwsSyncClientBuilder<? extends T, S> & AwsClientBuilder<? extends T, S>>
            S createAwsSyncClient(SdkHttpClient httpClient, T clientBuilder) {
        Properties config = createTestConfig();
        return clientBuilder
                .httpClient(httpClient)
                .credentialsProvider(AWSGeneralUtil.getCredentialsProvider(config))
                .region(AWSGeneralUtil.getRegion(config))
                .build();
    }

    public static Properties createTestConfig() {
        Properties config = new Properties();
        config.setProperty(AWS_REGION, TEST_REGION.toString());
        configureTestCredentials(config);
        return config;
    }

    private static void configureTestCredentials(Properties config) {
        Preconditions.checkNotNull(
                System.getenv("FLINK_AWS_USER"),
                "FLINK_AWS_USER not configured for end to end test.");
        Preconditions.checkNotNull(
                System.getenv("FLINK_AWS_PASSWORD"),
                "FLINK_AWS_PASSWORD not configured for end to end test.");
        config.setProperty(
                AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER),
                System.getenv("FLINK_AWS_USER"));
        config.setProperty(
                AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER),
                System.getenv("FLINK_AWS_PASSWORD"));
    }
}
