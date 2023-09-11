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

import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/** Dummy test to clean up stale resources. */
@Tag("requires-aws-credentials")
public class StaleResourceCleanupITCase {
    @Test
    void cleanUpStaleKinesisStreams() {
        try (SdkHttpClient httpClient = AWSServicesTestUtils.createHttpClient();
                KinesisClient kinesisClient =
                        AWSEndToEndTestUtils.createAwsSyncClient(
                                httpClient, KinesisClient.builder());
                AWSKinesisResourceManager kinesisResourceManager =
                        new AWSKinesisResourceManager(kinesisClient); ) {
            kinesisResourceManager.cleanUpStaleKinesisDataStreams();
        }
    }
}
