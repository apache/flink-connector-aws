/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.sqs.sink.testutils;

import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.utils.ImmutableMap;

/**
 * A set of static methods that can be used to call common AWS services on the Localstack container.
 */
public class SqsTestUtils {

    private static final ObjectMapper MAPPER = createObjectMapper();

    public static SqsClient createSqsClient(String endpoint, SdkHttpClient httpClient) {
        return AWSServicesTestUtils.createAwsSyncClient(endpoint, httpClient, SqsClient.builder());
    }

    public static DataStream<String> getSampleDataGenerator(
            StreamExecutionEnvironment env, int endValue) {
        return env.fromSequence(1, endValue)
                .map(Object::toString)
                .returns(String.class)
                .map(data -> MAPPER.writeValueAsString(ImmutableMap.of("data", data)));
    }

    public static void createSqs(String sqsName, SqsClient sqsClient) {
        CreateQueueRequest createQueueRequest =
                CreateQueueRequest.builder().queueName(sqsName).build();

        sqsClient.createQueue(createQueueRequest);
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper;
    }
}
