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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.sqs.sink.testutils.SqsTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.DockerImageVersions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;
import static org.apache.flink.connector.sqs.sink.testutils.SqsTestUtils.createSqsClient;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test suite for the {@code SqsSink} using a localstack container.
 */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class SqsSinkITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SqsSinkITCase.class);
    private static final int NUMBER_OF_ELEMENTS = 50;
    private StreamExecutionEnvironment env;
    private SdkHttpClient httpClient;
    private SqsClient sqsClient;

    @Container
    private static LocalstackContainer mockSqsContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK));

    @BeforeEach
    void setup() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        httpClient = AWSServicesTestUtils.createHttpClient();
        sqsClient = createSqsClient(mockSqsContainer.getEndpoint(), httpClient);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @AfterEach
    void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    void sqsSinkWritesCorrectDataToMockAWSServices() throws Exception {
        LOG.info("1 - Creating the SQS");
        AWSServicesTestUtils.createSqs("test-sqs", sqsClient);

        SqsSink<String> sqsSink =
                SqsSink.<String>builder()
                        .setSerializationSchema(new SimpleStringSchema())
                        .setSqsUrl("http://localhost:4576/queue/test-sqs")
                        .setSqsClientProperties(
                                createConfig(mockSqsContainer.getEndpoint()))
                        .build();

        SqsTestUtils.getSampleDataGenerator(env, NUMBER_OF_ELEMENTS).sinkTo(sqsSink);
        env.execute("Integration Test");
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            // Read data from SQS and validate
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl("http://localhost:4576/queue/test-sqs")
                    .maxNumberOfMessages(10) // max 10 can be read at a time
                    .build();

            messages.addAll(sqsClient.receiveMessage(receiveMessageRequest).messages());
        }

        // Add assertions here to validate the messages
        assertEquals(NUMBER_OF_ELEMENTS, messages.size(), "Number of messages received should match the number of elements sent");

        List<String> sentDataList = new ArrayList<>();
        SqsTestUtils.getSampleDataGenerator(env, NUMBER_OF_ELEMENTS).executeAndCollect().forEachRemaining(sentDataList::add);

        List<String> receivedDataList = new ArrayList<>();
        for (Message message : messages) {
            receivedDataList.add(new String(Base64.getDecoder().decode(message.body())));
        }

        Assertions.assertThat(sentDataList.containsAll(receivedDataList)).isTrue();
    }
}
