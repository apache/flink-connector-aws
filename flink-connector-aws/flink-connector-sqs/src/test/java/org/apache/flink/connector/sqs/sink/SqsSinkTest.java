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
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.sqs.sink.testutils.SqsTestUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;

/** Covers construction, defaults and sanity checking of {@link SqsSink}. */
class SqsSinkTest {

    private static final ElementConverter<String, SendMessageBatchRequestEntry> elementConverter =
            SqsSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @Test
    void sqsUrlMustNotBeNull() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                new SqsSink<>(
                                        elementConverter,
                                        10,
                                        50,
                                        5000,
                                        256000L,
                                        5000L,
                                        256 * 1000L,
                                        false,
                                        null,
                                        new Properties()))
                .withMessageContaining(
                        "The sqs url must not be null when initializing the SQS Sink.");
    }

    @Test
    void sqsUrlMustNotBeEmpty() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new SqsSink<>(
                                        elementConverter,
                                        500,
                                        16,
                                        10000,
                                        4 * 1024 * 1024L,
                                        5000L,
                                        1000 * 1024L,
                                        false,
                                        "",
                                        new Properties()))
                .withMessageContaining("The sqs url must be set when initializing the SQS Sink.");
    }

    @Test
    void sqsMaxBatchSizeMustNotBeGreaterThan10() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new SqsSink<>(
                                        elementConverter,
                                        100,
                                        50,
                                        5000,
                                        256000L,
                                        5000L,
                                        256 * 1000L,
                                        false,
                                        "testSqlUrl",
                                        new Properties()))
                .withMessageContaining("The sqs MaxBatchSize must not be greater than 10.");
    }

    @Test
    void sqsSinkFailsWhenAccessKeyIdIsNotProvided() {
        Properties properties = createConfig("https://non-exisitent-location");
        properties.setProperty(
                AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.BASIC.toString());
        properties.remove(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER));
        sqsSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
                properties, "Please set values for AWS Access Key ID");
    }

    @Test
    void sqsSinkFailsWhenRegionIsNotProvided() {
        Properties properties = createConfig("https://non-exisitent-location");
        properties.remove(AWS_REGION);
        sqsSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
                properties, "region must not be null.");
    }

    @Test
    void sqsSinkFailsWhenUnableToConnectToRemoteService() {
        Properties properties = createConfig("https://non-exisitent-location");
        properties.remove(TRUST_ALL_CERTIFICATES);
        sqsSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
                properties,
                "Received an UnknownHostException when attempting to interact with a service.");
    }

    private void sqsSinkFailsWithAppropriateMessageWhenInitialConditionsAreMisconfigured(
            Properties properties, String errorMessage) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SqsSink<String> sqsSink =
                SqsSink.<String>builder()
                        .setSqsSinkElementConverter(
                                SqsSinkElementConverter.<String>builder()
                                        .setSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setSqsUrl("sqs-url")
                        .setMaxBatchSize(5)
                        .setSqsClientProperties(properties)
                        .build();

        SqsTestUtils.getSampleDataGenerator(env, 10).sinkTo(sqsSink);

        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(() -> env.execute("Integration Test"))
                .havingCause()
                .havingCause()
                .withMessageContaining(errorMessage);
    }
}
