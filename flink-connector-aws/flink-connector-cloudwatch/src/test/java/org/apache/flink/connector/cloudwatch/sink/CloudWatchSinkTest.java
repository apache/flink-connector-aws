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

package org.apache.flink.connector.cloudwatch.sink;

import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.BASIC;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.ENV_VAR;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.SYS_PROP;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.WEB_IDENTITY_TOKEN;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_NAMESPACE;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class CloudWatchSinkTest {
    @Test
    public void testSuccessfullyCreateWithMinimalConfiguration() {
        CloudWatchSink.<MetricWriteRequest>builder().setNamespace(TEST_NAMESPACE).build();
    }

    @Test
    public void testElementConverterUsesDefaultConverterIfNotSet() {
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder().setNamespace(TEST_NAMESPACE).build();

        assertThat(sink)
                .extracting("elementConverter")
                .isInstanceOf(DefaultMetricWriteRequestElementConverter.class);
    }

    @Test
    public void testNamespaceRequired() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> CloudWatchSink.<MetricWriteRequest>builder().build())
                .withMessageContaining(
                        "The cloudWatch namespace must not be null when initializing the CloudWatch Sink.");
    }

    @Test
    public void testNamespaceNotBlank() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () -> CloudWatchSink.<MetricWriteRequest>builder().setNamespace("").build())
                .withMessageContaining(
                        "The cloudWatch namespace must be set when initializing the CloudWatch Sink.");
    }

    @Test
    public void testInvalidMaxBatchSize() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                CloudWatchSink.<MetricWriteRequest>builder()
                                        .setNamespace(TEST_NAMESPACE)
                                        .setMaxBatchSize(1001)
                                        .build())
                .withMessageContaining(
                        "The cloudWatch MaxBatchSize must not be greater than 1,000.");
    }

    @Test
    public void testInvalidMaxBatchSizeInBytes() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                CloudWatchSink.<MetricWriteRequest>builder()
                                        .setNamespace(TEST_NAMESPACE)
                                        .setMaxBatchSizeInBytes(1000001)
                                        .build())
                .withMessageContaining(
                        "The cloudWatch MaxBatchSizeInBytes must not be greater than 1,000,000.");
    }

    @Test
    public void testInvalidAwsRegionThrowsException() {
        Properties properties = new Properties();
        properties.setProperty(AWS_REGION, "some-invalid-region");
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setCloudWatchClientProperties(properties)
                        .setNamespace(TEST_NAMESPACE)
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Invalid AWS region set in config.");
    }

    @Test
    public void testIncompleteEnvironmentCredentialsProviderThrowsException() {
        Properties properties = new Properties();
        properties.put(AWS_CREDENTIALS_PROVIDER, ENV_VAR.toString());
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setCloudWatchClientProperties(properties)
                        .setNamespace(TEST_NAMESPACE)
                        .build();

        assertThatExceptionOfType(SdkClientException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Unable to load credentials from system settings.");
    }

    @Test
    public void testIncompleteSystemPropertyCredentialsProviderThrowsException() {
        Properties properties = new Properties();
        properties.put(AWS_CREDENTIALS_PROVIDER, SYS_PROP.toString());
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setCloudWatchClientProperties(properties)
                        .setNamespace(TEST_NAMESPACE)
                        .build();

        assertThatExceptionOfType(SdkClientException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Unable to load credentials from system settings.");
    }

    @Test
    public void testIncompleteBasicCredentialsProviderThrowsException() {
        Properties properties = new Properties();
        properties.put(AWS_CREDENTIALS_PROVIDER, BASIC.toString());
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setCloudWatchClientProperties(properties)
                        .setNamespace(TEST_NAMESPACE)
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining(
                        "Please set values for AWS Access Key ID ('aws.credentials.provider.basic.accesskeyid') and Secret Key ('aws.credentials.provider.basic.secretkey') when using the BASIC AWS credential provider type.");
    }

    @Test
    public void testIncompleteWebIdentityTokenCredentialsProviderThrowsException() {
        Properties properties = new Properties();
        properties.put(AWS_CREDENTIALS_PROVIDER, WEB_IDENTITY_TOKEN.toString());
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setCloudWatchClientProperties(properties)
                        .setNamespace(TEST_NAMESPACE)
                        .build();

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining(
                        "Either the environment variable AWS_WEB_IDENTITY_TOKEN_FILE or the javaproperty aws.webIdentityTokenFile must be set.");
    }

    @Test
    public void testInvalidCredentialsProviderThrowsException() {
        Properties properties = new Properties();
        properties.put(AWS_CREDENTIALS_PROVIDER, "INVALID_CREDENTIALS_PROVIDER");
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setCloudWatchClientProperties(properties)
                        .setNamespace(TEST_NAMESPACE)
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Invalid AWS Credential Provider Type set in config.");
    }

    @Test
    public void testGetWriterStateSerializer() {
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder().setNamespace(TEST_NAMESPACE).build();

        assertThat(sink.getWriterStateSerializer())
                .usingRecursiveComparison()
                .isEqualTo(new CloudWatchStateSerializer());
    }
}
