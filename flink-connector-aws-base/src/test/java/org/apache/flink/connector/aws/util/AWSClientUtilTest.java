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

package org.apache.flink.connector.aws.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.internal.DefaultAdaptiveRetryStrategy;
import software.amazon.awssdk.retries.internal.DefaultLegacyRetryStrategy;
import software.amazon.awssdk.retries.internal.circuitbreaker.TokenBucketStore;
import software.amazon.awssdk.retries.internal.ratelimiter.RateLimiterTokenBucketStore;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.util.AWSClientUtil.formatFlinkUserAgentPrefix;
import static org.apache.flink.connector.aws.util.AWSGeneralUtil.getSdkHttpConfigurationOptions;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link AWSClientUtil}. */
class AWSClientUtilTest {

    private static final String DEFAULT_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) *Destination* Connector";
    private static final String DEFAULT_USER_AGENT_PREFIX_FORMAT_V2 =
            "Apache Flink %s (%s) *Destination* Connector V2";

    @Test
    void testCreateKinesisAsyncClient() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        MockAsyncClientBuilder builder = mockKinesisAsyncClientBuilder();
        ClientOverrideConfiguration clientOverrideConfiguration =
                ClientOverrideConfiguration.builder().build();
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();

        AWSClientUtil.createAwsAsyncClient(
                properties, builder, httpClient, clientOverrideConfiguration);

        verify(builder).overrideConfiguration(clientOverrideConfiguration);
        verify(builder).httpClient(httpClient);
        verify(builder).region(Region.of("eu-west-2"));
        verify(builder)
                .credentialsProvider(argThat(cp -> cp instanceof DefaultCredentialsProvider));
        verify(builder, never()).endpointOverride(any());
    }

    @Test
    void testCreateAwsSyncClientWithoutOverrideConfiguration() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        AttributeMap.builder().build(), ApacheHttpClient.builder());

        S3Client s3Client =
                AWSClientUtil.createAwsSyncClient(
                        properties,
                        httpClient,
                        S3Client.builder(),
                        DEFAULT_USER_AGENT_PREFIX_FORMAT,
                        formatFlinkUserAgentPrefix(
                                DEFAULT_USER_AGENT_PREFIX_FORMAT
                                        + AWSClientUtil.V2_USER_AGENT_SUFFIX));

        ClientOverrideConfiguration resultOverrideConfiguration =
                s3Client.serviceClientConfiguration().overrideConfiguration();
        assertThat(resultOverrideConfiguration.retryStrategy().get())
                .isInstanceOf(DefaultLegacyRetryStrategy.class);
        assertThat(resultOverrideConfiguration.retryPolicy()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.retryMode()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.retryStrategyConfigurator())
                .isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.apiCallAttemptTimeout()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.apiCallTimeout()).isEqualTo(Optional.empty());
        assertThat(
                        resultOverrideConfiguration
                                .advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX)
                                .isPresent())
                .isTrue();
    }

    @Test
    void testCreateAwsSyncClientWithEmptyOverrideConfiguration() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        AttributeMap.builder().build(), ApacheHttpClient.builder());
        ClientOverrideConfiguration.Builder clientOverrideConfigurationBuilder =
                ClientOverrideConfiguration.builder();

        S3Client s3Client =
                AWSClientUtil.createAwsSyncClient(
                        properties,
                        httpClient,
                        S3Client.builder(),
                        clientOverrideConfigurationBuilder,
                        DEFAULT_USER_AGENT_PREFIX_FORMAT,
                        formatFlinkUserAgentPrefix(
                                DEFAULT_USER_AGENT_PREFIX_FORMAT
                                        + AWSClientUtil.V2_USER_AGENT_SUFFIX));

        ClientOverrideConfiguration resultOverrideConfiguration =
                s3Client.serviceClientConfiguration().overrideConfiguration();
        assertThat(resultOverrideConfiguration.retryStrategy().get())
                .isInstanceOf(DefaultLegacyRetryStrategy.class);
        assertThat(resultOverrideConfiguration.retryPolicy()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.retryMode()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.retryStrategyConfigurator())
                .isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.apiCallAttemptTimeout()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.apiCallTimeout()).isEqualTo(Optional.empty());
        assertThat(
                        resultOverrideConfiguration
                                .advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX)
                                .isPresent())
                .isTrue();
    }

    @Test
    void testCreateAwsSyncClientWithOverrideConfiguration() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        AttributeMap.builder().build(), ApacheHttpClient.builder());
        ClientOverrideConfiguration.Builder clientOverrideConfigurationBuilder =
                ClientOverrideConfiguration.builder();
        RetryStrategy overrideRetryStrategy =
                DefaultAdaptiveRetryStrategy.builder()
                        .maxAttempts(10)
                        .backoffStrategy(BackoffStrategy.fixedDelay(Duration.ofMillis(500)))
                        .throttlingBackoffStrategy(
                                BackoffStrategy.fixedDelay(Duration.ofMillis(500)))
                        .tokenBucketStore(TokenBucketStore.builder().build())
                        .rateLimiterTokenBucketStore(RateLimiterTokenBucketStore.builder().build())
                        .tokenBucketExceptionCost(5)
                        .build();
        clientOverrideConfigurationBuilder.retryStrategy(overrideRetryStrategy);

        S3Client s3Client =
                AWSClientUtil.createAwsSyncClient(
                        properties,
                        httpClient,
                        S3Client.builder(),
                        clientOverrideConfigurationBuilder,
                        DEFAULT_USER_AGENT_PREFIX_FORMAT,
                        formatFlinkUserAgentPrefix(
                                DEFAULT_USER_AGENT_PREFIX_FORMAT
                                        + AWSClientUtil.V2_USER_AGENT_SUFFIX));

        ClientOverrideConfiguration resultOverrideConfiguration =
                s3Client.serviceClientConfiguration().overrideConfiguration();
        assertThat(resultOverrideConfiguration.retryStrategy()).isPresent();
        RetryStrategy resultStrategy = resultOverrideConfiguration.retryStrategy().get();
        assertThat(resultStrategy.maxAttempts()).isEqualTo(10);

        assertThat(resultOverrideConfiguration.retryPolicy()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.retryMode()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.retryStrategyConfigurator())
                .isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.apiCallAttemptTimeout()).isEqualTo(Optional.empty());
        assertThat(resultOverrideConfiguration.apiCallTimeout()).isEqualTo(Optional.empty());
        assertThat(
                        resultOverrideConfiguration
                                .advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX)
                                .isPresent())
                .isTrue();
    }

    @Test
    void testGetSdkHttpConfigurationOptions() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        properties.setProperty(AWSConfigConstants.TRUST_ALL_CERTIFICATES, "true");
        properties.setProperty(AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP1_1");
        AttributeMap options = getSdkHttpConfigurationOptions(properties);

        assertThat(options.get(SdkHttpConfigurationOption.TCP_KEEPALIVE).booleanValue()).isTrue();
        assertThat(options.containsKey(SdkHttpConfigurationOption.MAX_CONNECTIONS)).isFalse();
        assertThat(options.containsKey(SdkHttpConfigurationOption.READ_TIMEOUT)).isFalse();
        assertThat(options.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES).booleanValue())
                .isTrue();
        assertThat(options.get(SdkHttpConfigurationOption.PROTOCOL)).isEqualTo(Protocol.HTTP1_1);
    }

    @Test
    void testCreateKinesisAsyncClientWithEndpointOverride() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        properties.setProperty(AWS_ENDPOINT, "https://localhost");

        MockAsyncClientBuilder builder = mockKinesisAsyncClientBuilder();
        ClientOverrideConfiguration clientOverrideConfiguration =
                ClientOverrideConfiguration.builder().build();
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();

        AWSClientUtil.createAwsAsyncClient(
                properties, builder, httpClient, clientOverrideConfiguration);

        verify(builder).endpointOverride(URI.create("https://localhost"));
    }

    @Test
    void testClientOverrideConfigurationWithDefaults() {
        SdkClientConfiguration clientConfiguration = SdkClientConfiguration.builder().build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSClientUtil.createClientOverrideConfiguration(
                clientConfiguration,
                builder,
                formatFlinkUserAgentPrefix(
                        DEFAULT_USER_AGENT_PREFIX_FORMAT + AWSClientUtil.V2_USER_AGENT_SUFFIX));

        verify(builder).build();
        verify(builder)
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_PREFIX,
                        formatFlinkUserAgentPrefix(DEFAULT_USER_AGENT_PREFIX_FORMAT_V2));
        verify(builder).putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, null);
        verify(builder, never()).apiCallAttemptTimeout(any());
        verify(builder, never()).apiCallTimeout(any());
    }

    @Test
    void testClientOverrideConfigurationUserAgentSuffix() {
        SdkClientConfiguration clientConfiguration =
                SdkClientConfiguration.builder()
                        .option(SdkAdvancedClientOption.USER_AGENT_SUFFIX, "suffix")
                        .build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSClientUtil.createClientOverrideConfiguration(
                clientConfiguration,
                builder,
                formatFlinkUserAgentPrefix(
                        DEFAULT_USER_AGENT_PREFIX_FORMAT + AWSClientUtil.V2_USER_AGENT_SUFFIX));

        verify(builder).putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, "suffix");
    }

    @Test
    void testClientOverrideConfigurationApiCallAttemptTimeout() {
        SdkClientConfiguration clientConfiguration =
                SdkClientConfiguration.builder()
                        .option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT, Duration.ofMillis(500))
                        .build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSClientUtil.createClientOverrideConfiguration(
                clientConfiguration,
                builder,
                formatFlinkUserAgentPrefix(
                        DEFAULT_USER_AGENT_PREFIX_FORMAT_V2 + AWSClientUtil.V2_USER_AGENT_SUFFIX));

        verify(builder).apiCallAttemptTimeout(Duration.ofMillis(500));
    }

    @Test
    void testClientOverrideConfigurationApiCallTimeout() {
        SdkClientConfiguration clientConfiguration =
                SdkClientConfiguration.builder()
                        .option(SdkClientOption.API_CALL_TIMEOUT, Duration.ofMillis(600))
                        .build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSClientUtil.createClientOverrideConfiguration(
                clientConfiguration,
                builder,
                formatFlinkUserAgentPrefix(
                        DEFAULT_USER_AGENT_PREFIX_FORMAT_V2 + AWSClientUtil.V2_USER_AGENT_SUFFIX));

        verify(builder).apiCallTimeout(Duration.ofMillis(600));
    }

    private MockAsyncClientBuilder mockKinesisAsyncClientBuilder() {
        MockAsyncClientBuilder builder = mock(MockAsyncClientBuilder.class);
        when(builder.overrideConfiguration(any(ClientOverrideConfiguration.class)))
                .thenReturn(builder);
        when(builder.httpClient(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.region(any())).thenReturn(builder);

        return builder;
    }

    private ClientOverrideConfiguration.Builder mockClientOverrideConfigurationBuilder() {
        ClientOverrideConfiguration.Builder builder =
                mock(ClientOverrideConfiguration.Builder.class);
        when(builder.putAdvancedOption(any(), any())).thenReturn(builder);
        when(builder.apiCallAttemptTimeout(any())).thenReturn(builder);
        when(builder.apiCallTimeout(any())).thenReturn(builder);

        return builder;
    }

    private static class MockAsyncClientBuilder
            implements AwsAsyncClientBuilder<MockAsyncClientBuilder, SdkClient>,
                    AwsClientBuilder<MockAsyncClientBuilder, SdkClient> {

        @Override
        public MockAsyncClientBuilder asyncConfiguration(
                ClientAsyncConfiguration clientAsyncConfiguration) {
            return null;
        }

        @Override
        public MockAsyncClientBuilder httpClient(SdkAsyncHttpClient sdkAsyncHttpClient) {
            return null;
        }

        @Override
        public MockAsyncClientBuilder httpClientBuilder(SdkAsyncHttpClient.Builder builder) {
            return null;
        }

        @Override
        public MockAsyncClientBuilder credentialsProvider(
                AwsCredentialsProvider awsCredentialsProvider) {
            return null;
        }

        @Override
        public MockAsyncClientBuilder region(Region region) {
            return null;
        }

        @Override
        public MockAsyncClientBuilder dualstackEnabled(Boolean aBoolean) {
            return null;
        }

        @Override
        public MockAsyncClientBuilder fipsEnabled(Boolean aBoolean) {
            return null;
        }

        @Override
        public MockAsyncClientBuilder overrideConfiguration(
                ClientOverrideConfiguration clientOverrideConfiguration) {
            return null;
        }

        @Override
        public ClientOverrideConfiguration overrideConfiguration() {
            return null;
        }

        @Override
        public MockAsyncClientBuilder endpointOverride(URI uri) {
            return null;
        }

        @Override
        public SdkClient build() {
            return null;
        }
    }
}
