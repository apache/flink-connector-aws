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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.util.Properties;

import static java.util.Collections.emptyList;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TCP_KEEPALIVE;

/** Creates instances of {@link KinesisProxySyncV2} or {@link KinesisProxyAsyncV2}. */
@Internal
public class KinesisProxyV2Factory {

    private static final FullJitterBackoff BACKOFF = new FullJitterBackoff();

    /**
     * Uses the given properties to instantiate a new instance of {@link KinesisProxyAsyncV2}.
     *
     * @param configProps the properties used to parse configuration
     * @return the Kinesis proxy
     */
    public static KinesisProxyAsyncV2Interface createKinesisProxyAsyncV2(
            final Properties configProps) {
        Preconditions.checkNotNull(configProps);

        final AttributeMap convertedProperties = AwsV2Util.convertProperties(configProps);
        final AttributeMap.Builder clientConfiguration = AttributeMap.builder();
        populateDefaultValues(clientConfiguration);

        final SdkAsyncHttpClient asyncHttpClient =
                AWSGeneralUtil.createAsyncHttpClient(
                        convertedProperties.merge(clientConfiguration.build()),
                        NettyNioAsyncHttpClient.builder());
        final FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(configProps, emptyList());

        Properties clientProperties = KinesisConfigUtil.getV2ConsumerClientProperties(configProps);

        final KinesisAsyncClient asyncClient =
                AWSClientUtil.createAwsAsyncClient(
                        clientProperties,
                        asyncHttpClient,
                        KinesisAsyncClient.builder(),
                        KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                        KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX,
                        KinesisStreamsConfigConstants.KINESIS_CLIENT_RETRY_STRATEGY_MAX_ATTEMPTS);

        return new KinesisProxyAsyncV2(asyncClient, asyncHttpClient, configuration);
    }

    /**
     * Uses the given properties to instantiate a new instance of {@link KinesisProxySyncV2}.
     *
     * @param configProps the properties used to parse configuration
     * @return the Kinesis proxy
     */
    public static KinesisProxySyncV2Interface createKinesisProxySyncV2(
            final Properties configProps) {
        Preconditions.checkNotNull(configProps);

        final AttributeMap convertedProperties = AwsV2Util.convertProperties(configProps);
        final AttributeMap.Builder clientConfiguration = AttributeMap.builder();
        populateDefaultValues(clientConfiguration);

        final SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        convertedProperties.merge(clientConfiguration.build()),
                        ApacheHttpClient.builder());

        final FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(configProps, emptyList());
        Properties clientProperties = KinesisConfigUtil.getV2ConsumerClientProperties(configProps);

        final KinesisClient client =
                AWSClientUtil.createAwsSyncClient(
                        clientProperties,
                        httpClient,
                        KinesisClient.builder(),
                        KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                        KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX,
                        KinesisStreamsConfigConstants.KINESIS_CLIENT_RETRY_STRATEGY_MAX_ATTEMPTS);

        return new KinesisProxySyncV2(client, httpClient, configuration, BACKOFF);
    }

    private static void populateDefaultValues(final AttributeMap.Builder clientConfiguration) {
        clientConfiguration.put(TCP_KEEPALIVE, true);
    }
}
