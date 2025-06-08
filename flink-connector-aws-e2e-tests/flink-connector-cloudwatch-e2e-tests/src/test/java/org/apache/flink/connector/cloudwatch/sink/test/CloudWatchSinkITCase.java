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

package org.apache.flink.connector.cloudwatch.sink.test;

import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.cloudwatch.sink.CloudWatchSink;
import org.apache.flink.connector.cloudwatch.sink.MetricWriteRequest;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;

import java.time.Instant;
import java.util.UUID;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link CloudWatchSink}. */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class CloudWatchSinkITCase {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchSinkITCase.class);

    private static String testMetricName;
    private static final int NUMBER_OF_ELEMENTS = 50;

    private static StreamExecutionEnvironment env;

    private CloudWatchClient cloudWatchClient;
    private SdkHttpClient httpClient;
    private static final Network network = Network.newNetwork();
    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";
    private static final String TEST_NAMESPACE = "test_namespace";

    @Container
    private static final LocalstackContainer MOCK_CLOUDWATCH_CONTAINER =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    @BeforeEach
    public void setup() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        testMetricName = UUID.randomUUID().toString();

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        httpClient = AWSServicesTestUtils.createHttpClient();

        cloudWatchClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        MOCK_CLOUDWATCH_CONTAINER.getEndpoint(),
                        httpClient,
                        CloudWatchClient.builder());

        LOG.info("Done setting up the localstack.");
    }

    @AfterEach
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, cloudWatchClient);
    }

    @Test
    public void testRandomDataSuccessfullyWritten() throws Exception {
        CloudWatchSink<MetricWriteRequest> cloudWatchSink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setNamespace(TEST_NAMESPACE)
                        .setCloudWatchClientProperties(
                                createConfig(MOCK_CLOUDWATCH_CONTAINER.getEndpoint()))
                        .build();

        Instant testTimestamp = Instant.now();

        env.fromSequence(1, NUMBER_OF_ELEMENTS)
                .map(
                        data ->
                                MetricWriteRequest.builder()
                                        .withMetricName(testMetricName)
                                        .addValue(1.0d)
                                        .withTimestamp(testTimestamp)
                                        .build())
                .sinkTo(cloudWatchSink);

        env.execute("Integration Test");

        GetMetricDataResponse response =
                cloudWatchClient.getMetricData(
                        GetMetricDataRequest.builder()
                                .metricDataQueries(
                                        MetricDataQuery.builder()
                                                .metricStat(getMetricStat("Sum"))
                                                .build(),
                                        MetricDataQuery.builder()
                                                .metricStat(getMetricStat("SampleCount"))
                                                .build())
                                .startTime(testTimestamp.minusSeconds(300))
                                .endTime(testTimestamp.plusSeconds(300))
                                .build());

        response.metricDataResults()
                .forEach(
                        result ->
                                assertThat(result.values())
                                        .containsExactly(Double.valueOf(NUMBER_OF_ELEMENTS)));
    }

    private static MetricStat getMetricStat(String stat) {
        return MetricStat.builder()
                .metric(
                        Metric.builder()
                                .namespace(TEST_NAMESPACE)
                                .metricName(testMetricName)
                                .build())
                .stat(stat)
                .period(300)
                .build();
    }
}
