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

package org.apache.flink.connector.cloudwatch.table.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.cloudwatch.sink.CloudWatchSink;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEYS;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_METRIC_NAME;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_NAMESPACE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_COUNT;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MAX_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_MIN_VALUE;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_STATS_SUM_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link CloudWatchSink} TableAPI. */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class CloudWatchTableAPIITCase extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchTableAPIITCase.class);

    private static String testDimensionValue1;
    private static String testDimensionValue2;

    private static StreamExecutionEnvironment env;

    private final Path sqlConnectorCloudWatchJar =
            ResourceTestUtils.getResource(".*cloudwatch.jar");

    private CloudWatchClient cloudWatchClient;
    private SdkHttpClient httpClient;
    private static final Network network = Network.newNetwork();
    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @Container
    private static final LocalstackContainer MOCK_CLOUDWATCH_CONTAINER =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                    .withEnv("AWS_CBOR_DISABLE", "1")
                    .withEnv(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.cloudwatch.shaded.com.amazonaws.sdk.disableCertChecking -Daws.cborEnabled=false")
                    .withLogConsumer((log) -> LOG.info(log.getUtf8String()))
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .environmentVariable("AWS_CBOR_DISABLE", "1")
                    .environmentVariable(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.cloudwatch.shaded.com.amazonaws.sdk.disableCertChecking -Daws.cborEnabled=false")
                    .network(network)
                    .logger(LOG)
                    .dependsOn(MOCK_CLOUDWATCH_CONTAINER)
                    .build();

    public static final FlinkContainers FLINK =
            FlinkContainers.builder().withTestcontainersSettings(TESTCONTAINERS_SETTINGS).build();

    @BeforeClass
    public static void setupFlink() throws Exception {
        FLINK.start();
    }

    @AfterClass
    public static void stopFlink() {
        FLINK.stop();
    }

    @Before
    public void setup() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        testDimensionValue1 = UUID.randomUUID().toString();
        testDimensionValue2 = UUID.randomUUID().toString();

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

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, cloudWatchClient);
    }

    @Test
    public void testTableApiAndSQL() throws Exception {
        executeSqlStatements(Collections.singletonList(getSqlStmt()));

        Instant testTimestamp = Instant.now();

        GetMetricDataResponse response;

        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));
        do {
            Thread.sleep(1000);
            response =
                    cloudWatchClient.getMetricData(
                            GetMetricDataRequest.builder()
                                    .metricDataQueries(
                                            MetricDataQuery.builder()
                                                    .metricStat(getMetricStat("Sum"))
                                                    .build(),
                                            MetricDataQuery.builder()
                                                    .metricStat(getMetricStat("SampleCount"))
                                                    .build(),
                                            MetricDataQuery.builder()
                                                    .metricStat(getMetricStat("Maximum"))
                                                    .build(),
                                            MetricDataQuery.builder()
                                                    .metricStat(getMetricStat("Minimum"))
                                                    .build())
                                    .startTime(testTimestamp.minusSeconds(300))
                                    .endTime(testTimestamp.plusSeconds(300))
                                    .build());
        } while (deadline.hasTimeLeft() && response.metricDataResults().get(0).values().isEmpty());

        assertThat(response.metricDataResults().get(0).values())
                .containsExactly(TEST_SAMPLE_VALUE + TEST_STATS_SUM_VALUE);
        assertThat(response.metricDataResults().get(1).values()).containsExactly(TEST_SAMPLE_COUNT);
        assertThat(response.metricDataResults().get(2).values())
                .containsExactly(TEST_STATS_MAX_VALUE);
        assertThat(response.metricDataResults().get(3).values())
                .containsExactly(TEST_STATS_MIN_VALUE);
    }

    private void executeSqlStatements(final List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorCloudWatchJar)
                        .build());
    }

    private static MetricStat getMetricStat(String stat) {
        return MetricStat.builder()
                .metric(
                        Metric.builder()
                                .namespace(TEST_NAMESPACE)
                                .metricName(TEST_METRIC_NAME)
                                .dimensions(
                                        Dimension.builder()
                                                .name(TEST_DIMENSION_KEY_1)
                                                .value(testDimensionValue1)
                                                .build(),
                                        Dimension.builder()
                                                .name(TEST_DIMENSION_KEY_2)
                                                .value(testDimensionValue2)
                                                .build())
                                .build())
                .stat(stat)
                .period(300)
                .build();
    }

    private static String getSqlStmt() throws IOException {
        String sqlStmt =
                IOUtils.toString(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("test-sink-table.sql")),
                        StandardCharsets.UTF_8);

        Map<String, String> valuesMap = new HashMap<>();

        valuesMap.put("namespace", TEST_NAMESPACE);
        valuesMap.put("dimension_keys", TEST_DIMENSION_KEYS);
        valuesMap.put("dimension_key_1", TEST_DIMENSION_KEY_1);
        valuesMap.put("dimension_key_2", TEST_DIMENSION_KEY_2);
        valuesMap.put("dimension_val_1", testDimensionValue1);
        valuesMap.put("dimension_val_2", testDimensionValue2);
        valuesMap.put("metric_name", TEST_METRIC_NAME);

        return new StrSubstitutor(valuesMap).replace(sqlStmt);
    }
}
