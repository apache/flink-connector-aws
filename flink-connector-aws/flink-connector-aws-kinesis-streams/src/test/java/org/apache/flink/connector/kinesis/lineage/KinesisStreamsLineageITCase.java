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

package org.apache.flink.connector.kinesis.lineage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that jobs using the Kinesis source/sink expose lineage through the
 * FLIP-314 {@link JobCreatedEvent}, for both the DataStream API and SQL.
 */
@Testcontainers
class KinesisStreamsLineageITCase {

    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";
    private static final String ACCOUNT_ID = "000000000000";
    private static final Region TEST_REGION = Region.AP_SOUTHEAST_1;
    private static final String EXPECTED_NAMESPACE =
            String.format("arn:aws:kinesis:%s:%s", TEST_REGION, ACCOUNT_ID);

    // The listener must be registered in the cluster configuration: MiniClusterExecutor creates
    // JobStatusChangedListeners from MiniCluster.getConfiguration(), not the per-job config.
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(clusterConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static Configuration clusterConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS,
                Collections.singletonList(CapturingLineageListenerFactory.class.getName()));
        return configuration;
    }

    @Container
    private static final LocalstackContainer MOCK_KINESIS_CONTAINER =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION));

    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;

    /** Captures lineage graphs from {@link JobCreatedEvent}s. */
    public static class CapturingLineageListenerFactory implements JobStatusChangedListenerFactory {

        private static final List<LineageGraph> CAPTURED_GRAPHS = new CopyOnWriteArrayList<>();

        @Override
        public JobStatusChangedListener createListener(Context context) {
            return (JobStatusChangedEvent event) -> {
                if (event instanceof JobCreatedEvent) {
                    CAPTURED_GRAPHS.add(((JobCreatedEvent) event).lineageGraph());
                }
            };
        }
    }

    @BeforeEach
    void setUp() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        CapturingLineageListenerFactory.CAPTURED_GRAPHS.clear();

        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        MOCK_KINESIS_CONTAINER.getEndpoint(), httpClient, KinesisClient.builder());
    }

    @AfterEach
    void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    @Test
    void testDataStreamJobExposesKinesisLineage() throws Exception {
        String sourceStream = "lineage-ds-source-stream";
        String sinkStream = "lineage-ds-sink-stream";
        createStream(sourceStream);
        createStream(sinkStream);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KinesisStreamsSource<String> source =
                KinesisStreamsSource.<String>builder()
                        .setStreamArn(streamArn(sourceStream))
                        .setSourceConfig(sourceConfiguration())
                        .setDeserializationSchema(new SimpleStringSchema())
                        .build();

        KinesisStreamsSink<String> sink =
                KinesisStreamsSink.<String>builder()
                        .setStreamArn(streamArn(sinkStream))
                        .setSerializationSchema(new SimpleStringSchema())
                        .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                        .setKinesisClientProperties(clientProperties())
                        .build();

        env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "kinesis-source",
                        TypeInformation.of(String.class))
                .sinkTo(sink);

        JobClient jobClient = env.executeAsync("lineage-datastream-job");
        try {
            LineageGraph graph = awaitLineageGraph();

            assertThat(graph.sources()).hasSize(1);
            LineageDataset sourceDataset = graph.sources().get(0).datasets().get(0);
            assertThat(sourceDataset.name()).isEqualTo("stream/" + sourceStream);
            assertThat(sourceDataset.namespace()).isEqualTo(EXPECTED_NAMESPACE);
            assertThat(sourceDataset.facets())
                    .containsKeys(
                            KinesisDatasetFacet.KINESIS_FACET_NAME,
                            TypeDatasetFacet.TYPE_FACET_NAME);

            assertThat(graph.sinks()).hasSize(1);
            LineageDataset sinkDataset = graph.sinks().get(0).datasets().get(0);
            assertThat(sinkDataset.name()).isEqualTo("stream/" + sinkStream);
            assertThat(sinkDataset.namespace()).isEqualTo(EXPECTED_NAMESPACE);
            assertThat(sinkDataset.facets()).containsKey(KinesisDatasetFacet.KINESIS_FACET_NAME);
        } finally {
            cancelSilently(jobClient);
        }
    }

    @Test
    void testSqlJobExposesKinesisLineage() throws Exception {
        String sourceStream = "lineage-sql-source-stream";
        String sinkStream = "lineage-sql-sink-stream";
        createStream(sourceStream);
        createStream(sinkStream);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(kinesisTableDdl("source_table", sourceStream));
        tableEnv.executeSql(kinesisTableDdl("sink_table", sinkStream));

        Optional<JobClient> jobClient =
                tableEnv.executeSql("INSERT INTO sink_table SELECT * FROM source_table")
                        .getJobClient();
        try {
            LineageGraph graph = awaitLineageGraph();

            // The SQL planner emits catalog-identity dataset names, but merges the physical
            // source's/sink's connector lineage (namespace + facets) into the datasets. The
            // source side requires the connector to expose a SourceProvider: the planner only
            // consults LineageVertexProvider in its SourceProvider branch.
            assertThat(graph.sources()).hasSize(1);
            LineageDataset sourceDataset = graph.sources().get(0).datasets().get(0);
            assertThat(sourceDataset.namespace()).isEqualTo(EXPECTED_NAMESPACE);
            assertThat(sourceDataset.facets()).containsKey(KinesisDatasetFacet.KINESIS_FACET_NAME);
            KinesisDatasetFacet sourceFacet =
                    (KinesisDatasetFacet)
                            sourceDataset.facets().get(KinesisDatasetFacet.KINESIS_FACET_NAME);
            assertThat(sourceFacet.getStreamArn()).isEqualTo(streamArn(sourceStream));
            assertThat(sourceFacet.getStreamName()).isEqualTo(sourceStream);

            assertThat(graph.sinks()).hasSize(1);
            LineageDataset sinkDataset = graph.sinks().get(0).datasets().get(0);
            assertThat(sinkDataset.namespace()).isEqualTo(EXPECTED_NAMESPACE);
            assertThat(sinkDataset.facets()).containsKey(KinesisDatasetFacet.KINESIS_FACET_NAME);
            KinesisDatasetFacet sinkFacet =
                    (KinesisDatasetFacet)
                            sinkDataset.facets().get(KinesisDatasetFacet.KINESIS_FACET_NAME);
            assertThat(sinkFacet.getStreamArn()).isEqualTo(streamArn(sinkStream));
            assertThat(sinkFacet.getStreamName()).isEqualTo(sinkStream);
        } finally {
            jobClient.ifPresent(this::cancelSilently);
        }
    }

    private void cancelSilently(JobClient client) {
        try {
            client.cancel().get();
        } catch (Exception e) {
            // The job may have already terminated; lineage is captured at submission time.
        }
    }

    private Configuration sourceConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(AWS_ENDPOINT, MOCK_KINESIS_CONTAINER.getEndpoint());
        configuration.setString(AWS_ACCESS_KEY_ID, "accessKeyId");
        configuration.setString(AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        configuration.setString(AWS_REGION, TEST_REGION.toString());
        configuration.setString(TRUST_ALL_CERTIFICATES, "true");
        configuration.setString(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        return configuration;
    }

    private Properties clientProperties() {
        Properties properties = new Properties();
        properties.setProperty(AWS_ENDPOINT, MOCK_KINESIS_CONTAINER.getEndpoint());
        properties.setProperty(AWS_ACCESS_KEY_ID, "accessKeyId");
        properties.setProperty(AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        properties.setProperty(AWS_REGION, TEST_REGION.toString());
        properties.setProperty(TRUST_ALL_CERTIFICATES, "true");
        properties.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        return properties;
    }

    private String kinesisTableDdl(String tableName, String streamName) {
        return String.format(
                "CREATE TABLE %s (payload STRING) WITH ("
                        + "'connector' = 'kinesis',"
                        + "'stream.arn' = '%s',"
                        + "'aws.region' = '%s',"
                        + "'aws.endpoint' = '%s',"
                        + "'aws.credentials.provider' = 'BASIC',"
                        + "'aws.credentials.basic.accesskeyid' = 'accessKeyId',"
                        + "'aws.credentials.basic.secretkey' = 'secretAccessKey',"
                        + "'aws.trust.all.certificates' = 'true',"
                        + "'format' = 'raw')",
                tableName, streamArn(streamName), TEST_REGION, MOCK_KINESIS_CONTAINER.getEndpoint());
    }

    private String streamArn(String streamName) {
        return String.format(
                "arn:aws:kinesis:%s:%s:stream/%s", TEST_REGION, ACCOUNT_ID, streamName);
    }

    private void createStream(String streamName) throws Exception {
        kinesisClient.createStream(
                CreateStreamRequest.builder().streamName(streamName).shardCount(1).build());

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        while (!kinesisClient
                .describeStream(builder -> builder.streamName(streamName))
                .streamDescription()
                .streamStatusAsString()
                .equals("ACTIVE")) {
            if (deadline.isOverdue()) {
                throw new AssertionError("Stream " + streamName + " did not become ACTIVE");
            }
            Thread.sleep(200);
        }
    }

    private LineageGraph awaitLineageGraph() throws InterruptedException {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        while (CapturingLineageListenerFactory.CAPTURED_GRAPHS.isEmpty()) {
            if (deadline.isOverdue()) {
                throw new AssertionError("No JobCreatedEvent lineage graph captured");
            }
            Thread.sleep(100);
        }
        return CapturingLineageListenerFactory.CAPTURED_GRAPHS.get(
                CapturingLineageListenerFactory.CAPTURED_GRAPHS.size() - 1);
    }
}
