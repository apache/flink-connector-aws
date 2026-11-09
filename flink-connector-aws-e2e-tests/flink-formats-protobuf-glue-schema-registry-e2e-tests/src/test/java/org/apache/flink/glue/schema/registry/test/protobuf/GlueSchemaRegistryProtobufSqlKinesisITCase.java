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

package org.apache.flink.glue.schema.registry.test.protobuf;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * SQL-path end-to-end test for the AWS Glue Schema Registry Protobuf format ({@code protobuf-glue})
 * exercising the Flink Table API through a Localstack Kinesis data plane against a <b>real</b> Glue
 * Schema Registry.
 *
 * <p>The Kinesis connector talks to Localstack via an explicit {@code aws.endpoint} + dummy {@code
 * BASIC} credentials, while the {@code protobuf-glue} format resolves the real GSR through the
 * default AWS credential chain, which we seed with the {@code IT_CASE_GLUE_SCHEMA_*} credentials
 * via JVM system properties in {@link #setup()} (the MiniCluster runs in this same JVM).
 *
 * <p>The test is tagged {@code requires-aws-credentials} so it only runs under the {@code
 * run-aws-end-to-end-tests} Maven profile, and is additionally {@code assumeThat}-gated to skip
 * when credentials are absent.
 *
 * <p>Scenarios:
 *
 * <ul>
 *   <li>{@link #testBasicMultiRowRoundTrip()} — multi-row round-trip of STRING/INT/BOOLEAN columns.
 *   <li>{@link #testNullableColumnsRoundTrip()} — nullable columns carrying explicit {@code NULL}
 *       values, exercising the proto3 explicit-presence fix (C2).
 *   <li>{@link #testCompressionRoundTrip()} — same round-trip with {@code
 *       protobuf-glue.schema.compression = ZLIB}, exercising the compression round-trip fix (C1).
 *   <li>{@link #existingSchemaWithoutAutoRegistration()} — a pre-existing schema is written to by a
 *       second table with {@code autoRegistration = false}, which must succeed because no
 *       registration is required.
 *   <li>{@link #missingSchemaWithoutAutoRegistrationFails()} — writing against a never-registered
 *       schema with {@code autoRegistration = false} must fail.
 * </ul>
 */
@Tag("requires-aws-credentials")
class GlueSchemaRegistryProtobufSqlKinesisITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(GlueSchemaRegistryProtobufSqlKinesisITCase.class);

    private static final String ACCESS_KEY = System.getenv("IT_CASE_GLUE_SCHEMA_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("IT_CASE_GLUE_SCHEMA_SECRET_KEY");

    /**
     * Alternative gate for environments where extracting key material is undesirable (SSO, instance
     * profiles, credential_process): when {@code true}, the test runs with the AWS default
     * credential provider chain instead of explicit keys.
     */
    private static final boolean USE_DEFAULT_CREDENTIALS =
            Boolean.parseBoolean(
                    System.getenv()
                            .getOrDefault("IT_CASE_GLUE_SCHEMA_USE_DEFAULT_CREDENTIALS", "false"));

    private static final String GSR_REGION =
            envOrDefault("IT_CASE_GLUE_SCHEMA_REGION", "ca-central-1");
    private static final String REGISTRY_NAME =
            envOrDefault("IT_CASE_GLUE_SCHEMA_REGISTRY_NAME", "default-registry");

    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";
    private static final String KINESIS_REGION = "ap-southeast-1";
    private static final String KINESIS_ACCOUNT = "000000000000";

    private static final LocalstackContainer MOCK_KINESIS_CONTAINER =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                    .withNetworkAliases("localstack");

    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;
    private StreamTableEnvironment tEnv;
    private final List<String> createdSchemas = new ArrayList<>();

    @BeforeAll
    static void beforeAll() {
        assumeThat(USE_DEFAULT_CREDENTIALS || (ACCESS_KEY != null && !ACCESS_KEY.isBlank()))
                .as("Credentials not configured, skipping test")
                .isTrue();
        assumeThat(USE_DEFAULT_CREDENTIALS || (SECRET_KEY != null && !SECRET_KEY.isBlank()))
                .as("Credentials not configured, skipping test")
                .isTrue();

        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        MOCK_KINESIS_CONTAINER.start();
    }

    @AfterAll
    static void afterAll() {
        if (MOCK_KINESIS_CONTAINER.isRunning()) {
            MOCK_KINESIS_CONTAINER.stop();
        }
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @BeforeEach
    void setup() {
        // Seed the default AWS credential chain so the protobuf-glue format authenticates against
        // the real Glue Schema Registry from inside the MiniCluster JVM. In default-credentials
        // mode the chain resolves them itself (profile, SSO, env).
        if (!USE_DEFAULT_CREDENTIALS) {
            System.setProperty("aws.accessKeyId", ACCESS_KEY);
            System.setProperty("aws.secretAccessKey", SECRET_KEY);
        }
        System.setProperty("aws.region", GSR_REGION);

        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        MOCK_KINESIS_CONTAINER.getEndpoint(), httpClient, KinesisClient.builder());

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        tEnv = StreamTableEnvironment.create(execEnv);

        LOG.info("Done setting up Localstack Kinesis + real GSR credential chain.");
    }

    @AfterEach
    void teardown() {
        cleanupSchemas();
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    @Test
    void testBasicMultiRowRoundTrip() throws Exception {
        String id = uniqueId("basic");
        String schemaName = schemaName(id);
        prepareStream(id);

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_basic ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, false)
                        + ","
                        + protobufGlueOptions(schemaName, true, null)
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_basic VALUES "
                                + "('Alice', 30, true),"
                                + "('Bob', 25, false),"
                                + "('Charlie', 35, true)")
                .await(120, TimeUnit.SECONDS);

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_basic ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, true)
                        + ","
                        + protobufGlueOptions(schemaName, false, null)
                        + ")");

        List<Row> rows = collect("SELECT * FROM kinesis_source_basic", 3, Duration.ofSeconds(90));

        assertThat(rows).hasSize(3);
        assertThat(rows)
                .extracting(row -> String.valueOf(row.getField(0)))
                .containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
    }

    @Test
    void testNullableColumnsRoundTrip() throws Exception {
        String id = uniqueId("nullable");
        String schemaName = schemaName(id);
        prepareStream(id);

        // All value columns are nullable (default in Flink SQL) so the proto3 explicit-presence
        // fix (C2) must round-trip explicit NULLs as NULL rather than proto3 defaults.
        String columns =
                "  id STRING," + "  opt_str STRING," + "  opt_int INT," + "  opt_bool BOOLEAN";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_nullable ("
                        + columns
                        + ") WITH ("
                        + kinesisOptions(id, false)
                        + ","
                        + protobufGlueOptions(schemaName, true, null)
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_nullable VALUES "
                                + "('R1', 'hello', 42, true),"
                                + "('R2', CAST(NULL AS STRING), CAST(NULL AS INT), CAST(NULL AS BOOLEAN))")
                .await(120, TimeUnit.SECONDS);

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_nullable ("
                        + columns
                        + ") WITH ("
                        + kinesisOptions(id, true)
                        + ","
                        + protobufGlueOptions(schemaName, false, null)
                        + ")");

        List<Row> rows =
                collect("SELECT * FROM kinesis_source_nullable", 2, Duration.ofSeconds(90));

        assertThat(rows).hasSize(2);

        Row r1 = findById(rows, "R1");
        assertThat(r1.getField(1)).isEqualTo("hello");
        assertThat(r1.getField(2)).isEqualTo(42);
        assertThat(r1.getField(3)).isEqualTo(true);

        Row r2 = findById(rows, "R2");
        assertThat(r2.getField(1)).as("nullable STRING should round-trip as NULL").isNull();
        assertThat(r2.getField(2)).as("nullable INT should round-trip as NULL").isNull();
        assertThat(r2.getField(3)).as("nullable BOOLEAN should round-trip as NULL").isNull();
    }

    @Test
    void testCompressionRoundTrip() throws Exception {
        String id = uniqueId("compression");
        String schemaName = schemaName(id);
        prepareStream(id);

        // 'schema.compression' = 'ZLIB' exercises the compression round-trip fix (C1): the reader
        // must transparently decompress GSR-compressed payloads.
        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_zlib ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, false)
                        + ","
                        + protobufGlueOptions(schemaName, true, "ZLIB")
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_zlib VALUES "
                                + "('Dave', 40, true),"
                                + "('Eve', 28, false),"
                                + "('Frank', 33, true)")
                .await(120, TimeUnit.SECONDS);

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_zlib ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, true)
                        + ","
                        + protobufGlueOptions(schemaName, false, "ZLIB")
                        + ")");

        List<Row> rows = collect("SELECT * FROM kinesis_source_zlib", 3, Duration.ofSeconds(90));

        assertThat(rows).hasSize(3);
        assertThat(rows)
                .extracting(row -> String.valueOf(row.getField(0)))
                .containsExactlyInAnyOrder("Dave", "Eve", "Frank");
    }

    @Test
    void existingSchemaWithoutAutoRegistration() throws Exception {
        String id = uniqueId("existing");
        String schemaName = schemaName(id);
        prepareStream(id);

        // Phase A: register the schema (and its first version) in real GSR through a sink with
        // autoRegistration = true, landing two rows on the stream.
        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_existing_a ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, false)
                        + ","
                        + protobufGlueOptionsExplicitAutoReg(schemaName, true)
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_existing_a VALUES "
                                + "('Alice', 30, true),"
                                + "('Bob', 25, false)")
                .await(120, TimeUnit.SECONDS);

        // Phase B: a second sink at the SAME schema name with autoRegistration = false must succeed
        // because the schema already exists in GSR — no registration is attempted.
        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_existing_b ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, false)
                        + ","
                        + protobufGlueOptionsExplicitAutoReg(schemaName, false)
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_existing_b VALUES "
                                + "('Charlie', 35, true),"
                                + "('Dave', 40, false)")
                .await(120, TimeUnit.SECONDS);

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_existing ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, true)
                        + ","
                        + protobufGlueOptionsExplicitAutoReg(schemaName, false)
                        + ")");

        List<Row> rows =
                collect("SELECT * FROM kinesis_source_existing", 4, Duration.ofSeconds(90));

        assertThat(rows)
                .as(
                        "all four rows (two written under autoRegistration=true, two under "
                                + "autoRegistration=false against the pre-existing schema) must arrive")
                .hasSize(4);
        assertThat(rows)
                .extracting(row -> String.valueOf(row.getField(0)))
                .containsExactlyInAnyOrder("Alice", "Bob", "Charlie", "Dave");
    }

    @Test
    void missingSchemaWithoutAutoRegistrationFails() throws Exception {
        String id = uniqueId("missing");
        String schemaName = schemaName(id);
        prepareStream(id);

        // The schema name is never registered in GSR and autoRegistration is disabled, so the sink
        // has no schema to serialize against and the INSERT job must fail.
        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_missing ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + kinesisOptions(id, false)
                        + ","
                        + protobufGlueOptionsExplicitAutoReg(schemaName, false)
                        + ")");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO kinesis_sink_missing VALUES "
                                                        + "('Alice', 30, true)")
                                        .await(120, TimeUnit.SECONDS))
                .as(
                        "writing against a schema that does not exist in GSR with "
                                + "autoRegistration=false must fail rather than silently registering it")
                .isInstanceOf(Exception.class);
    }

    private List<Row> collect(String selectSql, int expected, Duration timeout) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tEnv.executeSql(selectSql).collect()) {
            Deadline deadline = Deadline.fromNow(timeout);
            while (rows.size() < expected && deadline.hasTimeLeft()) {
                if (iterator.hasNext()) {
                    Row row = iterator.next();
                    LOG.info("collected row: {}", row);
                    rows.add(row);
                } else {
                    Thread.sleep(500);
                }
            }
        }
        return rows;
    }

    private Row findById(List<Row> rows, String id) {
        for (Row row : rows) {
            if (id.equals(String.valueOf(row.getField(0)))) {
                return row;
            }
        }
        throw new AssertionError("Row with id '" + id + "' not found in " + rows);
    }

    private String kinesisOptions(String streamName, boolean source) {
        StringBuilder sb = new StringBuilder();
        sb.append("  'connector' = 'kinesis',");
        sb.append("  'stream.arn' = '").append(streamArn(streamName)).append("',");
        sb.append("  'aws.region' = '").append(KINESIS_REGION).append("',");
        sb.append("  'aws.endpoint' = '").append(MOCK_KINESIS_CONTAINER.getEndpoint()).append("',");
        sb.append("  'aws.credentials.provider' = 'BASIC',");
        sb.append("  'aws.credentials.basic.accesskeyid' = 'accessKeyId',");
        sb.append("  'aws.credentials.basic.secretkey' = 'secretAccessKey',");
        sb.append("  'aws.trust.all.certificates' = 'true',");
        sb.append("  'aws.http.protocol.version' = 'HTTP1_1'");
        if (source) {
            sb.append(",  'source.init.position' = 'TRIM_HORIZON'");
        }
        return sb.toString();
    }

    private String protobufGlueOptions(String schemaName, boolean forSink, String compression) {
        StringBuilder sb = new StringBuilder();
        sb.append("  'format' = 'protobuf-glue',");
        sb.append("  'protobuf-glue.aws.region' = '").append(GSR_REGION).append("',");
        sb.append("  'protobuf-glue.registry.name' = '").append(REGISTRY_NAME).append("',");
        sb.append("  'protobuf-glue.schema.name' = '").append(schemaName).append("'");
        if (forSink) {
            sb.append(",  'protobuf-glue.schema.autoRegistration' = 'true'");
        }
        if (compression != null) {
            sb.append(",  'protobuf-glue.schema.compression' = '").append(compression).append("'");
        }
        return sb.toString();
    }

    private String protobufGlueOptionsExplicitAutoReg(String schemaName, boolean autoRegistration) {
        StringBuilder sb = new StringBuilder();
        sb.append("  'format' = 'protobuf-glue',");
        sb.append("  'protobuf-glue.aws.region' = '").append(GSR_REGION).append("',");
        sb.append("  'protobuf-glue.registry.name' = '").append(REGISTRY_NAME).append("',");
        sb.append("  'protobuf-glue.schema.name' = '").append(schemaName).append("',");
        sb.append("  'protobuf-glue.schema.autoRegistration' = '")
                .append(autoRegistration)
                .append("'");
        return sb.toString();
    }

    private void prepareStream(String streamName) throws Exception {
        kinesisClient.createStream(
                CreateStreamRequest.builder().streamName(streamName).shardCount(1).build());

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        while (deadline.hasTimeLeft()) {
            if (streamActive(streamName)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Stream " + streamName + " did not become ACTIVE in time");
    }

    private boolean streamActive(String streamName) {
        try {
            return kinesisClient
                            .describeStream(
                                    DescribeStreamRequest.builder().streamName(streamName).build())
                            .streamDescription()
                            .streamStatus()
                    == StreamStatus.ACTIVE;
        } catch (Exception e) {
            return false;
        }
    }

    private void cleanupSchemas() {
        if (createdSchemas.isEmpty()) {
            return;
        }
        try (SdkHttpClient glueHttpClient = AWSServicesTestUtils.createHttpClient();
                GlueClient glueClient =
                        GlueClient.builder()
                                .region(Region.of(GSR_REGION))
                                .httpClient(glueHttpClient)
                                .build()) {
            for (String schemaName : createdSchemas) {
                try {
                    glueClient.deleteSchema(
                            DeleteSchemaRequest.builder()
                                    .schemaId(
                                            SchemaId.builder()
                                                    .registryName(REGISTRY_NAME)
                                                    .schemaName(schemaName)
                                                    .build())
                                    .build());
                    LOG.info("Deleted GSR schema {}", schemaName);
                } catch (Exception e) {
                    LOG.warn(
                            "Best-effort cleanup failed for schema {}: {}",
                            schemaName,
                            e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("Best-effort GSR schema cleanup skipped: {}", e.getMessage());
        }
        createdSchemas.clear();
    }

    private String uniqueId(String scenario) {
        return "gsr_pb_sql_" + scenario + "_" + Long.toHexString(System.nanoTime());
    }

    private String schemaName(String id) {
        String schemaName = "flink-protobuf-glue-e2e-" + id;
        createdSchemas.add(schemaName);
        return schemaName;
    }

    private String streamArn(String streamName) {
        return "arn:aws:kinesis:"
                + KINESIS_REGION
                + ":"
                + KINESIS_ACCOUNT
                + ":stream/"
                + streamName;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.trim().isEmpty()) ? defaultValue : value;
    }
}
