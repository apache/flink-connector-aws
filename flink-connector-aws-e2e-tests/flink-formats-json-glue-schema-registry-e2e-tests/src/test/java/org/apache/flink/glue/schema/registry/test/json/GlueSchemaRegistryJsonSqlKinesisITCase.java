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

package org.apache.flink.glue.schema.registry.test.json;

import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * End-to-end test for the {@code json-glue} Flink SQL format factory.
 *
 * <p>Exercises the SQL table path (DDL {@code 'format' = 'json-glue'}) end-to-end: rows are written
 * to a Localstack Kinesis stream through the {@code kinesis} SQL connector encoded by the JSON GSR
 * serialization schema (which registers the JSON Schema derived from the Flink {@link
 * org.apache.flink.table.types.logical.RowType} against a <b>real</b> AWS Glue Schema Registry),
 * and read back through a second SQL table whose decoder resolves and strips the GSR header — and,
 * when the writer compressed the payload, decompresses it via {@code FacadeGsrJsonReader} (review
 * finding C1).
 *
 * <p>The Kinesis data plane is emulated by Localstack; the schema registry calls hit a real GSR in
 * the configured region. The test therefore requires real credentials and is gated on the {@code
 * IT_CASE_GLUE_SCHEMA_ACCESS_KEY} / {@code IT_CASE_GLUE_SCHEMA_SECRET_KEY} environment variables.
 * It carries the {@code requires-aws-credentials} JUnit tag so it only runs under the {@code
 * run-aws-end-to-end-tests} Maven profile; when the credentials are absent it skips cleanly (via
 * {@code assumeThat}) before the Localstack container is even started.
 *
 * <p>Optional overrides: {@code IT_CASE_GLUE_SCHEMA_REGION} (default {@code ca-central-1}) and
 * {@code IT_CASE_GLUE_SCHEMA_REGISTRY_NAME} (default {@code default-registry}).
 */
@Tag("requires-aws-credentials")
class GlueSchemaRegistryJsonSqlKinesisITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(GlueSchemaRegistryJsonSqlKinesisITCase.class);

    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";

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

    /** Region the (Localstack) Kinesis data plane runs in; also the ARN region. */
    private static final String KINESIS_REGION = "ap-southeast-1";

    private static final String STREAM_ARN_PREFIX =
            "arn:aws:kinesis:" + KINESIS_REGION + ":000000000000:stream/";

    private LocalstackContainer mockKinesisContainer;
    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;

    /** GSR schema names created during a test, deleted best-effort in teardown. */
    private final List<String> createdSchemaNames = new ArrayList<>();

    @BeforeEach
    void setup() {
        // Skip cleanly (before starting Docker) when real GSR credentials are not configured.
        assumeThat(USE_DEFAULT_CREDENTIALS || (ACCESS_KEY != null && !ACCESS_KEY.isBlank()))
                .as("Credentials not configured, skipping test")
                .isTrue();
        assumeThat(USE_DEFAULT_CREDENTIALS || (SECRET_KEY != null && !SECRET_KEY.isBlank()))
                .as("Credentials not configured, skipping test")
                .isTrue();

        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        // Feed the GSR facade's DefaultCredentialsProvider chain. The json-glue format factory
        // builds its config map without credential keys, so it resolves credentials from the
        // system-property / environment chain — these properties supply them. In
        // default-credentials mode the chain resolves them itself (profile, SSO, env).
        if (!USE_DEFAULT_CREDENTIALS) {
            System.setProperty("aws.accessKeyId", ACCESS_KEY);
            System.setProperty("aws.secretAccessKey", SECRET_KEY);
        }
        System.setProperty("aws.region", GSR_REGION);

        mockKinesisContainer =
                new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                        .withNetworkAliases("localstack");
        mockKinesisContainer.start();

        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        mockKinesisContainer.getEndpoint(), httpClient, KinesisClient.builder());

        LOG.info("Localstack Kinesis ready at {}", mockKinesisContainer.getEndpoint());
    }

    @AfterEach
    void teardown() {
        deleteCreatedSchemas();
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
        if (mockKinesisContainer != null) {
            mockKinesisContainer.stop();
        }
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    /** (a) Basic multi-row round-trip write+read via {@code 'format' = 'json-glue'}. */
    @Test
    void testBasicMultiRowRoundTrip() throws Exception {
        String streamName = uniqueName("json_sql_basic");
        String streamArn = createStream(streamName);
        String schemaName = registerSchemaName("basic");

        StreamTableEnvironment tEnv = newTableEnv();

        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";
        tEnv.executeSql(sinkDdl("basic_sink", columns, streamArn, schemaName, null));
        tEnv.executeSql(
                        "INSERT INTO basic_sink VALUES "
                                + "('Alice', 42, 'blue'),"
                                + "('Bob', 7, 'green'),"
                                + "('Charlie', 99, 'red')")
                .await(2, TimeUnit.MINUTES);

        tEnv.executeSql(sourceDdl("basic_source", columns, streamArn, schemaName, null));
        List<Row> rows = collect(tEnv, "SELECT * FROM basic_source", 3, Duration.ofSeconds(90));

        assertThat(rows).hasSize(3);
        assertThat(rows)
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
    }

    /** (b) Nullable columns, including NULL values, round-trip correctly. */
    @Test
    void testNullableColumnsRoundTrip() throws Exception {
        String streamName = uniqueName("json_sql_nullable");
        String streamArn = createStream(streamName);
        String schemaName = registerSchemaName("nullable");

        StreamTableEnvironment tEnv = newTableEnv();

        // id is NOT NULL (required in the derived JSON Schema); name/note are nullable.
        String columns = "id INT NOT NULL, name STRING, note STRING";
        tEnv.executeSql(sinkDdl("nullable_sink", columns, streamArn, schemaName, null));
        tEnv.executeSql(
                        "INSERT INTO nullable_sink VALUES "
                                + "(1, 'Alice', 'note-a'),"
                                + "(2, 'Bob', CAST(NULL AS STRING)),"
                                + "(3, CAST(NULL AS STRING), 'note-c')")
                .await(2, TimeUnit.MINUTES);

        tEnv.executeSql(sourceDdl("nullable_source", columns, streamArn, schemaName, null));
        List<Row> rows = collect(tEnv, "SELECT * FROM nullable_source", 3, Duration.ofSeconds(90));

        assertThat(rows).hasSize(3);
        Row rowWithNullNote = findById(rows, 2);
        Row rowWithNullName = findById(rows, 3);
        assertThat(rowWithNullNote.getField(2)).as("note of id=2 should be NULL").isNull();
        assertThat(rowWithNullName.getField(1)).as("name of id=3 should be NULL").isNull();
    }

    /**
     * (c) Compression round-trip. Uses {@code json-glue.schema.compression = ZLIB} so the writer
     * compresses the payload and the reader must decompress it — exercising the C1 fix in {@code
     * FacadeGsrJsonReader}.
     */
    @Test
    void testCompressionRoundTrip() throws Exception {
        String streamName = uniqueName("json_sql_zlib");
        String streamArn = createStream(streamName);
        String schemaName = registerSchemaName("zlib");

        StreamTableEnvironment tEnv = newTableEnv();

        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";
        tEnv.executeSql(sinkDdl("zlib_sink", columns, streamArn, schemaName, "ZLIB"));
        tEnv.executeSql(
                        "INSERT INTO zlib_sink VALUES "
                                + "('Dave', 13, 'yellow'),"
                                + "('Eve', 55, 'purple')")
                .await(2, TimeUnit.MINUTES);

        tEnv.executeSql(sourceDdl("zlib_source", columns, streamArn, schemaName, "ZLIB"));
        List<Row> rows = collect(tEnv, "SELECT * FROM zlib_source", 2, Duration.ofSeconds(90));

        assertThat(rows).hasSize(2);
        assertThat(rows)
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder("Dave", "Eve");
    }

    /**
     * (d) Complex types round-trip: nested ROW, ARRAY&lt;STRING&gt; and MAP&lt;STRING,STRING&gt;
     * are all supported by {@code JsonSchemaConverter} (MAP keys must be string-typed).
     */
    @Test
    void testComplexTypesRoundTrip() throws Exception {
        String streamName = uniqueName("json_sql_complex");
        String streamArn = createStream(streamName);
        String schemaName = registerSchemaName("complex");

        StreamTableEnvironment tEnv = newTableEnv();

        String columns =
                "order_id STRING,"
                        + " customer ROW<name STRING, age INT>,"
                        + " tags ARRAY<STRING>,"
                        + " attrs MAP<STRING, STRING>";
        tEnv.executeSql(sinkDdl("complex_sink", columns, streamArn, schemaName, null));
        tEnv.executeSql(
                        "INSERT INTO complex_sink VALUES ("
                                + "  'ORD-1',"
                                + "  ROW('Jane', 30),"
                                + "  ARRAY['priority', 'express'],"
                                + "  MAP['source', 'web', 'campaign', 'summer']"
                                + ")")
                .await(2, TimeUnit.MINUTES);

        tEnv.executeSql(sourceDdl("complex_source", columns, streamArn, schemaName, null));
        List<Row> rows = collect(tEnv, "SELECT * FROM complex_source", 1, Duration.ofSeconds(90));

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField(0)).isEqualTo("ORD-1");
        Row customer = (Row) row.getField(1);
        assertThat(customer).isNotNull();
        assertThat(customer.getField(0)).isEqualTo("Jane");
        assertThat(row.getField(2)).isNotNull();
        assertThat(row.getField(3)).isNotNull();
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    /**
     * (e) A schema registered by a prior writer can be reused by a second writer that does
     * <b>not</b> auto-register. Phase A writes two rows through a table with {@code
     * 'json-glue.schema.autoRegistration' = 'true'}, registering the derived JSON Schema in real
     * GSR. Phase B writes two more rows through a second table that targets the <b>same</b> schema
     * name with {@code 'json-glue.schema.autoRegistration' = 'false'} — this must succeed against
     * the now-existing schema. All four rows are then read back and asserted.
     */
    @Test
    void existingSchemaWithoutAutoRegistration() throws Exception {
        String streamName = uniqueName("json_sql_existing");
        String streamArn = createStream(streamName);
        String schemaName = registerSchemaName("existing");

        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";

        // Phase A: register the JSON Schema in real GSR via autoRegistration=true.
        StreamTableEnvironment tEnvA = newTableEnv();
        tEnvA.executeSql(sinkDdl("existing_sink_auto", columns, streamArn, schemaName, null));
        tEnvA.executeSql(
                        "INSERT INTO existing_sink_auto VALUES "
                                + "('Alice', 1, 'blue'),"
                                + "('Bob', 2, 'green')")
                .await(2, TimeUnit.MINUTES);

        // Phase B: reuse the now-existing schema with autoRegistration=false — must succeed.
        StreamTableEnvironment tEnvB = newTableEnv();
        tEnvB.executeSql(
                sinkDdlNoAutoRegister("existing_sink_noauto", columns, streamArn, schemaName));
        tEnvB.executeSql(
                        "INSERT INTO existing_sink_noauto VALUES "
                                + "('Charlie', 3, 'red'),"
                                + "('Dave', 4, 'yellow')")
                .await(2, TimeUnit.MINUTES);

        // All four rows written under the same schema must round-trip.
        StreamTableEnvironment tEnvR = newTableEnv();
        tEnvR.executeSql(sourceDdl("existing_source", columns, streamArn, schemaName, null));
        List<Row> rows =
                collect(tEnvR, "SELECT * FROM existing_source", 4, Duration.ofSeconds(120));

        assertThat(rows).hasSize(4);
        assertThat(rows)
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder("Alice", "Bob", "Charlie", "Dave");
    }

    /**
     * (f) Writing with {@code 'json-glue.schema.autoRegistration' = 'false'} against a schema name
     * that was never registered in GSR must fail — the writer cannot resolve a schema and is not
     * permitted to create one.
     */
    @Test
    void missingSchemaWithoutAutoRegistrationFails() throws Exception {
        String streamName = uniqueName("json_sql_missing");
        String streamArn = createStream(streamName);
        // Unique name that is never registered in GSR (still tracked for best-effort cleanup).
        String schemaName = registerSchemaName("missing");

        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";

        StreamTableEnvironment tEnv = newTableEnv();
        tEnv.executeSql(sinkDdlNoAutoRegister("missing_sink", columns, streamArn, schemaName));

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO missing_sink VALUES "
                                                        + "('Alice', 1, 'blue')")
                                        .await(2, TimeUnit.MINUTES))
                .as(
                        "writing with autoRegistration=false against a schema that was never "
                                + "registered in GSR must fail")
                .isInstanceOf(Exception.class);
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        return StreamTableEnvironment.create(execEnv);
    }

    private String sinkDdl(
            String table, String columns, String streamArn, String schemaName, String compression) {
        return "CREATE TABLE "
                + table
                + " ("
                + columns
                + ") WITH ("
                + kinesisConnectorOptions(streamArn, false)
                + gsrFormatOptions(schemaName, true, compression)
                + ")";
    }

    private String sinkDdlNoAutoRegister(
            String table, String columns, String streamArn, String schemaName) {
        return "CREATE TABLE "
                + table
                + " ("
                + columns
                + ") WITH ("
                + kinesisConnectorOptions(streamArn, false)
                + "  'format' = 'json-glue',"
                + "  'json-glue.aws.region' = '"
                + GSR_REGION
                + "',"
                + "  'json-glue.registry.name' = '"
                + REGISTRY_NAME
                + "',"
                + "  'json-glue.schema.name' = '"
                + schemaName
                + "',"
                + "  'json-glue.schema.autoRegistration' = 'false'"
                + ")";
    }

    private String sourceDdl(
            String table, String columns, String streamArn, String schemaName, String compression) {
        return "CREATE TABLE "
                + table
                + " ("
                + columns
                + ") WITH ("
                + kinesisConnectorOptions(streamArn, true)
                + gsrFormatOptions(schemaName, false, compression)
                + ")";
    }

    private String kinesisConnectorOptions(String streamArn, boolean source) {
        StringBuilder sb = new StringBuilder();
        sb.append("  'connector' = 'kinesis',");
        sb.append("  'stream.arn' = '").append(streamArn).append("',");
        sb.append("  'aws.region' = '").append(KINESIS_REGION).append("',");
        sb.append("  'aws.endpoint' = '").append(mockKinesisContainer.getEndpoint()).append("',");
        sb.append("  'aws.credentials.provider' = 'BASIC',");
        sb.append("  'aws.credentials.basic.accesskeyid' = 'accessKeyId',");
        sb.append("  'aws.credentials.basic.secretkey' = 'secretAccessKey',");
        sb.append("  'aws.trust.all.certificates' = 'true',");
        sb.append("  'aws.http.protocol.version' = 'HTTP1_1',");
        if (source) {
            sb.append("  'source.init.position' = 'TRIM_HORIZON',");
        }
        return sb.toString();
    }

    private String gsrFormatOptions(String schemaName, boolean autoRegister, String compression) {
        StringBuilder sb = new StringBuilder();
        sb.append("  'format' = 'json-glue',");
        sb.append("  'json-glue.aws.region' = '").append(GSR_REGION).append("',");
        sb.append("  'json-glue.registry.name' = '").append(REGISTRY_NAME).append("',");
        sb.append("  'json-glue.schema.name' = '").append(schemaName).append("'");
        if (autoRegister) {
            sb.append(",  'json-glue.schema.autoRegistration' = 'true'");
        }
        if (compression != null) {
            sb.append(",  'json-glue.schema.compression' = '").append(compression).append("'");
        }
        return sb.toString();
    }

    private List<Row> collect(
            StreamTableEnvironment tEnv, String query, int expected, Duration timeout)
            throws Exception {
        List<Row> collected = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tEnv.executeSql(query).collect()) {
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                if (iterator.hasNext()) {
                    Row row = iterator.next();
                    LOG.info("collected row: {}", row);
                    collected.add(row);
                } else {
                    Thread.sleep(500);
                }
            }
        }
        return collected;
    }

    private String createStream(String streamName) throws Exception {
        kinesisClient.createStream(
                CreateStreamRequest.builder().streamName(streamName).shardCount(1).build());
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (streamActive(streamName)) {
                return STREAM_ARN_PREFIX + streamName;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Kinesis stream did not become ACTIVE: " + streamName);
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

    private String registerSchemaName(String scenario) {
        String name = "flink-json-glue-sql-e2e-" + scenario + "-" + UUID.randomUUID();
        createdSchemaNames.add(name);
        return name;
    }

    private void deleteCreatedSchemas() {
        if (createdSchemaNames.isEmpty()) {
            return;
        }
        try (GlueClient glueClient =
                GlueClient.builder()
                        .region(Region.of(GSR_REGION))
                        .credentialsProvider(itCredentialsProvider())
                        .build()) {
            for (String schemaName : createdSchemaNames) {
                try {
                    glueClient.deleteSchema(
                            DeleteSchemaRequest.builder()
                                    .schemaId(
                                            SchemaId.builder()
                                                    .registryName(REGISTRY_NAME)
                                                    .schemaName(schemaName)
                                                    .build())
                                    .build());
                } catch (Exception e) {
                    LOG.warn(
                            "Best-effort GSR schema cleanup failed for {}: {}",
                            schemaName,
                            e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not create GlueClient for schema cleanup: {}", e.getMessage());
        } finally {
            createdSchemaNames.clear();
        }
    }

    private static Row findById(List<Row> rows, int id) {
        return rows.stream()
                .filter(row -> Integer.valueOf(id).equals(row.getField(0)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no row with id=" + id));
    }

    private static String uniqueName(String prefix) {
        return prefix + "_" + Long.toUnsignedString(UUID.randomUUID().getMostSignificantBits(), 36);
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }

    /** Credentials for the out-of-band verification/cleanup clients. */
    private static AwsCredentialsProvider itCredentialsProvider() {
        return USE_DEFAULT_CREDENTIALS
                ? DefaultCredentialsProvider.create()
                : StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));
    }
}
