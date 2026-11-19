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

package org.apache.flink.glue.schema.registry.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * End-to-end test for the {@code avro-glue} Flink SQL format factory.
 *
 * <p>This is the SQL-path counterpart of {@link GlueSchemaRegistryAvroKinesisITCase} (which
 * exercises the DataStream API). Kinesis I/O runs against a Localstack container; the Glue Schema
 * Registry calls go to <b>real AWS</b>. The class is gated on the {@code
 * IT_CASE_GLUE_SCHEMA_ACCESS_KEY} / {@code IT_CASE_GLUE_SCHEMA_SECRET_KEY} environment variables
 * and tagged {@code requires-aws-credentials}, so it is excluded from the credential-free {@code
 * run-end-to-end-tests} profile and only runs under {@code run-aws-end-to-end-tests}. Without
 * credentials it skips cleanly (the container is never started).
 *
 * <p>Each test uses its own Localstack Kinesis stream and a run-unique GSR schema name (timestamp
 * suffix) so re-runs do not collide. Created GSR schemas are best-effort deleted in {@link
 * #afterAll()}.
 */
@ExtendWith(MiniClusterExtension.class)
@Tag("requires-aws-credentials")
class GlueSchemaRegistryAvroSqlKinesisITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(GlueSchemaRegistryAvroSqlKinesisITCase.class);

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

    /** Region for the real Glue Schema Registry calls. Overridable for the test account. */
    private static final String GSR_REGION =
            envOrDefault("IT_CASE_GLUE_SCHEMA_REGION", "ca-central-1");

    /** Registry that backs the schemas; GSR's implicit default registry when unset. */
    private static final String REGISTRY_NAME =
            envOrDefault("IT_CASE_GLUE_SCHEMA_REGISTRY", "default-registry");

    /** Region used for the Localstack Kinesis endpoint (mirrors the DataStream ITCase). */
    private static final String KINESIS_REGION = "ap-southeast-1";

    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";

    /** Unique per JVM run so parallel/repeat runs never reuse a GSR schema name. */
    private static final String RUN_ID = String.valueOf(System.currentTimeMillis());

    /** GSR schema names created during the run, for best-effort teardown. */
    private static final Set<String> CREATED_SCHEMAS = new LinkedHashSet<>();

    private static final LocalstackContainer LOCALSTACK =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                    .withNetworkAliases("localstack");

    private static SdkHttpClient httpClient;
    private static KinesisClient kinesisClient;

    private StreamTableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() {
        assumeThat(USE_DEFAULT_CREDENTIALS || (ACCESS_KEY != null && !ACCESS_KEY.isBlank()))
                .as("Credentials not configured, skipping test")
                .isTrue();
        assumeThat(USE_DEFAULT_CREDENTIALS || (SECRET_KEY != null && !SECRET_KEY.isBlank()))
                .as("Credentials not configured, skipping test")
                .isTrue();

        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        LOCALSTACK.start();
        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        LOCALSTACK.getEndpoint(), httpClient, KinesisClient.builder());
        LOG.info("Localstack Kinesis endpoint ready at {}", LOCALSTACK.getEndpoint());
    }

    @AfterAll
    static void afterAll() {
        deleteCreatedSchemas();
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
        if (LOCALSTACK.isRunning()) {
            LOCALSTACK.stop();
        }
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @BeforeEach
    void setUp() {
        // The SQL format's GSR client resolves credentials from the default chain inside the
        // MiniCluster JVM; expose the real IT credentials via system properties. In
        // default-credentials mode the chain resolves them itself (profile, SSO, env).
        if (!USE_DEFAULT_CREDENTIALS) {
            System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), ACCESS_KEY);
            System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), SECRET_KEY);
        }
        System.setProperty(SdkSystemSetting.AWS_REGION.property(), GSR_REGION);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    void tearDown() {
        System.clearProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property());
        System.clearProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property());
        System.clearProperty(SdkSystemSetting.AWS_REGION.property());
    }

    // ---------------------------------------------------------------------------------------------
    // Scenarios (ported one-to-one from the AvroGlueSqlE2E manual driver)
    // ---------------------------------------------------------------------------------------------

    @Test
    void basicRoundTrip() throws Exception {
        String streamArn = createStream("gsr_avro_sql_basic");
        String schemaName = schemaName("basic");
        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";
        List<String> formatOpts =
                autoRegOpts(schemaName, "avro-glue.schema.autoRegistration", "true");

        createKinesisTable("basic_sink", columns, streamArn, false, formatOpts);
        tEnv.executeSql(
                        "INSERT INTO basic_sink VALUES "
                                + "('Alice', 42, 'blue'),"
                                + "('Bob', 7, 'green'),"
                                + "('Charlie', 99, 'red')")
                .await(120, TimeUnit.SECONDS);

        createKinesisTable("basic_source", columns, streamArn, true, sourceOpts(schemaName));
        List<Row> rows = collect("SELECT * FROM basic_source", 3, Duration.ofSeconds(90));

        List<String> names = firstFieldStrings(rows);
        assertThat(names).contains("Alice", "Bob", "Charlie");
    }

    @Test
    void customNamespaceAndRecordName() throws Exception {
        String streamArn = createStream("gsr_avro_sql_custom_ns");
        String schemaName = schemaName("custom-ns");
        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";
        List<String> formatOpts =
                autoRegOpts(
                        schemaName,
                        "avro-glue.schema.autoRegistration",
                        "true",
                        "avro-glue.avro.namespace",
                        "com.example.events",
                        "avro-glue.avro.record-name",
                        "UserEvent");

        createKinesisTable("custom_ns_sink", columns, streamArn, false, formatOpts);
        tEnv.executeSql(
                        "INSERT INTO custom_ns_sink VALUES "
                                + "('Dave', 13, 'yellow'),"
                                + "('Eve', 55, 'purple')")
                .await(120, TimeUnit.SECONDS);

        createKinesisTable("custom_ns_source", columns, streamArn, true, sourceOpts(schemaName));
        List<Row> rows = collect("SELECT * FROM custom_ns_source", 2, Duration.ofSeconds(90));

        assertThat(firstFieldStrings(rows)).contains("Dave", "Eve");
    }

    @Test
    void fetchFromRegistry() throws Exception {
        String streamArn = createStream("gsr_avro_sql_fetch");
        String schemaName = schemaName("fetch");
        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";

        // Seed the schema in GSR (explicit namespace/record-name) via auto-registration.
        createKinesisTable(
                "fetch_seed_sink",
                columns,
                streamArn,
                false,
                autoRegOpts(
                        schemaName,
                        "avro-glue.schema.autoRegistration",
                        "true",
                        "avro-glue.avro.namespace",
                        "com.example.events",
                        "avro-glue.avro.record-name",
                        "UserEvent"));
        tEnv.executeSql("INSERT INTO fetch_seed_sink VALUES ('Seed', 0, 'none')")
                .await(120, TimeUnit.SECONDS);

        // Write with fetchFromRegistry=true (no explicit namespace) — schema fetched from GSR.
        createKinesisTable(
                "fetch_sink",
                columns,
                streamArn,
                false,
                autoRegOpts(schemaName, "avro-glue.schema.fetchFromRegistry", "true"));
        tEnv.executeSql(
                        "INSERT INTO fetch_sink VALUES "
                                + "('Frank', 21, 'orange'),"
                                + "('Grace', 33, 'pink')")
                .await(120, TimeUnit.SECONDS);

        createKinesisTable("fetch_source", columns, streamArn, true, sourceOpts(schemaName));
        List<Row> rows = collect("SELECT * FROM fetch_source", 3, Duration.ofSeconds(90));

        assertThat(firstFieldStrings(rows)).contains("Frank", "Grace");
    }

    @Test
    void complexTypes() throws Exception {
        String streamArn = createStream("gsr_avro_sql_complex");
        String schemaName = schemaName("complex");
        String columns =
                "order_id STRING,"
                        + "order_time TIMESTAMP(3),"
                        + "total_amount DECIMAL(10, 2),"
                        + "customer ROW<name STRING, email STRING, age INT>,"
                        + "items ARRAY<ROW<product_name STRING, quantity INT, price DECIMAL(8, 2)>>,"
                        + "tags ARRAY<STRING>,"
                        + "metadata MAP<STRING, STRING>,"
                        + "notes STRING";

        createKinesisTable(
                "complex_sink",
                columns,
                streamArn,
                false,
                autoRegOpts(
                        schemaName,
                        "avro-glue.schema.autoRegistration",
                        "true",
                        "avro-glue.avro.namespace",
                        "com.example.orders",
                        "avro-glue.avro.record-name",
                        "OrderEvent"));

        tEnv.executeSql(
                        "INSERT INTO complex_sink VALUES ("
                                + "  'ORD-001',"
                                + "  TIMESTAMP '2024-01-15 10:30:00',"
                                + "  CAST(199.99 AS DECIMAL(10, 2)),"
                                + "  ROW('John Doe', 'john@example.com', 35),"
                                + "  ARRAY[ROW('Widget', 2, CAST(49.99 AS DECIMAL(8, 2))), "
                                + "        ROW('Gadget', 1, CAST(99.99 AS DECIMAL(8, 2)))],"
                                + "  ARRAY['priority', 'express'],"
                                + "  MAP['source', 'web', 'campaign', 'summer-sale'],"
                                + "  'Handle with care'"
                                + ")")
                .await(120, TimeUnit.SECONDS);
        tEnv.executeSql(
                        "INSERT INTO complex_sink VALUES ("
                                + "  'ORD-002',"
                                + "  TIMESTAMP '2024-01-15 11:45:00',"
                                + "  CAST(75.50 AS DECIMAL(10, 2)),"
                                + "  ROW('Jane Smith', 'jane@example.com', 28),"
                                + "  ARRAY[ROW('Gizmo', 3, CAST(25.00 AS DECIMAL(8, 2)))],"
                                + "  ARRAY['standard'],"
                                + "  MAP['source', 'mobile'],"
                                + "  CAST(NULL AS STRING)"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        createKinesisTable("complex_source", columns, streamArn, true, sourceOpts(schemaName));
        List<Row> rows = collect("SELECT * FROM complex_source", 2, Duration.ofSeconds(90));

        Row ord001 = findByOrderId(rows, "ORD-001");
        Row ord002 = findByOrderId(rows, "ORD-002");
        assertThat(ord001).as("ORD-001 present").isNotNull();
        assertThat(ord002).as("ORD-002 present").isNotNull();

        Row customer = (Row) ord001.getField(3);
        assertThat(customer).isNotNull();
        assertThat(customer.getField(0)).isEqualTo("John Doe");
        assertThat(ord001.getField(7)).as("ORD-001 notes not null").isNotNull();
        assertThat(ord002.getField(7)).as("ORD-002 notes null (nullable)").isNull();
    }

    @Test
    void compatibilityBackwardRejectsNewRequiredField() throws Exception {
        String streamArn = createStream("gsr_avro_sql_compat_bw");
        String schemaName = schemaName("compat-backward");

        // v1: all fields NOT NULL -> required Avro fields.
        createKinesisTable(
                "compat_bw_v1",
                "user_name STRING NOT NULL, age INT NOT NULL, city STRING NOT NULL",
                streamArn,
                false,
                compatOpts(schemaName, "BACKWARD"));
        tEnv.executeSql("INSERT INTO compat_bw_v1 VALUES ('Alice', 30, 'Seattle')")
                .await(120, TimeUnit.SECONDS);

        // v2: add a nullable field -> backward compatible.
        createKinesisTable(
                "compat_bw_v2",
                "user_name STRING NOT NULL, age INT NOT NULL, city STRING NOT NULL, email STRING",
                streamArn,
                false,
                compatOpts(schemaName, "BACKWARD"));
        tEnv.executeSql(
                        "INSERT INTO compat_bw_v2 VALUES ('Bob', 25, 'Portland', 'bob@example.com')")
                .await(120, TimeUnit.SECONDS);

        // v3: ADD a required (NOT NULL, no default) field -> BACKWARD violation,
        // must be rejected. (Removing a field is backward-compatible in Avro:
        // a new reader simply ignores the extra field in old data. The true
        // violation is a new required reader field that old data cannot supply.)
        createKinesisTable(
                "compat_bw_v3",
                "user_name STRING NOT NULL, age INT NOT NULL, city STRING NOT NULL,"
                        + " country STRING NOT NULL",
                streamArn,
                false,
                compatOpts(schemaName, "BACKWARD"));
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO compat_bw_v3 VALUES"
                                                        + " ('Charlie', 35, 'Boston', 'USA')")
                                        .await(120, TimeUnit.SECONDS))
                .as("adding a required field without default must violate BACKWARD compatibility")
                .isInstanceOf(Exception.class);
    }

    @Test
    void compatibilityNoneAllowsAnyEvolution() throws Exception {
        String streamArn = createStream("gsr_avro_sql_compat_none");
        String schemaName = schemaName("compat-none");

        createKinesisTable(
                "compat_none_v1",
                "user_name STRING, age INT, city STRING",
                streamArn,
                false,
                compatOpts(schemaName, "NONE"));
        tEnv.executeSql("INSERT INTO compat_none_v1 VALUES ('Dave', 40, 'Denver')")
                .await(120, TimeUnit.SECONDS);

        createKinesisTable(
                "compat_none_v2",
                "user_name STRING, age INT",
                streamArn,
                false,
                compatOpts(schemaName, "NONE"));
        // Removing a field is normally incompatible; NONE must allow it without throwing.
        tEnv.executeSql("INSERT INTO compat_none_v2 VALUES ('Eve', 28)")
                .await(120, TimeUnit.SECONDS);
    }

    @Test
    void compatibilityFullAllowsAddingOptional() throws Exception {
        String streamArn = createStream("gsr_avro_sql_compat_full");
        String schemaName = schemaName("compat-full");

        createKinesisTable(
                "compat_full_v1",
                "user_name STRING, age INT, city STRING",
                streamArn,
                false,
                compatOpts(schemaName, "FULL"));
        tEnv.executeSql("INSERT INTO compat_full_v1 VALUES ('Frank', 45, 'Chicago')")
                .await(120, TimeUnit.SECONDS);

        createKinesisTable(
                "compat_full_v2",
                "user_name STRING, age INT, city STRING, email STRING",
                streamArn,
                false,
                compatOpts(schemaName, "FULL"));
        // Adding an optional field is safe in both directions -> allowed under FULL.
        tEnv.executeSql(
                        "INSERT INTO compat_full_v2 VALUES ('Grace', 32, 'Boston', 'grace@example.com')")
                .await(120, TimeUnit.SECONDS);
    }

    @Test
    void existingSchemaWithoutAutoRegistration() throws Exception {
        String schemaName = schemaName("existing-no-autoreg");
        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";

        // Phase A: register the schema (and a first version) by writing through a table that
        // auto-registers it. This mirrors a one-time governed schema-onboarding step.
        String seedStreamArn = createStream("gsr_avro_sql_existing_seed");
        createKinesisTable(
                "existing_seed_sink",
                columns,
                seedStreamArn,
                false,
                autoRegOpts(schemaName, "avro-glue.schema.autoRegistration", "true"));
        tEnv.executeSql(
                        "INSERT INTO existing_seed_sink VALUES "
                                + "('Heidi', 11, 'teal'),"
                                + "('Ivan', 22, 'olive')")
                .await(120, TimeUnit.SECONDS);

        // Phase B: a NEW stream for isolation, SAME schema name, but autoRegistration=false. The
        // write must succeed because the schema and a compatible version already exist in GSR —
        // this is the production governance pattern where apps are forbidden from registering.
        String prodStreamArn = createStream("gsr_avro_sql_existing_prod");
        createKinesisTable(
                "existing_prod_sink",
                columns,
                prodStreamArn,
                false,
                autoRegOpts(schemaName, "avro-glue.schema.autoRegistration", "false"));
        tEnv.executeSql(
                        "INSERT INTO existing_prod_sink VALUES "
                                + "('Judy', 33, 'maroon'),"
                                + "('Mallory', 44, 'navy')")
                .await(120, TimeUnit.SECONDS);

        createKinesisTable(
                "existing_prod_source", columns, prodStreamArn, true, sourceOpts(schemaName));
        List<Row> rows = collect("SELECT * FROM existing_prod_source", 2, Duration.ofSeconds(90));

        assertThat(firstFieldStrings(rows))
                .as(
                        "rows written against a pre-existing schema without auto-registration must"
                                + " round-trip")
                .contains("Judy", "Mallory");
    }

    @Test
    void missingSchemaWithoutAutoRegistrationFails() throws Exception {
        String streamArn = createStream("gsr_avro_sql_missing");
        String schemaName = schemaName("missing-no-autoreg");
        String columns = "user_name STRING, favorite_number INT, favorite_color STRING";

        // The schema name has never been registered and autoRegistration is off, so the write must
        // fail with a clear error rather than silently creating the schema.
        createKinesisTable(
                "missing_sink",
                columns,
                streamArn,
                false,
                autoRegOpts(schemaName, "avro-glue.schema.autoRegistration", "false"));
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO missing_sink VALUES ('Oscar', 1,"
                                                        + " 'gray')")
                                        .await(120, TimeUnit.SECONDS))
                .as(
                        "writing against a missing schema without auto-registration must fail with"
                                + " a clear error rather than silently creating the schema")
                .isInstanceOf(Exception.class);
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private void createKinesisTable(
            String tableName,
            String columns,
            String streamArn,
            boolean source,
            List<String> formatOptions) {
        List<String> with = new ArrayList<>();
        with.add("'connector' = 'kinesis'");
        with.add("'stream.arn' = '" + streamArn + "'");
        with.add("'aws.region' = '" + KINESIS_REGION + "'");
        with.add("'aws.endpoint' = '" + LOCALSTACK.getEndpoint() + "'");
        with.add("'aws.credentials.provider' = 'BASIC'");
        with.add("'aws.credentials.basic.accesskeyid' = 'accessKeyId'");
        with.add("'aws.credentials.basic.secretkey' = 'secretAccessKey'");
        with.add("'aws.trust.all.certificates' = 'true'");
        with.add("'aws.http.protocol.version' = 'HTTP1_1'");
        if (source) {
            with.add("'source.init.position' = 'TRIM_HORIZON'");
        }
        with.add("'format' = 'avro-glue'");
        with.add("'avro-glue.aws.region' = '" + GSR_REGION + "'");
        with.add("'avro-glue.registry.name' = '" + REGISTRY_NAME + "'");
        with.addAll(formatOptions);

        String ddl =
                "CREATE TABLE "
                        + tableName
                        + " ("
                        + columns
                        + ") WITH ("
                        + String.join(", ", with)
                        + ")";
        tEnv.executeSql(ddl);
    }

    /** Builds format options starting with the (required) schema name, then key/value pairs. */
    private List<String> autoRegOpts(String schemaName, String... kvPairs) {
        List<String> opts = new ArrayList<>();
        opts.add("'avro-glue.schema.name' = '" + schemaName + "'");
        for (int i = 0; i + 1 < kvPairs.length; i += 2) {
            opts.add("'" + kvPairs[i] + "' = '" + kvPairs[i + 1] + "'");
        }
        return opts;
    }

    private List<String> sourceOpts(String schemaName) {
        return autoRegOpts(schemaName);
    }

    private List<String> compatOpts(String schemaName, String compatibility) {
        return autoRegOpts(
                schemaName,
                "avro-glue.schema.autoRegistration",
                "true",
                "avro-glue.schema.compatibility",
                compatibility);
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

    private static List<String> firstFieldStrings(List<Row> rows) {
        List<String> names = new ArrayList<>();
        for (Row row : rows) {
            Object first = row.getField(0);
            if (first != null) {
                names.add(first.toString());
            }
        }
        return names;
    }

    private static Row findByOrderId(List<Row> rows, String orderId) {
        for (Row row : rows) {
            Object field = row.getField(0);
            if (field != null && orderId.equals(field.toString())) {
                return row;
            }
        }
        return null;
    }

    /** Creates a Localstack Kinesis stream, waits until ACTIVE, and returns its ARN. */
    private String createStream(String baseName) throws Exception {
        String streamName = baseName + "_" + RUN_ID;
        kinesisClient.createStream(
                CreateStreamRequest.builder().streamName(streamName).shardCount(1).build());

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        while (!streamActive(streamName)) {
            if (deadline.isOverdue()) {
                throw new IllegalStateException("Stream did not become ACTIVE: " + streamName);
            }
            Thread.sleep(500);
        }
        return kinesisClient
                .describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
                .streamDescription()
                .streamARN();
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

    private String schemaName(String base) {
        String name = "flink-avro-glue-sql-e2e-" + base + "-" + RUN_ID;
        CREATED_SCHEMAS.add(name);
        return name;
    }

    private static void deleteCreatedSchemas() {
        if (CREATED_SCHEMAS.isEmpty()) {
            return;
        }
        try (GlueClient glue =
                GlueClient.builder()
                        .region(Region.of(GSR_REGION))
                        .credentialsProvider(itCredentialsProvider())
                        .build()) {
            for (String schema : CREATED_SCHEMAS) {
                try {
                    glue.deleteSchema(
                            DeleteSchemaRequest.builder()
                                    .schemaId(
                                            SchemaId.builder()
                                                    .registryName(REGISTRY_NAME)
                                                    .schemaName(schema)
                                                    .build())
                                    .build());
                    LOG.info("Deleted GSR schema {}", schema);
                } catch (Exception e) {
                    LOG.warn(
                            "Best-effort delete of GSR schema {} failed: {}",
                            schema,
                            e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not create GlueClient for schema cleanup: {}", e.getMessage());
        }
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
