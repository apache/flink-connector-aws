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

package org.apache.flink.table.catalog.glue.test;

import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * End-to-end test that stores Kinesis table definitions in the <b>real</b> AWS Glue Data Catalog
 * and then moves data through them with Flink: records are written to a Localstack Kinesis stream,
 * a streaming SQL job reads the catalog-resolved source table, filters, and writes to a second
 * catalog-resolved Kinesis sink table, and the sink stream is read back out of band.
 *
 * <p>This proves that what the catalog persists in Glue is a working Kinesis table definition:
 * every connector option ({@code stream.arn}, {@code aws.endpoint}, {@code
 * aws.credentials.provider}, format, source/sink options) must survive the Glue round-trip
 * unchanged, the schema must resolve to the right JSON fields, and the resolved tables must be
 * usable as source and sink by a real job - not just describable.
 *
 * <p>Glue credentials follow the same gates as {@link GlueCatalogEndToEndITCase}; Kinesis runs in
 * Localstack and needs only Docker.
 */
@Tag("requires-aws-credentials")
@Testcontainers
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class GlueCatalogKinesisEndToEndITCase {

    private static final String ACCESS_KEY = System.getenv("IT_CASE_GLUE_CATALOG_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("IT_CASE_GLUE_CATALOG_SECRET_KEY");
    private static final String SESSION_TOKEN = System.getenv("IT_CASE_GLUE_CATALOG_SESSION_TOKEN");
    private static final String REGION =
            System.getenv().getOrDefault("IT_CASE_GLUE_CATALOG_REGION", "us-east-1");
    private static final boolean USE_DEFAULT_CREDENTIALS =
            Boolean.parseBoolean(
                    System.getenv()
                            .getOrDefault("IT_CASE_GLUE_CATALOG_USE_DEFAULT_CREDENTIALS", "false"));

    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";
    private static final String CATALOG_NAME = "glue_kinesis_e2e";
    private static final String DB_NAME =
            "glue_e2e_kin_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private static final String INPUT_STREAM = "orders";
    private static final String OUTPUT_STREAM = "large_orders";

    /** Stream ARNs as Localstack reports them, resolved once the streams exist. */
    private static String inputStreamArn;

    private static String outputStreamArn;

    @Container
    static final LocalstackContainer LOCALSTACK =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION));

    private static TableEnvironment tEnv;
    private static GlueClient rawGlueClient;
    private static SdkHttpClient httpClient;
    private static KinesisClient kinesisClient;

    @BeforeAll
    static void setUp() {
        assumeThat(USE_DEFAULT_CREDENTIALS || (ACCESS_KEY != null && !ACCESS_KEY.isBlank()))
                .as("Credentials not configured, skipping test...")
                .isTrue();
        if (!USE_DEFAULT_CREDENTIALS) {
            assumeThat(SECRET_KEY).as("Secret key not configured, skipping test...").isNotBlank();
            System.setProperty("aws.accessKeyId", ACCESS_KEY);
            System.setProperty("aws.secretAccessKey", SECRET_KEY);
            if (SESSION_TOKEN != null && !SESSION_TOKEN.isBlank()) {
                System.setProperty("aws.sessionToken", SESSION_TOKEN);
            }
        }
        System.setProperty("aws.region", REGION);
        // Localstack does not speak CBOR.
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        rawGlueClient = GlueClient.builder().build();
        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        LOCALSTACK.getEndpoint(), httpClient, KinesisClient.builder());
        inputStreamArn = createStream(INPUT_STREAM);
        outputStreamArn = createStream(OUTPUT_STREAM);

        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG "
                        + CATALOG_NAME
                        + " WITH ('type' = 'glue', 'region' = '"
                        + REGION
                        + "', 'default-database' = 'default')");
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);
    }

    @AfterAll
    static void tearDown() {
        if (rawGlueClient != null) {
            try {
                for (Table table :
                        rawGlueClient.getTables(b -> b.databaseName(DB_NAME)).tableList()) {
                    rawGlueClient.deleteTable(b -> b.databaseName(DB_NAME).name(table.name()));
                }
                rawGlueClient.deleteDatabase(b -> b.name(DB_NAME));
            } catch (EntityNotFoundException ignored) {
                // already dropped by the test
            }
            rawGlueClient.close();
        }
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.sessionToken");
        System.clearProperty("aws.region");
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    void testKinesisReadAndWriteThroughCatalogResolvedTables() throws Exception {
        tEnv.executeSql("CREATE DATABASE " + DB_NAME);
        // Table definitions live in real Glue; the data plane is Localstack Kinesis.
        tEnv.executeSql(
                "CREATE TABLE "
                        + DB_NAME
                        + ".orders ("
                        + "  orderCode STRING,"
                        + "  quantity BIGINT"
                        + ") WITH ("
                        + kinesisOptions(inputStreamArn)
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'source.shard.discovery.interval' = '1000ms'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE "
                        + DB_NAME
                        + ".large_orders ("
                        + "  orderCode STRING,"
                        + "  quantity BIGINT"
                        + ") WITH ("
                        + kinesisOptions(outputStreamArn)
                        + "  'sink.batch.max-size' = '1'"
                        + ")");

        // The connector options must come back from Glue exactly as declared: they are what
        // makes the table usable at all.
        Table glueTable =
                rawGlueClient.getTable(b -> b.databaseName(DB_NAME).name("orders")).table();
        assertThat(glueTable.parameters())
                .containsEntry("connector", "kinesis")
                .containsEntry("stream.arn", inputStreamArn)
                .containsEntry("aws.endpoint", LOCALSTACK.getEndpoint())
                .containsEntry("format", "json");
        // A Kinesis table has no filesystem location; Glue must not be shown one.
        assertThat(glueTable.storageDescriptor().location()).isNull();

        // WRITE side of the pipeline: put JSON records on the input stream.
        for (int i = 1; i <= 20; i++) {
            putRecord(INPUT_STREAM, "{\"orderCode\":\"order-" + i + "\",\"quantity\":" + i + "}");
        }

        // A real streaming job from the catalog-resolved Kinesis source to the
        // catalog-resolved Kinesis sink, filtering on a restored camelCase column.
        TableResult job =
                tEnv.executeSql(
                        "INSERT INTO "
                                + DB_NAME
                                + ".large_orders SELECT orderCode, quantity FROM "
                                + DB_NAME
                                + ".orders WHERE quantity > 10");
        JobClient jobClient = job.getJobClient().orElseThrow(IllegalStateException::new);
        try {
            // READ side: the sink stream must end up with exactly the 10 filtered records.
            List<String> output = readRecords(OUTPUT_STREAM, 10, Duration.ofMinutes(3), jobClient);
            assertThat(output)
                    .hasSize(10)
                    .allSatisfy(
                            json -> {
                                assertThat(json).contains("\"orderCode\":\"order-");
                                long quantity =
                                        Long.parseLong(
                                                json.replaceAll(".*\"quantity\":(\\d+).*", "$1"));
                                assertThat(quantity).isGreaterThan(10);
                            });
        } finally {
            cancelQuietly(jobClient);
        }

        tEnv.executeSql("DROP TABLE " + DB_NAME + ".orders");
        tEnv.executeSql("DROP TABLE " + DB_NAME + ".large_orders");
        tEnv.executeSql("DROP DATABASE " + DB_NAME);
    }

    private static String kinesisOptions(String streamArn) {
        return "  'connector' = 'kinesis',"
                + "  'stream.arn' = '"
                + streamArn
                + "',"
                + "  'aws.region' = '"
                + REGION
                + "',"
                + "  'aws.endpoint' = '"
                + LOCALSTACK.getEndpoint()
                + "',"
                + "  'aws.credentials.provider' = 'BASIC',"
                + "  'aws.credentials.basic.accesskeyid' = 'localstack',"
                + "  'aws.credentials.basic.secretkey' = 'localstack',"
                + "  'aws.trust.all.certificates' = 'true',"
                + "  'aws.http.protocol.version' = 'HTTP1_1',"
                + "  'format' = 'json',";
    }

    /** Creates the stream, waits until it is ACTIVE and returns its ARN. */
    private static String createStream(String streamName) {
        kinesisClient.createStream(b -> b.streamName(streamName).shardCount(1));
        long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
        while (true) {
            StreamDescription description =
                    kinesisClient.describeStream(b -> b.streamName(streamName)).streamDescription();
            if (description.streamStatus() == StreamStatus.ACTIVE) {
                return description.streamARN();
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Stream " + streamName + " did not become ACTIVE");
            }
            sleep(500);
        }
    }

    private static void putRecord(String streamName, String json) {
        kinesisClient.putRecord(
                PutRecordRequest.builder()
                        .streamName(streamName)
                        .partitionKey("pk")
                        .data(SdkBytes.fromString(json, StandardCharsets.UTF_8))
                        .build());
    }

    private static List<String> readRecords(
            String streamName, int expected, Duration timeout, JobClient jobClient)
            throws Exception {
        String shardId =
                kinesisClient
                        .describeStream(b -> b.streamName(streamName))
                        .streamDescription()
                        .shards()
                        .get(0)
                        .shardId();
        String iterator =
                kinesisClient
                        .getShardIterator(
                                b ->
                                        b.streamName(streamName)
                                                .shardId(shardId)
                                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON))
                        .shardIterator();
        List<String> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (records.size() < expected && System.nanoTime() < deadline) {
            // A failed job would otherwise only surface as an empty stream after the timeout.
            // The local cluster is gone once the job terminates, so the result future (captured
            // at submission) is the only reliable signal; get() rethrows the job's own failure.
            if (jobClient.getJobExecutionResult().isDone()) {
                jobClient.getJobExecutionResult().get(30, TimeUnit.SECONDS);
                throw new IllegalStateException(
                        "Streaming job terminated before producing the expected output");
            }
            String current = iterator;
            GetRecordsResponse response = kinesisClient.getRecords(b -> b.shardIterator(current));
            for (Record record : response.records()) {
                records.add(record.data().asUtf8String());
            }
            iterator = response.nextShardIterator();
            if (response.records().isEmpty()) {
                sleep(500);
            }
        }
        return records;
    }

    private static void cancelQuietly(JobClient jobClient) {
        try {
            if (!jobClient.getJobExecutionResult().isDone()) {
                jobClient.cancel().get(1, TimeUnit.MINUTES);
            }
        } catch (Exception e) {
            // The job (and its local cluster) is already gone; nothing left to cancel.
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
