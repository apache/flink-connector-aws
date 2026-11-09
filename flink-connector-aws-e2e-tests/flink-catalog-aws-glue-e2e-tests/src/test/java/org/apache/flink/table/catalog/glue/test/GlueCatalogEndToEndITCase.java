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

package org.apache.flink.table.catalog.glue.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.Table;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * End-to-end test for the Glue catalog against <b>real AWS Glue</b>.
 *
 * <p>This test exercises the complete user path: {@code CREATE CATALOG ... WITH ('type'='glue')}
 * discovers {@code GlueCatalogFactory} via SPI, the factory builds its own {@code GlueClient} from
 * the default credential/region chain, and all catalog operations flow through Flink SQL DDL and
 * the planner down to the real Glue service.
 *
 * <p>Following the convention of the other AWS end-to-end tests in this repository, the test is
 * gated on explicit credentials and skips cleanly when they are absent:
 *
 * <ul>
 *   <li>{@code IT_CASE_GLUE_CATALOG_ACCESS_KEY} - AWS access key id (required)
 *   <li>{@code IT_CASE_GLUE_CATALOG_SECRET_KEY} - AWS secret access key (required)
 *   <li>{@code IT_CASE_GLUE_CATALOG_SESSION_TOKEN} - session token (optional, for temporary
 *       credentials)
 *   <li>{@code IT_CASE_GLUE_CATALOG_REGION} - region (optional, defaults to {@code us-east-1})
 * </ul>
 *
 * <p>Credentials are handed to the factory-built client through the AWS SDK system properties
 * ({@code aws.accessKeyId}, {@code aws.secretAccessKey}, {@code aws.sessionToken}, {@code
 * aws.region}), which the default provider chain resolves - no test-only code path exists in the
 * factory.
 *
 * <p>Unlike the moto-based integration tests in the catalog module, this test also covers the two
 * behaviours only real Glue exhibits: column-name lowercasing with case restoration through the
 * catalog, and {@code dropDatabase} (whose emptiness check requires the UDF API that moto does not
 * implement).
 *
 * <p>Run with: {@code mvn verify -Prun-aws-end-to-end-tests} with the environment variables above.
 */
@Tag("requires-aws-credentials")
class GlueCatalogEndToEndITCase {

    private static final String ACCESS_KEY = System.getenv("IT_CASE_GLUE_CATALOG_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("IT_CASE_GLUE_CATALOG_SECRET_KEY");
    private static final String SESSION_TOKEN = System.getenv("IT_CASE_GLUE_CATALOG_SESSION_TOKEN");
    private static final String REGION =
            System.getenv().getOrDefault("IT_CASE_GLUE_CATALOG_REGION", "us-east-1");

    /**
     * Alternative gate for environments where extracting key material is undesirable (SSO, instance
     * profiles, credential_process): when {@code true}, the test runs with the AWS default
     * credential provider chain instead of explicit keys.
     */
    private static final boolean USE_DEFAULT_CREDENTIALS =
            Boolean.parseBoolean(
                    System.getenv()
                            .getOrDefault("IT_CASE_GLUE_CATALOG_USE_DEFAULT_CREDENTIALS", "false"));

    private static final String CATALOG_NAME = "glue_e2e";
    private static final String DB_NAME =
            "flink_e2e_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    private static final String DATA_DB_NAME = DB_NAME + "_data";
    private static final String FIDELITY_DB_NAME = DB_NAME + "_fid";
    private static final String PARTITIONED_DB_NAME = DB_NAME + "_part";
    private static final String FOREIGN_DB_NAME = DB_NAME + "_foreign";

    private static TableEnvironment tEnv;
    private static GlueClient rawGlueClient;

    @BeforeAll
    static void setUp() {
        assumeThat(USE_DEFAULT_CREDENTIALS || (ACCESS_KEY != null && !ACCESS_KEY.isBlank()))
                .as("Credentials not configured, skipping test...")
                .isTrue();

        GlueClientBuilder rawClientBuilder = GlueClient.builder().region(Region.of(REGION));
        if (!USE_DEFAULT_CREDENTIALS) {
            assumeThat(SECRET_KEY).as("Secret key not configured, skipping test...").isNotBlank();

            // Route the factory-built GlueClient's default provider chain to the test
            // credentials.
            System.setProperty("aws.accessKeyId", ACCESS_KEY);
            System.setProperty("aws.secretAccessKey", SECRET_KEY);
            if (SESSION_TOKEN != null && !SESSION_TOKEN.isBlank()) {
                System.setProperty("aws.sessionToken", SESSION_TOKEN);
            }

            // Independent client for out-of-band verification and cleanup.
            AwsCredentials rawCredentials =
                    (SESSION_TOKEN != null && !SESSION_TOKEN.isBlank())
                            ? AwsSessionCredentials.create(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN)
                            : AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY);
            rawClientBuilder.credentialsProvider(StaticCredentialsProvider.create(rawCredentials));
        }
        System.setProperty("aws.region", REGION);
        rawGlueClient = rawClientBuilder.build();

        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG "
                        + CATALOG_NAME
                        + " WITH ('type' = 'glue', 'region' = '"
                        + REGION
                        + "', 'default-database' = 'default')");
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);
    }

    @AfterEach
    void cleanUpTables() {
        if (rawGlueClient == null) {
            return;
        }
        for (String db : new String[] {DB_NAME, DATA_DB_NAME}) {
            try {
                rawGlueClient
                        .getTables(b -> b.databaseName(db))
                        .tableList()
                        .forEach(
                                t ->
                                        rawGlueClient.deleteTable(
                                                b -> b.databaseName(db).name(t.name())));
            } catch (EntityNotFoundException ignored) {
                // database already gone
            }
        }
    }

    @AfterAll
    static void tearDown() {
        if (rawGlueClient != null) {
            for (String db :
                    new String[] {
                        DB_NAME,
                        DATA_DB_NAME,
                        FIDELITY_DB_NAME,
                        PARTITIONED_DB_NAME,
                        FOREIGN_DB_NAME
                    }) {
                try {
                    rawGlueClient
                            .getTables(b -> b.databaseName(db))
                            .tableList()
                            .forEach(
                                    t ->
                                            rawGlueClient.deleteTable(
                                                    b -> b.databaseName(db).name(t.name())));
                    rawGlueClient.deleteDatabase(b -> b.name(db));
                } catch (EntityNotFoundException ignored) {
                    // already dropped by the test
                }
            }
            rawGlueClient.close();
        }
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.sessionToken");
        System.clearProperty("aws.region");
    }

    @Test
    void testDatabaseAndTableLifecycleEndToEnd() {
        tEnv.executeSql(
                "CREATE DATABASE IF NOT EXISTS "
                        + DB_NAME
                        + " COMMENT 'Flink Glue catalog e2e test database'");
        assertThat(sql("SHOW DATABASES")).extracting(r -> r.getField(0)).contains(DB_NAME);

        // All object names are fully qualified so the test never depends on a pre-existing
        // 'default' database in the target account.
        tEnv.executeSql(
                "CREATE TABLE "
                        + DB_NAME
                        + ".orders_e2e ("
                        + "  orderId STRING,"
                        + "  orderTotal DOUBLE,"
                        + "  orderTime TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = 'arn:aws:kinesis:"
                        + REGION
                        + ":000000000000:stream/e2e-orders',"
                        + "  'format' = 'json'"
                        + ")");
        assertThat(sql("SHOW TABLES FROM " + DB_NAME))
                .extracting(r -> r.getField(0))
                .contains("orders_e2e");

        // Case restoration through the catalog: real Glue lowercases physical column names,
        // the catalog must restore the original camelCase on read.
        List<String> describedColumns =
                sql("DESCRIBE " + DB_NAME + ".orders_e2e").stream()
                        .map(r -> String.valueOf(r.getField(0)))
                        .collect(Collectors.toList());
        assertThat(describedColumns).containsExactly("orderId", "orderTotal", "orderTime");

        // Out-of-band wire verification: the table exists in real Glue, and Glue stored the
        // physical column names lowercased (the behaviour emulators do not reproduce).
        List<Column> glueColumns =
                rawGlueClient
                        .getTable(
                                GetTableRequest.builder()
                                        .databaseName(DB_NAME)
                                        .name("orders_e2e")
                                        .build())
                        .table()
                        .storageDescriptor()
                        .columns();
        assertThat(glueColumns)
                .extracting(Column::name)
                .containsExactly("orderid", "ordertotal", "ordertime");

        // Full teardown through SQL - including dropDatabase, whose emptiness check
        // (listFunctions) cannot run against moto and is only covered here.
        tEnv.executeSql("DROP TABLE " + DB_NAME + ".orders_e2e");
        assertThat(sql("SHOW TABLES FROM " + DB_NAME))
                .extracting(r -> r.getField(0))
                .doesNotContain("orders_e2e");

        tEnv.executeSql("DROP DATABASE " + DB_NAME);
        assertThat(sql("SHOW DATABASES")).extracting(r -> r.getField(0)).doesNotContain(DB_NAME);
    }

    @Test
    void testSchemaFidelityEndToEnd() {
        // Watermarks, primary keys, computed and metadata columns cannot be represented as
        // Glue columns; they round-trip through flink.schema.* table parameters. Expression
        // validation makes the planner probe the catalog's function APIs, so this test also
        // exercises getFunction/functionExists against real Glue.
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + FIDELITY_DB_NAME);
        tEnv.executeSql(
                "CREATE TABLE "
                        + FIDELITY_DB_NAME
                        + ".events_fidelity ("
                        + "  userId STRING,"
                        + "  eventTime TIMESTAMP(3),"
                        + "  price DOUBLE,"
                        + "  doublePrice AS price * 2,"
                        + "  kafkaOffset BIGINT METADATA FROM 'offset' VIRTUAL,"
                        + "  WATERMARK FOR eventTime AS eventTime - INTERVAL '5' SECOND,"
                        + "  PRIMARY KEY (userId) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = 'arn:aws:kinesis:"
                        + REGION
                        + ":000000000000:stream/e2e-fidelity',"
                        + "  'format' = 'json'"
                        + ")");

        String createTable =
                sql("SHOW CREATE TABLE " + FIDELITY_DB_NAME + ".events_fidelity")
                        .get(0)
                        .getField(0)
                        .toString();
        assertThat(createTable)
                .contains("`doublePrice` AS `price` * 2")
                .contains("`kafkaOffset` BIGINT METADATA FROM 'offset' VIRTUAL")
                .contains("WATERMARK FOR `eventTime` AS `eventTime` - INTERVAL '5' SECOND")
                .contains("PRIMARY KEY (`userId`) NOT ENFORCED");

        // Column order preserved, including the interleaved non-physical columns.
        assertThat(sql("DESCRIBE " + FIDELITY_DB_NAME + ".events_fidelity"))
                .extracting(r -> String.valueOf(r.getField(0)))
                .containsExactly("userId", "eventTime", "price", "doublePrice", "kafkaOffset");

        // Only the physical columns land as Glue columns on the real service.
        List<Column> glueColumns =
                rawGlueClient
                        .getTable(
                                GetTableRequest.builder()
                                        .databaseName(FIDELITY_DB_NAME)
                                        .name("events_fidelity")
                                        .build())
                        .table()
                        .storageDescriptor()
                        .columns();
        assertThat(glueColumns)
                .extracting(Column::name)
                .containsExactly("userid", "eventtime", "price");

        tEnv.executeSql("DROP TABLE " + FIDELITY_DB_NAME + ".events_fidelity");
        tEnv.executeSql("DROP DATABASE " + FIDELITY_DB_NAME);
    }

    @Test
    void testPartitionedTableEndToEnd() {
        // Real Glue stores partition keys outside the storage descriptor and returns them after
        // the data columns; a partition key declared FIRST is the case that reorders columns on
        // read, so every column must still come back at its declared position with its declared
        // type, and partition DDL must work against the real service.
        String db = PARTITIONED_DB_NAME;
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + db);
        tEnv.executeSql(
                "CREATE TABLE "
                        + db
                        + ".sales ("
                        + "  region STRING,"
                        + "  ts TIMESTAMP(3),"
                        + "  id INT NOT NULL,"
                        + "  amount DECIMAL(10, 2),"
                        + "  createdAt TIMESTAMP_LTZ(3),"
                        + "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"
                        + ") PARTITIONED BY (region) WITH ("
                        + "  'connector' = 'filesystem',"
                        + "  'path' = 's3://bucket/e2e-sales',"
                        + "  'format' = 'json'"
                        + ")");

        assertThat(sql("DESCRIBE " + db + ".sales"))
                .extracting(
                        r -> String.valueOf(r.getField(0)),
                        r -> String.valueOf(r.getField(1)),
                        r -> r.getField(2))
                .containsExactly(
                        tuple("region", "STRING", true),
                        tuple("ts", "TIMESTAMP(3) *ROWTIME*", true),
                        tuple("id", "INT", false),
                        tuple("amount", "DECIMAL(10, 2)", true),
                        tuple("createdAt", "TIMESTAMP_LTZ(3)", true));
        assertThat(sql("SHOW CREATE TABLE " + db + ".sales").get(0).getField(0).toString())
                .contains("PARTITIONED BY (`region`)");

        Table glueTable =
                rawGlueClient
                        .getTable(GetTableRequest.builder().databaseName(db).name("sales").build())
                        .table();
        assertThat(glueTable.partitionKeys()).extracting(Column::name).containsExactly("region");
        assertThat(glueTable.storageDescriptor().columns())
                .extracting(Column::name)
                .containsExactly("ts", "id", "amount", "createdat");

        // Partition DDL against real Glue.
        tEnv.executeSql("ALTER TABLE " + db + ".sales ADD PARTITION (region = 'eu')");
        tEnv.executeSql("ALTER TABLE " + db + ".sales ADD PARTITION (region = 'us')");
        assertThat(sql("SHOW PARTITIONS " + db + ".sales"))
                .extracting(r -> String.valueOf(r.getField(0)))
                .containsExactlyInAnyOrder("region=eu", "region=us");
        tEnv.executeSql("ALTER TABLE " + db + ".sales DROP PARTITION (region = 'eu')");
        assertThat(sql("SHOW PARTITIONS " + db + ".sales"))
                .extracting(r -> String.valueOf(r.getField(0)))
                .containsExactly("region=us");

        tEnv.executeSql("DROP TABLE " + db + ".sales");
        tEnv.executeSql("DROP DATABASE " + db);
    }

    @Test
    void testTablesWrittenByOtherEnginesAreReadable() {
        // Tables written by Athena, Spark, Hive or a crawler are never produced by Flink's own
        // writer, so this is the only path where the real service hands the catalog uppercase,
        // parameterized and complex Hive type strings and non-Flink table types. Create such
        // objects out of band, exactly as Athena DDL stores them, then read them through Flink.
        String db = FOREIGN_DB_NAME;
        rawGlueClient.createDatabase(b -> b.databaseInput(d -> d.name(db)));
        rawGlueClient.createTable(
                b ->
                        b.databaseName(db)
                                .tableInput(
                                        t ->
                                                t.name("athena_events")
                                                        .tableType("EXTERNAL_TABLE")
                                                        .parameters(
                                                                java.util.Collections.singletonMap(
                                                                        "classification",
                                                                        "parquet"))
                                                        .partitionKeys(
                                                                Column.builder()
                                                                        .name("dt")
                                                                        .type("string")
                                                                        .build())
                                                        .storageDescriptor(
                                                                sd ->
                                                                        sd.location(
                                                                                        "s3://bucket/athena/events/")
                                                                                .columns(
                                                                                        Column
                                                                                                .builder()
                                                                                                .name(
                                                                                                        "id")
                                                                                                .type(
                                                                                                        "BIGINT")
                                                                                                .build(),
                                                                                        Column
                                                                                                .builder()
                                                                                                .name(
                                                                                                        "name")
                                                                                                .type(
                                                                                                        "varchar(255)")
                                                                                                .build(),
                                                                                        Column
                                                                                                .builder()
                                                                                                .name(
                                                                                                        "code")
                                                                                                .type(
                                                                                                        "CHAR(3)")
                                                                                                .build(),
                                                                                        Column
                                                                                                .builder()
                                                                                                .name(
                                                                                                        "price")
                                                                                                .type(
                                                                                                        "DECIMAL(10,2)")
                                                                                                .build(),
                                                                                        Column
                                                                                                .builder()
                                                                                                .name(
                                                                                                        "tags")
                                                                                                .type(
                                                                                                        "ARRAY<STRING>")
                                                                                                .build(),
                                                                                        Column
                                                                                                .builder()
                                                                                                .name(
                                                                                                        "attrs")
                                                                                                .type(
                                                                                                        "MAP<STRING,INT>")
                                                                                                .build(),
                                                                                        Column
                                                                                                .builder()
                                                                                                .name(
                                                                                                        "address")
                                                                                                .type(
                                                                                                        "STRUCT<city:STRING,zip:INT>")
                                                                                                .build()))));
        rawGlueClient.createTable(
                b ->
                        b.databaseName(db)
                                .tableInput(
                                        t ->
                                                t.name("athena_view")
                                                        .tableType("VIRTUAL_VIEW")
                                                        .viewOriginalText(
                                                                "SELECT id, name FROM athena_events")
                                                        .viewExpandedText(
                                                                "SELECT id, name FROM athena_events")
                                                        .storageDescriptor(
                                                                sd ->
                                                                        sd.columns(
                                                                                Column.builder()
                                                                                        .name("id")
                                                                                        .type(
                                                                                                "bigint")
                                                                                        .build(),
                                                                                Column.builder()
                                                                                        .name(
                                                                                                "name")
                                                                                        .type(
                                                                                                "string")
                                                                                        .build()))));

        assertThat(sql("SHOW TABLES IN " + db))
                .extracting(r -> String.valueOf(r.getField(0)))
                .containsExactlyInAnyOrder("athena_events", "athena_view");
        assertThat(sql("SHOW VIEWS IN " + db))
                .extracting(r -> String.valueOf(r.getField(0)))
                .containsExactly("athena_view");

        // Every foreign type string is understood, whatever its case, and partition keys come
        // back as columns.
        assertThat(sql("DESCRIBE " + db + ".athena_events"))
                .extracting(r -> String.valueOf(r.getField(0)), r -> String.valueOf(r.getField(1)))
                .containsExactly(
                        tuple("id", "BIGINT"),
                        tuple("name", "VARCHAR(255)"),
                        tuple("code", "CHAR(3)"),
                        tuple("price", "DECIMAL(10, 2)"),
                        tuple("tags", "ARRAY<STRING>"),
                        tuple("attrs", "MAP<STRING, INT>"),
                        tuple("address", "ROW<`city` STRING, `zip` INT>"),
                        tuple("dt", "STRING"));
        assertThat(sql("SHOW CREATE TABLE " + db + ".athena_events").get(0).getField(0).toString())
                .contains("PARTITIONED BY (`dt`)");
        assertThat(sql("DESCRIBE " + db + ".athena_view"))
                .extracting(r -> String.valueOf(r.getField(0)))
                .containsExactly("id", "name");

        // The documented way to query a foreign table: a Flink table over the same data that
        // reuses the Glue schema without touching the object the other engine owns.
        tEnv.executeSql(
                "CREATE TABLE "
                        + db
                        + ".flink_events WITH ("
                        + "  'connector' = 'filesystem',"
                        + "  'path' = 's3://bucket/athena/events/',"
                        + "  'format' = 'parquet'"
                        + ") LIKE "
                        + db
                        + ".athena_events");
        assertThat(sql("DESCRIBE " + db + ".flink_events"))
                .extracting(r -> String.valueOf(r.getField(0)))
                .containsExactly("id", "name", "code", "price", "tags", "attrs", "address", "dt");
        Table foreign =
                rawGlueClient
                        .getTable(
                                GetTableRequest.builder()
                                        .databaseName(db)
                                        .name("athena_events")
                                        .build())
                        .table();
        assertThat(foreign.tableType()).isEqualTo("EXTERNAL_TABLE");
        assertThat(foreign.parameters())
                .doesNotContainKeys("connector", "flink.schema.column-order");

        tEnv.executeSql("DROP TABLE " + db + ".flink_events");
        tEnv.executeSql("DROP DATABASE " + db + " CASCADE");
    }

    @Test
    void testDataRoundTripThroughCatalogResolvedTables(@TempDir Path sinkDir) throws Exception {
        // Batch mode so the filesystem sink finalizes its files on job completion
        // (a streaming filesystem sink only commits files on checkpoints).
        TableEnvironment batchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        batchEnv.executeSql(
                "CREATE CATALOG "
                        + CATALOG_NAME
                        + " WITH ('type' = 'glue', 'region' = '"
                        + REGION
                        + "', 'default-database' = 'default')");
        batchEnv.executeSql("USE CATALOG " + CATALOG_NAME);
        batchEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + DATA_DB_NAME);

        // Both tables are REGISTERED IN REAL GLUE; execution resolves connector, format and
        // schema from what Glue stored - proving property and type fidelity through the wire.
        batchEnv.executeSql(
                "CREATE TABLE "
                        + DATA_DB_NAME
                        + ".orders_src ("
                        + "  orderId STRING,"
                        + "  orderTotal DOUBLE,"
                        + "  orderTime TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'datagen',"
                        + "  'number-of-rows' = '100',"
                        + "  'fields.orderId.length' = '12'"
                        + ")");
        batchEnv.executeSql(
                "CREATE TABLE "
                        + DATA_DB_NAME
                        + ".orders_copy ("
                        + "  orderId STRING,"
                        + "  orderTotal DOUBLE,"
                        + "  orderTime TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'filesystem',"
                        + "  'path' = '"
                        + sinkDir.toUri()
                        + "',"
                        + "  'format' = 'json'"
                        + ")");

        // WRITE: a real Flink job from the catalog-resolved datagen source into the
        // catalog-resolved filesystem sink.
        batchEnv.executeSql(
                        "INSERT INTO "
                                + DATA_DB_NAME
                                + ".orders_copy SELECT * FROM "
                                + DATA_DB_NAME
                                + ".orders_src")
                .await();

        // READ: back through the catalog, projecting and filtering on restored camelCase
        // columns to prove runtime column resolution, not just DESCRIBE output.
        List<Row> rows = new ArrayList<>();
        batchEnv.executeSql(
                        "SELECT orderId, orderTotal FROM "
                                + DATA_DB_NAME
                                + ".orders_copy WHERE orderId IS NOT NULL"
                                + " AND orderTime IS NOT NULL")
                .collect()
                .forEachRemaining(rows::add);
        assertThat(rows).hasSize(100);
        assertThat(rows)
                .allSatisfy(
                        row -> {
                            assertThat((String) row.getField(0)).hasSize(12);
                            assertThat(row.getField(1)).isInstanceOf(Double.class);
                        });

        // Teardown through SQL DDL against real Glue.
        batchEnv.executeSql("DROP TABLE " + DATA_DB_NAME + ".orders_src");
        batchEnv.executeSql("DROP TABLE " + DATA_DB_NAME + ".orders_copy");
        batchEnv.executeSql("DROP DATABASE " + DATA_DB_NAME);
    }

    private static List<Row> sql(String statement) {
        List<Row> rows = new ArrayList<>();
        tEnv.executeSql(statement).collect().forEachRemaining(rows::add);
        return rows;
    }
}
