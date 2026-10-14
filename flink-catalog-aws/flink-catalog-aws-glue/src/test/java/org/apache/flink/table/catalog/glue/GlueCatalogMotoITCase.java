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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.Table;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for {@link GlueCatalog} against a moto Glue emulator running in a container.
 *
 * <p>Unlike {@code GlueCatalogTest}, which uses an in-JVM {@code FakeGlueClient}, this test
 * exercises the real AWS SDK wire path (serialization, endpoint handling, error unmarshalling)
 * against moto's Glue implementation.
 *
 * <p>Scope notes on moto fidelity gaps (verified against moto 5.0.28):
 *
 * <ul>
 *   <li>moto does not implement the Glue UDF API ({@code GetUserDefinedFunctions} returns HTTP
 *       500), so function operations are not covered here. This also excludes {@link
 *       GlueCatalog#dropDatabase} from coverage, because its emptiness check lists functions; test
 *       cleanup therefore goes through the raw SDK client instead.
 *   <li>moto does not replicate real Glue's column-name lowercasing behaviour, so case-preservation
 *       semantics are intentionally NOT asserted here; they are verified against real AWS Glue (see
 *       the integration evidence on the PR).
 * </ul>
 */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class GlueCatalogMotoITCase {

    private static final int MOTO_PORT = 5000;

    @Container
    private static final GenericContainer<?> MOTO =
            new GenericContainer<>("motoserver/moto:5.0.28").withExposedPorts(MOTO_PORT);

    private static final AtomicInteger DB_COUNTER = new AtomicInteger();

    private static GlueClient glueClient;
    private static GlueCatalog glueCatalog;

    /** Unique per-test database name; cleaned up through the raw SDK client after each test. */
    private String databaseName;

    @BeforeAll
    static void setUp() {
        URI endpoint =
                URI.create(
                        String.format(
                                "http://%s:%d", MOTO.getHost(), MOTO.getMappedPort(MOTO_PORT)));
        glueClient =
                GlueClient.builder()
                        .endpointOverride(endpoint)
                        .region(Region.US_EAST_1)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create("testing", "testing")))
                        .build();
        glueCatalog = new GlueCatalog("glue_moto_catalog", "default", "us-east-1", glueClient);
        glueCatalog.open();
    }

    @AfterAll
    static void tearDown() {
        if (glueCatalog != null) {
            glueCatalog.close();
        }
    }

    @BeforeEach
    void assignDatabaseName() {
        databaseName = "moto_db_" + DB_COUNTER.incrementAndGet();
    }

    @AfterEach
    void cleanDatabase() {
        // Cleanup goes through the raw SDK client: the catalog's dropDatabase lists functions,
        // which moto does not implement (HTTP 500).
        try {
            List<Table> tables =
                    glueClient.getTables(builder -> builder.databaseName(databaseName)).tableList();
            for (Table table : tables) {
                glueClient.deleteTable(
                        builder -> builder.databaseName(databaseName).name(table.name()));
            }
            glueClient.deleteDatabase(builder -> builder.name(databaseName));
        } catch (EntityNotFoundException e) {
            // Test never created the database (or already removed it) - nothing to clean.
        }
    }

    private static CatalogDatabase database() {
        return new CatalogDatabaseImpl(Collections.emptyMap(), "moto itcase db");
    }

    private static ResolvedCatalogTable kinesisTable(List<String> partitionKeys) {
        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("user_id", DataTypes.STRING()),
                        Column.physical("order_total", DataTypes.DOUBLE()));
        Map<String, String> options = new HashMap<>();
        options.put("connector", "kinesis");
        options.put("stream.arn", "arn:aws:kinesis:us-east-1:000000000000:stream/orders");
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("moto itcase table")
                        .partitionKeys(partitionKeys)
                        .options(options)
                        .build();
        return new ResolvedCatalogTable(table, resolvedSchema);
    }

    @Test
    void testDatabaseCreateAndGet() throws Exception {
        glueCatalog.createDatabase(databaseName, database(), false);

        assertThat(glueCatalog.databaseExists(databaseName)).isTrue();
        assertThat(glueCatalog.listDatabases()).contains(databaseName);
        assertThat(glueCatalog.getDatabase(databaseName).getComment()).isEqualTo("moto itcase db");

        assertThatThrownBy(() -> glueCatalog.createDatabase(databaseName, database(), false))
                .isInstanceOf(DatabaseAlreadyExistException.class);
        // ignoreIfExists must swallow the conflict on the wire path
        glueCatalog.createDatabase(databaseName, database(), true);
    }

    @Test
    void testTableRoundTrip() throws Exception {
        glueCatalog.createDatabase(databaseName, database(), false);
        ObjectPath path = new ObjectPath(databaseName, "orders");

        glueCatalog.createTable(path, kinesisTable(Collections.emptyList()), false);

        assertThat(glueCatalog.tableExists(path)).isTrue();
        assertThat(glueCatalog.listTables(databaseName)).contains("orders");

        CatalogTable readBack = (CatalogTable) glueCatalog.getTable(path);
        assertThat(readBack.getOptions()).containsEntry("connector", "kinesis");
        assertThat(readBack.getOptions())
                .containsEntry(
                        "stream.arn", "arn:aws:kinesis:us-east-1:000000000000:stream/orders");
        assertThat(readBack.getUnresolvedSchema().getColumns()).hasSize(2);

        glueCatalog.dropTable(path, false);
        assertThat(glueCatalog.tableExists(path)).isFalse();
    }

    @Test
    void testGetMissingTableThrows() throws Exception {
        glueCatalog.createDatabase(databaseName, database(), false);

        ObjectPath missing = new ObjectPath(databaseName, "nope");
        assertThatThrownBy(() -> glueCatalog.getTable(missing))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testAlterTableAddsColumnOnWire() throws Exception {
        glueCatalog.createDatabase(databaseName, database(), false);
        ObjectPath path = new ObjectPath(databaseName, "orders");
        glueCatalog.createTable(path, kinesisTable(Collections.emptyList()), false);

        ResolvedSchema widened =
                ResolvedSchema.of(
                        Column.physical("user_id", DataTypes.STRING()),
                        Column.physical("order_total", DataTypes.DOUBLE()),
                        Column.physical("currency", DataTypes.STRING()));
        Map<String, String> options = new HashMap<>();
        options.put("connector", "kinesis");
        options.put("stream.arn", "arn:aws:kinesis:us-east-1:000000000000:stream/orders");
        CatalogTable altered =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(widened).build())
                        .comment("moto itcase table")
                        .partitionKeys(Collections.emptyList())
                        .options(options)
                        .build();
        glueCatalog.alterTable(path, new ResolvedCatalogTable(altered, widened), false);

        CatalogTable readBack = (CatalogTable) glueCatalog.getTable(path);
        assertThat(readBack.getUnresolvedSchema().getColumns()).hasSize(3);
    }

    @Test
    void testPartitionLifecycle() throws Exception {
        glueCatalog.createDatabase(databaseName, database(), false);
        ObjectPath path = new ObjectPath(databaseName, "orders");
        glueCatalog.createTable(path, kinesisTable(Arrays.asList("user_id")), false);

        CatalogPartitionSpec spec =
                new CatalogPartitionSpec(Collections.singletonMap("user_id", "alice"));
        CatalogPartition partition =
                new CatalogPartitionImpl(Collections.emptyMap(), "moto partition");

        glueCatalog.createPartition(path, spec, partition, false);

        assertThat(glueCatalog.partitionExists(path, spec)).isTrue();
        assertThat(glueCatalog.listPartitions(path)).hasSize(1);
        assertThat(glueCatalog.listPartitions(path).get(0).getPartitionSpec())
                .containsEntry("user_id", "alice");

        glueCatalog.dropPartition(path, spec, false);
        assertThat(glueCatalog.partitionExists(path, spec)).isFalse();
    }
}
