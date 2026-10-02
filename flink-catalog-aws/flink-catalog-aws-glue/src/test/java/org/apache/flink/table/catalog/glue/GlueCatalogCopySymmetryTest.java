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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.glue.operator.FakeGlueClient;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Read/write symmetry of the catalog API. Everything a read method returns must be safe to feed
 * into the matching write method for a <em>different</em> object: {@code getX(a)} followed by
 * {@code createX(b, result)} must produce a {@code b} that reads back identically to {@code a}, and
 * must never leak catalog bookkeeping or Glue storage metadata into the copy. This is the pattern a
 * catalog migration or "clone" tool follows, and the review round that found the partition location
 * copy bug.
 */
@ExtendWith(RealGlueCleanupExtension.class)
class GlueCatalogCopySymmetryTest {

    private GlueClient glueClient;
    private GlueCatalog glueCatalog;
    private String db;

    @BeforeEach
    void setUp() throws Exception {
        glueClient = GlueTestClientFactory.createClient();
        glueCatalog = new GlueCatalog("glueCatalog", "default", "us-east-1", glueClient);
        db = GlueTestClientFactory.uniqueName("copydb");
        glueCatalog.createDatabase(
                db, new CatalogDatabaseImpl(Collections.emptyMap(), "copy tests"), false);
    }

    @AfterEach
    void tearDown() {
        glueCatalog.close();
    }

    // ------------------------------------------------------------------------------------------
    // getX(a) -> createX(b, result) -> getX(b) == getX(a)
    // ------------------------------------------------------------------------------------------

    @Test
    void testTableCopyReadsBackIdentically() throws Exception {
        ObjectPath source = new ObjectPath(db, "source_tbl");
        ObjectPath copy = new ObjectPath(db, "copy_tbl");

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("userId", DataTypes.STRING().notNull()),
                        Column.physical("eventTime", DataTypes.TIMESTAMP(3)),
                        Column.physical("amount", DataTypes.DECIMAL(10, 2)).withComment("money"),
                        Column.physical("region", DataTypes.STRING()));
        Map<String, String> options = new HashMap<>();
        options.put("connector", "datagen");
        options.put("number-of-rows", "3");
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("source comment")
                        .partitionKeys(Collections.singletonList("region"))
                        .options(options)
                        .build();
        glueCatalog.createTable(source, new ResolvedCatalogTable(table, resolvedSchema), false);

        CatalogBaseTable read = glueCatalog.getTable(source);
        // The read result must be a plain user-level definition: no bookkeeping, nothing that
        // the connector factory would reject as an unknown option.
        assertThat(read.getOptions()).containsExactlyInAnyOrderEntriesOf(options);

        // Copy it. The planner would resolve the schema; do the same here.
        glueCatalog.createTable(
                copy, new ResolvedCatalogTable((CatalogTable) read, resolvedSchema), false);

        CatalogBaseTable readCopy = glueCatalog.getTable(copy);
        assertThat(readCopy.getOptions()).containsExactlyInAnyOrderEntriesOf(options);
        assertThat(readCopy.getComment()).isEqualTo(read.getComment());
        assertThat(((CatalogTable) readCopy).getPartitionKeys())
                .isEqualTo(((CatalogTable) read).getPartitionKeys());
        assertThat(readCopy.getUnresolvedSchema().toString())
                .isEqualTo(read.getUnresolvedSchema().toString());

        // And the copy is a first-class table on the Glue side: its own name, own bookkeeping.
        Table glueCopy =
                glueClient.getTable(b -> b.databaseName(db.toLowerCase()).name("copy_tbl")).table();
        assertThat(glueCopy.parameters())
                .containsEntry("flink.original-table-name", "copy_tbl")
                .doesNotContainKey("owner")
                .doesNotContainKey("table.input.format");
    }

    @Test
    void testViewCopyReadsBackIdentically() throws Exception {
        ObjectPath source = new ObjectPath(db, "source_view");
        ObjectPath copy = new ObjectPath(db, "copy_view");

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.INT()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3)));
        CatalogView view =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "view comment",
                        "SELECT id, ts FROM t",
                        "SELECT `t`.`id`, `t`.`ts` FROM `glueCatalog`.`" + db + "`.`t` AS `t`",
                        Collections.emptyMap());
        glueCatalog.createTable(source, new ResolvedCatalogView(view, resolvedSchema), false);

        CatalogBaseTable read = glueCatalog.getTable(source);
        assertThat(read).isInstanceOf(CatalogView.class);
        assertThat(read.getOptions()).isEmpty();

        glueCatalog.createTable(
                copy, new ResolvedCatalogView((CatalogView) read, resolvedSchema), false);
        CatalogView readCopy = (CatalogView) glueCatalog.getTable(copy);

        assertThat(readCopy.getOriginalQuery()).isEqualTo(((CatalogView) read).getOriginalQuery());
        assertThat(readCopy.getExpandedQuery()).isEqualTo(((CatalogView) read).getExpandedQuery());
        assertThat(readCopy.getComment()).isEqualTo(read.getComment());
        assertThat(readCopy.getOptions()).isEmpty();
        assertThat(readCopy.getUnresolvedSchema().toString())
                .isEqualTo(read.getUnresolvedSchema().toString());
    }

    @Test
    void testDatabaseCopyReadsBackIdentically() throws Exception {
        String copyDb = GlueTestClientFactory.uniqueName("copydb2");
        Map<String, String> props = new HashMap<>();
        props.put("team", "streaming");
        props.put("tier", "gold");
        glueCatalog.dropDatabase(db, false, true);
        glueCatalog.createDatabase(db, new CatalogDatabaseImpl(props, "db comment"), false);

        CatalogDatabase read = glueCatalog.getDatabase(db);
        // No bookkeeping in the user-visible properties.
        assertThat(read.getProperties()).containsExactlyInAnyOrderEntriesOf(props);

        glueCatalog.createDatabase(copyDb, read, false);
        CatalogDatabase readCopy = glueCatalog.getDatabase(copyDb);
        assertThat(readCopy.getProperties()).containsExactlyInAnyOrderEntriesOf(props);
        assertThat(readCopy.getComment()).isEqualTo(read.getComment());
        // The copy carries its own original name, not the source's.
        assertThat(
                        glueClient
                                .getDatabase(b -> b.name(copyDb.toLowerCase()))
                                .database()
                                .parameters())
                .containsEntry("flink.original-database-name", copyDb);
    }

    @Test
    void testFunctionCopyReadsBackIdentically() throws Exception {
        assumeThat(glueClient)
                .as("moto/real Glue UDF API is covered by the e2e suite")
                .isInstanceOf(FakeGlueClient.class);
        ObjectPath source = new ObjectPath(db, "source_fn");
        ObjectPath copy = new ObjectPath(db, "copy_fn");
        CatalogFunction fn =
                new CatalogFunctionImpl("com.example.MyScalar", FunctionLanguage.SCALA);
        glueCatalog.createFunction(source, fn, false);

        CatalogFunction read = glueCatalog.getFunction(source);
        // The language prefix the catalog stores in Glue must not leak into the class name.
        assertThat(read.getClassName()).isEqualTo("com.example.MyScalar");
        assertThat(read.getFunctionLanguage()).isEqualTo(FunctionLanguage.SCALA);

        glueCatalog.createFunction(copy, read, false);
        CatalogFunction readCopy = glueCatalog.getFunction(copy);
        assertThat(readCopy.getClassName()).isEqualTo(read.getClassName());
        assertThat(readCopy.getFunctionLanguage()).isEqualTo(read.getFunctionLanguage());
    }

    @Test
    void testAlterTableWithItsOwnReadResultIsANoOp() throws Exception {
        ObjectPath path = new ObjectPath(db, "alter_me");
        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.INT().notNull()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3)));
        Map<String, String> options = new HashMap<>();
        options.put("connector", "datagen");
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("before")
                        .options(options)
                        .build();
        glueCatalog.createTable(path, new ResolvedCatalogTable(table, resolvedSchema), false);
        CatalogBaseTable before = glueCatalog.getTable(path);

        // alterTable(getTable(a)) must leave a unchanged.
        glueCatalog.alterTable(
                path, new ResolvedCatalogTable((CatalogTable) before, resolvedSchema), false);
        CatalogBaseTable after = glueCatalog.getTable(path);
        assertThat(after.getOptions()).isEqualTo(before.getOptions());
        assertThat(after.getComment()).isEqualTo(before.getComment());
        assertThat(after.getUnresolvedSchema().toString())
                .isEqualTo(before.getUnresolvedSchema().toString());

        // And a real change persists with the same fidelity as create.
        ResolvedSchema widened =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.INT().notNull()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3)),
                        Column.physical("amount", DataTypes.DECIMAL(10, 2)));
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.put("number-of-rows", "7");
        CatalogTable altered =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(widened).build())
                        .comment("after")
                        .options(newOptions)
                        .build();
        glueCatalog.alterTable(path, new ResolvedCatalogTable(altered, widened), false);
        CatalogBaseTable read = glueCatalog.getTable(path);
        assertThat(read.getOptions()).containsExactlyInAnyOrderEntriesOf(newOptions);
        assertThat(read.getComment()).isEqualTo("after");
        assertThat(read.getUnresolvedSchema().getColumns()).hasSize(3);
    }

    // ------------------------------------------------------------------------------------------
    // Glue storage metadata set out of band must not break queries
    // ------------------------------------------------------------------------------------------

    /**
     * Owner and input/output formats are Glue storage metadata that another engine or the console
     * can set on a table Flink created. They used to be surfaced as table options, which the
     * connector factory then rejected as unknown options at query time. Planning a query over such
     * a table must succeed and {@code SHOW CREATE TABLE} must show only the declared options.
     */
    @Test
    void testQueryPlansOverTableWhoseGlueMetadataWasSetOutOfBand() throws Exception {
        ObjectPath path = new ObjectPath(db, "owned");
        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.INT()),
                        Column.physical("name", DataTypes.STRING()));
        Map<String, String> options = new HashMap<>();
        options.put("connector", "datagen");
        options.put("number-of-rows", "5");
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .options(options)
                        .build();
        glueCatalog.createTable(path, new ResolvedCatalogTable(table, resolvedSchema), false);

        // Another engine / the console sets storage metadata on the same Glue table.
        Table stored =
                glueClient.getTable(b -> b.databaseName(db.toLowerCase()).name("owned")).table();
        StorageDescriptor sd =
                stored.storageDescriptor().toBuilder()
                        .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
                        .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                        .parameters(Collections.singletonMap("serialization.format", "1"))
                        .build();
        glueClient.updateTable(
                b ->
                        b.databaseName(db.toLowerCase())
                                .tableInput(
                                        TableInput.builder()
                                                .name(stored.name())
                                                .tableType(stored.tableType())
                                                .parameters(stored.parameters())
                                                .partitionKeys(stored.partitionKeys())
                                                .storageDescriptor(sd)
                                                .owner("data-platform-team")
                                                .description(stored.description())
                                                .build()));

        assertThat(glueCatalog.getTable(path).getOptions())
                .containsExactlyInAnyOrderEntriesOf(options);

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.registerCatalog("g", glueCatalog);
        assertThatCode(() -> tEnv.explainSql("SELECT * FROM g.`" + db + "`.owned"))
                .doesNotThrowAnyException();
        String showCreate =
                CollectionUtil.iteratorToList(
                                tEnv.executeSql("SHOW CREATE TABLE g.`" + db + "`.owned").collect())
                        .get(0)
                        .getField(0)
                        .toString();
        assertThat(showCreate)
                .contains("'connector' = 'datagen'")
                .doesNotContain("owner")
                .doesNotContain("table.input.format")
                .doesNotContain("serialization.format");
    }

    // ------------------------------------------------------------------------------------------
    // Glue-side state the catalog did not write must fail loudly, not partially
    // ------------------------------------------------------------------------------------------

    /**
     * A Glue partition whose value count does not match the table's partition keys (another engine
     * changed the keys, or the partition is corrupt) must not be truncated into a spec naming a
     * partition that does not exist.
     */
    @Test
    void testPartitionWithWrongValueCountFailsInsteadOfTruncating() throws Exception {
        assumeThat(glueClient)
                .as("real Glue rejects the mismatched partition itself")
                .isInstanceOf(FakeGlueClient.class);
        ObjectPath path = new ObjectPath(db, "ptn");
        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.INT()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("day", DataTypes.STRING()));
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .partitionKeys(Arrays.asList("region", "day"))
                        .options(Collections.emptyMap())
                        .build();
        glueCatalog.createTable(path, new ResolvedCatalogTable(table, resolvedSchema), false);
        glueCatalog.createPartition(
                path,
                new CatalogPartitionSpec(twoKeys("eu", "2026-01-01")),
                new org.apache.flink.table.catalog.CatalogPartitionImpl(new HashMap<>(), null),
                false);

        // Out of band: a partition with a single value on a two-key table.
        Table stored =
                glueClient.getTable(b -> b.databaseName(db.toLowerCase()).name("ptn")).table();
        glueClient.createPartition(
                b ->
                        b.databaseName(db.toLowerCase())
                                .tableName("ptn")
                                .partitionInput(
                                        PartitionInput.builder()
                                                .values(Collections.singletonList("us"))
                                                .storageDescriptor(
                                                        stored.storageDescriptor().toBuilder()
                                                                .location(
                                                                        stored.storageDescriptor()
                                                                                        .location()
                                                                                + "/region=us")
                                                                .build())
                                                .build()));

        assertThatThrownBy(() -> glueCatalog.listPartitions(path))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("[us]")
                .hasMessageContaining("1 value(s)")
                .hasMessageContaining("2 partition key(s)");
    }

    private static Map<String, String> twoKeys(String region, String day) {
        Map<String, String> spec = new HashMap<>();
        spec.put("region", region);
        spec.put("day", day);
        return spec;
    }
}
