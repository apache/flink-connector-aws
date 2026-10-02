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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that schema features which AWS Glue columns cannot represent - computed columns, metadata
 * columns, watermarks, and primary keys - survive a full create/read round-trip, and that views do
 * not leak their columns into Glue partition keys.
 */
@ExtendWith(RealGlueCleanupExtension.class)
class GlueCatalogSchemaFidelityTest {

    private GlueClient glueClient;
    private GlueCatalog glueCatalog;
    private String databaseName;
    private String glueDatabaseName;

    @BeforeEach
    void setUp() throws Exception {
        glueClient = GlueTestClientFactory.createClient();
        glueCatalog = new GlueCatalog("test_catalog", "default", "us-east-1", glueClient);
        databaseName = GlueTestClientFactory.uniqueName("fidelitydb");
        glueDatabaseName = databaseName.toLowerCase();
        glueCatalog.createDatabase(
                databaseName, new CatalogDatabaseImpl(new HashMap<>(), "fidelity tests"), false);
    }

    @AfterEach
    void tearDown() {
        if (glueCatalog != null) {
            glueCatalog.close();
        }
    }

    @Test
    void testWatermarkPrimaryKeyAndNonPhysicalColumnsRoundTrip() throws Exception {
        String tableName = GlueTestClientFactory.uniqueName("fidelitytable");
        ObjectPath path = new ObjectPath(databaseName, tableName);

        List<Column> columns =
                Arrays.asList(
                        Column.physical("userId", DataTypes.STRING().notNull()),
                        Column.physical("eventTime", DataTypes.TIMESTAMP(3)),
                        Column.physical("price", DataTypes.DOUBLE()),
                        Column.computed(
                                "doublePrice", sqlExpression("`price` * 2", DataTypes.DOUBLE())),
                        Column.metadata("kafkaOffset", DataTypes.BIGINT(), "offset", true));
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        columns,
                        Collections.singletonList(
                                WatermarkSpec.of(
                                        "eventTime",
                                        sqlExpression(
                                                "`eventTime` - INTERVAL '5' SECOND",
                                                DataTypes.TIMESTAMP(3)))),
                        UniqueConstraint.primaryKey(
                                "PK_userId", Collections.singletonList("userId")));

        Map<String, String> options = new HashMap<>();
        options.put("connector", "kinesis");
        options.put("stream.arn", "arn:aws:kinesis:us-east-1:000000000000:stream/fidelity");

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("schema fidelity round-trip")
                        .partitionKeys(Collections.emptyList())
                        .options(options)
                        .build();
        glueCatalog.createTable(
                path, new ResolvedCatalogTable(catalogTable, resolvedSchema), false);

        CatalogBaseTable readBack = glueCatalog.getTable(path);
        Schema schema = readBack.getUnresolvedSchema();

        // Column order preserved, including the interleaved non-physical columns.
        assertThat(schema.getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("userId", "eventTime", "price", "doublePrice", "kafkaOffset");

        // Computed and metadata columns restored with the right kinds.
        assertThat(schema.getColumns().get(3)).isInstanceOf(Schema.UnresolvedComputedColumn.class);
        assertThat(schema.getColumns().get(4)).isInstanceOf(Schema.UnresolvedMetadataColumn.class);
        Schema.UnresolvedMetadataColumn metadataColumn =
                (Schema.UnresolvedMetadataColumn) schema.getColumns().get(4);
        assertThat(metadataColumn.getMetadataKey()).isEqualTo("offset");
        assertThat(metadataColumn.isVirtual()).isTrue();

        // Watermark and primary key restored.
        assertThat(schema.getWatermarkSpecs()).hasSize(1);
        assertThat(schema.getWatermarkSpecs().get(0).getColumnName()).isEqualTo("eventTime");
        assertThat(schema.getPrimaryKey()).isPresent();
        assertThat(schema.getPrimaryKey().get().getColumnNames()).containsExactly("userId");

        // Internal flink.schema.* parameters must not leak into the exposed options.
        assertThat(readBack.getOptions().keySet())
                .noneMatch(key -> key.startsWith("flink.schema."));

        // The Glue table itself only carries the physical columns.
        Table glueTable = getRawGlueTable(tableName.toLowerCase());
        assertThat(glueTable.storageDescriptor().columns())
                .extracting(software.amazon.awssdk.services.glue.model.Column::name)
                .containsExactly("userid", "eventtime", "price");
    }

    @Test
    void testViewColumnsAreNotPersistedAsPartitionKeys() throws Exception {
        String viewName = GlueTestClientFactory.uniqueName("fidelityview");
        ObjectPath path = new ObjectPath(databaseName, viewName);

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("userId", DataTypes.STRING()),
                        Column.physical("total", DataTypes.DOUBLE()));
        CatalogView catalogView =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "a view",
                        "SELECT userId, total FROM src",
                        "SELECT userId, total FROM src",
                        Collections.emptyMap());
        glueCatalog.createTable(path, new ResolvedCatalogView(catalogView, resolvedSchema), false);

        // The stored Glue table must not have the view's columns duplicated as partition keys.
        Table glueTable = getRawGlueTable(viewName.toLowerCase());
        assertThat(glueTable.partitionKeys()).isEmpty();
        assertThat(glueTable.storageDescriptor().columns()).hasSize(2);

        // And the view must read back cleanly with its columns intact.
        CatalogBaseTable readBack = glueCatalog.getTable(path);
        assertThat(readBack.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.VIEW);
        assertThat(readBack.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("userId", "total");
    }

    private Table getRawGlueTable(String glueTableName) {
        return glueClient
                .getTable(
                        GetTableRequest.builder()
                                .databaseName(glueDatabaseName)
                                .name(glueTableName)
                                .build())
                .table();
    }

    /**
     * Regression test for the partition-key reordering bug: Glue returns physical columns as
     * storage-descriptor columns followed by partition keys, which differs from the declared order
     * whenever a partition key is not declared last. Per-column fidelity metadata must therefore be
     * keyed by name, not by declared position, or the {@code TIMESTAMP(3)} override intended for
     * {@code ts} would land on {@code id}.
     */
    @Test
    void testPartitionedTableDeclaredOrderAndTypesSurviveRoundTrip() throws Exception {
        String tableName = GlueTestClientFactory.uniqueName("partitionedfidelity");
        ObjectPath path = new ObjectPath(databaseName, tableName);

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3)),
                        Column.physical("id", DataTypes.INT().notNull()),
                        Column.physical("amount", DataTypes.DECIMAL(10, 2)),
                        Column.physical("createdAt", DataTypes.TIMESTAMP_LTZ(3)));
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("partition key declared first")
                        .partitionKeys(Collections.singletonList("region"))
                        .options(kinesisOptions("partitioned"))
                        .build();
        glueCatalog.createTable(
                path, new ResolvedCatalogTable(catalogTable, resolvedSchema), false);

        // Glue itself stores the partition key outside the storage descriptor.
        Table glueTable = getRawGlueTable(tableName.toLowerCase());
        assertThat(glueTable.partitionKeys())
                .extracting(software.amazon.awssdk.services.glue.model.Column::name)
                .containsExactly("region");
        assertThat(glueTable.storageDescriptor().columns())
                .extracting(software.amazon.awssdk.services.glue.model.Column::name)
                .containsExactly("ts", "id", "amount", "createdat");

        CatalogBaseTable readBack = glueCatalog.getTable(path);
        assertThat(((CatalogTable) readBack).getPartitionKeys()).containsExactly("region");
        Schema schema = readBack.getUnresolvedSchema();
        assertThat(schema.getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("region", "ts", "id", "amount", "createdAt");

        // Every column keeps exactly its declared type: precision, nullability and LTZ semantics.
        assertThat(physicalType(schema, 0)).isEqualTo(DataTypes.STRING());
        assertThat(physicalType(schema, 1)).isEqualTo(DataTypes.TIMESTAMP(3));
        assertThat(physicalType(schema, 2)).isEqualTo(DataTypes.INT().notNull());
        assertThat(physicalType(schema, 3)).isEqualTo(DataTypes.DECIMAL(10, 2));
        assertThat(physicalType(schema, 4)).isEqualTo(DataTypes.TIMESTAMP_LTZ(3));
    }

    @Test
    void testColumnCommentsSurviveRoundTrip() throws Exception {
        String tableName = GlueTestClientFactory.uniqueName("commentfidelity");
        ObjectPath path = new ObjectPath(databaseName, tableName);

        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("userId", DataTypes.STRING())
                                        .withComment("the user"),
                                Column.physical("price", DataTypes.DOUBLE()),
                                Column.computed(
                                                "doublePrice",
                                                sqlExpression("`price` * 2", DataTypes.DOUBLE()))
                                        .withComment("twice the price"),
                                Column.metadata("offset", DataTypes.BIGINT(), "offset", true)
                                        .withComment("record offset")),
                        Collections.emptyList(),
                        null);
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("column comments")
                        .partitionKeys(Collections.emptyList())
                        .options(kinesisOptions("comments"))
                        .build();
        glueCatalog.createTable(
                path, new ResolvedCatalogTable(catalogTable, resolvedSchema), false);

        // Physical column comments are stored natively on the Glue column.
        Table glueTable = getRawGlueTable(tableName.toLowerCase());
        assertThat(glueTable.storageDescriptor().columns().get(0).comment()).isEqualTo("the user");

        Schema schema = glueCatalog.getTable(path).getUnresolvedSchema();
        assertThat(schema.getColumns().get(0).getComment()).contains("the user");
        assertThat(schema.getColumns().get(1).getComment()).isEmpty();
        assertThat(schema.getColumns().get(2).getComment()).contains("twice the price");
        assertThat(schema.getColumns().get(3).getComment()).contains("record offset");
    }

    @Test
    void testViewColumnTypesSurviveRoundTrip() throws Exception {
        String viewName = GlueTestClientFactory.uniqueName("viewfidelity");
        ObjectPath path = new ObjectPath(databaseName, viewName);

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("eventTime", DataTypes.TIMESTAMP(3).notNull()),
                        Column.physical("total", DataTypes.DECIMAL(12, 4)));
        CatalogView catalogView =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "typed view",
                        "SELECT eventTime, total FROM src",
                        "SELECT eventTime, total FROM src",
                        Collections.emptyMap());
        glueCatalog.createTable(path, new ResolvedCatalogView(catalogView, resolvedSchema), false);

        CatalogBaseTable readBack = glueCatalog.getTable(path);
        assertThat(readBack.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.VIEW);
        Schema schema = readBack.getUnresolvedSchema();
        assertThat(physicalType(schema, 0)).isEqualTo(DataTypes.TIMESTAMP(3).notNull());
        assertThat(physicalType(schema, 1)).isEqualTo(DataTypes.DECIMAL(12, 4));
        assertThat(readBack.getOptions().keySet())
                .noneMatch(key -> key.startsWith("flink.schema."));
    }

    /**
     * Tables created by other engines (Athena, crawlers, Spark, Hive) carry Glue's own table types
     * and parameterised Hive type strings. They must be readable, not rejected.
     */
    @Test
    void testForeignExternalTableWithHiveTypesIsReadable() throws Exception {
        String glueTableName = GlueTestClientFactory.uniqueName("athenatable").toLowerCase();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(glueDatabaseName)
                        .tableInput(
                                TableInput.builder()
                                        .name(glueTableName)
                                        .tableType("EXTERNAL_TABLE")
                                        .parameters(
                                                Collections.singletonMap(
                                                        "classification", "parquet"))
                                        .partitionKeys(glueColumn("dt", "string"))
                                        .storageDescriptor(
                                                StorageDescriptor.builder()
                                                        .location("s3://bucket/athena/")
                                                        .columns(
                                                                glueColumn("name", "varchar(255)"),
                                                                glueColumn("code", "char(10)"),
                                                                glueColumn(
                                                                        "amount", "decimal(10,2)"),
                                                                glueColumn("qty", "int"),
                                                                glueColumn("tags", "array<string>"),
                                                                glueColumn(
                                                                        "attrs",
                                                                        "map<string,bigint>"))
                                                        .build())
                                        .build())
                        .build());

        ObjectPath path = new ObjectPath(databaseName, glueTableName);
        assertThat(glueCatalog.tableExists(path)).isTrue();
        CatalogBaseTable readBack = glueCatalog.getTable(path);
        assertThat(readBack.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.TABLE);
        assertThat(((CatalogTable) readBack).getPartitionKeys()).containsExactly("dt");

        Schema schema = readBack.getUnresolvedSchema();
        assertThat(schema.getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("name", "code", "amount", "qty", "tags", "attrs", "dt");
        assertThat(physicalType(schema, 0)).isEqualTo(DataTypes.VARCHAR(255));
        assertThat(physicalType(schema, 1)).isEqualTo(DataTypes.CHAR(10));
        assertThat(physicalType(schema, 2)).isEqualTo(DataTypes.DECIMAL(10, 2));
        assertThat(physicalType(schema, 3)).isEqualTo(DataTypes.INT());
        assertThat(physicalType(schema, 4)).isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(physicalType(schema, 5))
                .isEqualTo(DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()));
        // Foreign options are exposed; the planner reports the missing connector, not the catalog.
        assertThat(readBack.getOptions()).containsEntry("classification", "parquet");
    }

    /**
     * Some crawlers and Spark versions list the partition column in the storage descriptor as well
     * as in {@code partitionKeys}. The Flink schema must contain it once, as a partition column.
     */
    @Test
    void testForeignTableWithPartitionColumnDuplicatedInStorageDescriptor() throws Exception {
        String glueTableName = GlueTestClientFactory.uniqueName("duptable").toLowerCase();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(glueDatabaseName)
                        .tableInput(
                                TableInput.builder()
                                        .name(glueTableName)
                                        .tableType("EXTERNAL_TABLE")
                                        .partitionKeys(glueColumn("dt", "string"))
                                        .storageDescriptor(
                                                StorageDescriptor.builder()
                                                        .location("s3://bucket/dup/")
                                                        .columns(
                                                                glueColumn("id", "bigint"),
                                                                glueColumn("dt", "string"))
                                                        .build())
                                        .build())
                        .build());

        CatalogBaseTable readBack =
                glueCatalog.getTable(new ObjectPath(databaseName, glueTableName));
        Schema schema = readBack.getUnresolvedSchema();
        assertThat(schema.getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("id", "dt");
        assertThat(((CatalogTable) readBack).getPartitionKeys()).containsExactly("dt");
    }

    /**
     * Glue objects without a storage descriptor exist (some governed tables and virtual views).
     * Reading one must yield an empty physical schema, not a NullPointerException.
     */
    @Test
    void testForeignTableWithoutStorageDescriptorIsReadable() throws Exception {
        String glueTableName = GlueTestClientFactory.uniqueName("nosdtable").toLowerCase();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(glueDatabaseName)
                        .tableInput(
                                TableInput.builder()
                                        .name(glueTableName)
                                        .tableType("EXTERNAL_TABLE")
                                        .parameters(Collections.singletonMap("k", "v"))
                                        .build())
                        .build());

        ObjectPath path = new ObjectPath(databaseName, glueTableName);
        CatalogBaseTable readBack = glueCatalog.getTable(path);
        assertThat(readBack.getUnresolvedSchema().getColumns()).isEmpty();
        assertThat(readBack.getOptions()).containsEntry("k", "v");
        assertThat(glueCatalog.listTables(databaseName)).contains(glueTableName);
    }

    @Test
    void testForeignVirtualViewIsReadAsView() throws Exception {
        String glueViewName = GlueTestClientFactory.uniqueName("athenaview").toLowerCase();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(glueDatabaseName)
                        .tableInput(
                                TableInput.builder()
                                        .name(glueViewName)
                                        .tableType("VIRTUAL_VIEW")
                                        .viewOriginalText("SELECT 1 AS one")
                                        .storageDescriptor(
                                                StorageDescriptor.builder()
                                                        .columns(glueColumn("one", "int"))
                                                        .build())
                                        .build())
                        .build());

        CatalogBaseTable readBack =
                glueCatalog.getTable(new ObjectPath(databaseName, glueViewName));
        assertThat(readBack.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.VIEW);
        assertThat(((CatalogView) readBack).getOriginalQuery()).isEqualTo("SELECT 1 AS one");
        assertThat(((CatalogView) readBack).getExpandedQuery()).isEqualTo("SELECT 1 AS one");
    }

    @Test
    void testAlterTableRejectsPartitionKeyChange() throws Exception {
        String tableName = GlueTestClientFactory.uniqueName("alterpartitioned");
        ObjectPath path = new ObjectPath(databaseName, tableName);
        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("id", DataTypes.INT()));
        ResolvedCatalogTable original =
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(resolvedSchema)
                                                .build())
                                .partitionKeys(Collections.singletonList("region"))
                                .options(kinesisOptions("alter"))
                                .build(),
                        resolvedSchema);
        glueCatalog.createTable(path, original, false);

        ResolvedCatalogTable repartitioned =
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(resolvedSchema)
                                                .build())
                                .partitionKeys(Collections.emptyList())
                                .options(kinesisOptions("alter"))
                                .build(),
                        resolvedSchema);
        assertThatThrownBy(() -> glueCatalog.alterTable(path, repartitioned, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("changing the partition keys")
                .hasMessageContaining("[region] -> []");

        // The table is untouched.
        assertThat(((CatalogTable) glueCatalog.getTable(path)).getPartitionKeys())
                .containsExactly("region");
    }

    @Test
    void testAlterTableRefusesToOverwriteView() throws Exception {
        String viewName = GlueTestClientFactory.uniqueName("alterview");
        ObjectPath path = new ObjectPath(databaseName, viewName);
        ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical("one", DataTypes.INT()));
        glueCatalog.createTable(
                path,
                new ResolvedCatalogView(
                        CatalogView.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                null,
                                "SELECT 1",
                                "SELECT 1",
                                Collections.emptyMap()),
                        resolvedSchema),
                false);

        ResolvedCatalogTable asTable =
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(resolvedSchema)
                                                .build())
                                .partitionKeys(Collections.emptyList())
                                .options(kinesisOptions("alter"))
                                .build(),
                        resolvedSchema);
        assertThatThrownBy(() -> glueCatalog.alterTable(path, asTable, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("the Glue object is a VIEW");

        // The view's query text survives the refused alter.
        assertThat(((CatalogView) glueCatalog.getTable(path)).getOriginalQuery())
                .isEqualTo("SELECT 1");
    }

    @Test
    void testReservedOptionsAreRejected() {
        ObjectPath path =
                new ObjectPath(databaseName, GlueTestClientFactory.uniqueName("reserved"));
        ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical("id", DataTypes.INT()));

        for (String reserved :
                Arrays.asList(
                        "flink.schema.column-order",
                        "flink.schema.watermark.0.rowtime",
                        "flink.original-table-name")) {
            Map<String, String> options = kinesisOptions("reserved");
            options.put(reserved, "x");
            ResolvedCatalogTable table =
                    new ResolvedCatalogTable(
                            CatalogTable.newBuilder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .fromResolvedSchema(resolvedSchema)
                                                    .build())
                                    .partitionKeys(Collections.emptyList())
                                    .options(options)
                                    .build(),
                            resolvedSchema);
            assertThatThrownBy(() -> glueCatalog.createTable(path, table, false))
                    .isInstanceOf(CatalogException.class)
                    .hasMessageContaining("'" + reserved + "' is reserved");
        }
    }

    @Test
    void testUnresolvedTableIsRejectedWithClearMessage() {
        ObjectPath path =
                new ObjectPath(databaseName, GlueTestClientFactory.uniqueName("unresolved"));
        CatalogTable unresolved =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .partitionKeys(Collections.emptyList())
                        .options(kinesisOptions("unresolved"))
                        .build();
        assertThatThrownBy(() -> glueCatalog.createTable(path, unresolved, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("must be resolved");
    }

    /**
     * Resolves the physical column at {@code index} to a concrete {@link DataType}. Types restored
     * from fidelity metadata are {@link UnresolvedDataType}s (the planner resolves them), so they
     * are resolved here through a real planner type factory.
     */
    private static DataType physicalType(Schema schema, int index) {
        Schema.UnresolvedColumn column = schema.getColumns().get(index);
        assertThat(column).isInstanceOf(Schema.UnresolvedPhysicalColumn.class);
        AbstractDataType<?> dataType = ((Schema.UnresolvedPhysicalColumn) column).getDataType();
        if (dataType instanceof DataType) {
            return (DataType) dataType;
        }
        return ((UnresolvedDataType) dataType).toDataType(TYPE_FACTORY);
    }

    private static final DataTypeFactory TYPE_FACTORY =
            ((TableEnvironmentInternal)
                            TableEnvironment.create(EnvironmentSettings.inStreamingMode()))
                    .getCatalogManager()
                    .getDataTypeFactory();

    private static Map<String, String> kinesisOptions(String stream) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "kinesis");
        options.put("stream.arn", "arn:aws:kinesis:us-east-1:000000000000:stream/" + stream);
        return options;
    }

    private static software.amazon.awssdk.services.glue.model.Column glueColumn(
            String name, String type) {
        return software.amazon.awssdk.services.glue.model.Column.builder()
                .name(name)
                .type(type)
                .build();
    }

    /**
     * Returns a minimal {@link ResolvedExpression} whose serializable form is the given SQL string,
     * mirroring what the planner produces when resolving DDL.
     */
    private static ResolvedExpression sqlExpression(String sql, DataType outputDataType) {
        return new ResolvedExpression() {
            @Override
            public DataType getOutputDataType() {
                return outputDataType;
            }

            @Override
            public List<ResolvedExpression> getResolvedChildren() {
                return Collections.emptyList();
            }

            @Override
            public String asSerializableString() {
                return sql;
            }

            @Override
            public String asSummaryString() {
                return sql;
            }

            @Override
            public List<org.apache.flink.table.expressions.Expression> getChildren() {
                return Collections.emptyList();
            }

            @Override
            public <R> R accept(ExpressionVisitor<R> visitor) {
                return visitor.visit(this);
            }
        };
    }
}
