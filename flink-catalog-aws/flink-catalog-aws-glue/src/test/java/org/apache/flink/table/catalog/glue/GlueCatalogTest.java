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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
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
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.operator.GlueDatabaseOperator;
import org.apache.flink.table.catalog.glue.operator.GlueFunctionOperator;
import org.apache.flink.table.catalog.glue.operator.GluePartitionOperator;
import org.apache.flink.table.catalog.glue.operator.GlueTableOperator;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.COLUMN_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.COLUMN_2;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.COMMENT;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.DATABASE_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.DATABASE_2;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.DATABASE_DESCRIPTION;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.FUNCTION_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_2;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_3;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_4;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_5;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.VIEW_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.VIEW_2;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getDatabaseParams;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getDummyCatalogDatabase;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getDummyCatalogTable;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getDummyCatalogTableWithPartition;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getDummyTableParams;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getPartitionSpecParams;

class GlueCatalogTest {

    public static final String WAREHOUSE_PATH = "s3://bucket";
    private static final String CATALOG_NAME = "glue";
    private static DummyGlueClient glue;
    private static GlueCatalog glueCatalog;

    @BeforeAll
    static void setUp() {
        glue = new DummyGlueClient();
        String glueCatalogId = "dummy-catalog-Id";
        GlueDatabaseOperator glueDatabaseOperator =
                new GlueDatabaseOperator(CATALOG_NAME, glue, glueCatalogId);
        GlueTableOperator glueTableOperator =
                new GlueTableOperator(CATALOG_NAME, glue, glueCatalogId);
        GluePartitionOperator gluePartitionOperator =
                new GluePartitionOperator(CATALOG_NAME, glue, glueCatalogId);
        GlueFunctionOperator glueFunctionOperator =
                new GlueFunctionOperator(CATALOG_NAME, glue, glueCatalogId);
        glueCatalog =
                new GlueCatalog(
                        CATALOG_NAME,
                        GlueCatalog.DEFAULT_DB,
                        glue,
                        glueDatabaseOperator,
                        glueTableOperator,
                        gluePartitionOperator,
                        glueFunctionOperator);
    }

    @BeforeEach
    public void clear() {
        glue.setDatabaseMap(new HashMap<>());
        glue.setTableMap(new HashMap<>());
        glue.setPartitionMap(new HashMap<>());
        glue.setUserDefinedFunctionMap(new HashMap<>());
    }

    // ------ Database
    @Test
    void testCreateDatabase() throws DatabaseNotExistException {

        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueCatalog.getDatabase(DATABASE_1));
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(getDatabaseParams(), DATABASE_DESCRIPTION);
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false));
        CatalogDatabase database = glueCatalog.getDatabase(DATABASE_1);
        Assertions.assertNotNull(database);
        Assertions.assertNotNull(database.getProperties());
        Assertions.assertNotNull(database.getComment());
        Assertions.assertEquals(DATABASE_DESCRIPTION, database.getComment());
        Assertions.assertThrows(
                DatabaseAlreadyExistException.class,
                () -> glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false));
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(DATABASE_2, catalogDatabase, true));
    }

    @Test
    void testAlterDatabase() throws DatabaseNotExistException, DatabaseAlreadyExistException {
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueCatalog.getDatabase(DATABASE_1));
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(getDatabaseParams(), DATABASE_DESCRIPTION);
        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        CatalogDatabase database = glueCatalog.getDatabase(DATABASE_1);
        Assertions.assertNotNull(database);
        Assertions.assertNotNull(database.getProperties());
        Assertions.assertNotNull(database.getComment());
        Assertions.assertEquals(DATABASE_DESCRIPTION, database.getComment());

        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> glueCatalog.alterDatabase(DATABASE_2, database, false));
        Assertions.assertDoesNotThrow(() -> glueCatalog.alterDatabase(DATABASE_2, database, true));

        Map<String, String> properties = catalogDatabase.getProperties();
        properties.put("newKey", "val");
        CatalogDatabase newCatalogDatabase = catalogDatabase.copy(properties);
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.alterDatabase(DATABASE_1, newCatalogDatabase, false));
        CatalogDatabase database1 = glueCatalog.getDatabase(DATABASE_1);
        Assertions.assertNotNull(database1);
        Assertions.assertNotNull(database1.getProperties());
        Assertions.assertEquals(database1.getProperties(), properties);
        Assertions.assertNotNull(database1.getComment());
        Assertions.assertEquals(DATABASE_DESCRIPTION, database1.getComment());
    }

    @Test
    void testDatabaseExists()
            throws DatabaseAlreadyExistException, DatabaseNotEmptyException,
                    DatabaseNotExistException {
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_1));
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(getDatabaseParams(), DATABASE_DESCRIPTION);
        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertTrue(glueCatalog.databaseExists(DATABASE_1));
        glueCatalog.dropDatabase(DATABASE_1, true, true);
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_1));

        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertTrue(glueCatalog.databaseExists(DATABASE_1));
        glueCatalog.dropDatabase(DATABASE_1, false, false);
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_1));

        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertTrue(glueCatalog.databaseExists(DATABASE_1));
        glueCatalog.dropDatabase(DATABASE_1, true, false);
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_1));

        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertTrue(glueCatalog.databaseExists(DATABASE_1));
        glueCatalog.dropDatabase(DATABASE_1, false, true);
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_1));
    }

    @Test
    void testDropDatabase() throws DatabaseAlreadyExistException {

        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_1, true, false));

        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> glueCatalog.dropDatabase(DATABASE_2, false, true));

        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> glueCatalog.dropDatabase(DATABASE_2, false, false));

        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(getDatabaseParams(), DATABASE_DESCRIPTION);
        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_1, true, true));
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueCatalog.getDatabase(DATABASE_1));
        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_1, false, false));
        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_1, false, true));
        glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false);
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_1, true, false));
    }

    @Test
    void testListDatabases() {
        Assertions.assertEquals(new ArrayList<>(), glueCatalog.listDatabases());
        List<String> expectedDatabasesList = Arrays.asList(DATABASE_1, DATABASE_2);
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(getDatabaseParams(), DATABASE_DESCRIPTION);
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(DATABASE_1, catalogDatabase, false));
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(DATABASE_2, catalogDatabase, false));
        Assertions.assertEquals(expectedDatabasesList, glueCatalog.listDatabases());
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_1, false, false));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_2, false, false));
    }

    @Test
    void testGetDatabase() throws DatabaseNotExistException {

        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_1));
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_2));
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueCatalog.getDatabase(DATABASE_1));
        createDatabase(DATABASE_1);
        CatalogDatabase db = glueCatalog.getDatabase(DATABASE_1);
        Assertions.assertEquals(getDummyCatalogDatabase().getComment(), db.getComment());
        Assertions.assertEquals(getDatabaseParams(), db.getProperties());
    }

    @Test
    void testIsDatabaseEmpty()
            throws TableAlreadyExistException, DatabaseNotExistException,
                    FunctionAlreadyExistException {
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(DATABASE_1, getDummyCatalogDatabase(), false));
        Assertions.assertTrue(glueCatalog.isDatabaseEmpty(DATABASE_1));

        // create a table for the database
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        glueCatalog.createTable(tablePath, GlueCatalogTestUtils.getDummyCatalogTable(), false);
        Assertions.assertFalse(glueCatalog.isDatabaseEmpty(DATABASE_1));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropTable(tablePath, false));
        Assertions.assertTrue(glueCatalog.isDatabaseEmpty(DATABASE_1));

        // create userDefinedFunctions for the database
        ObjectPath functionPath = new ObjectPath(DATABASE_1, FUNCTION_1);
        Assertions.assertDoesNotThrow(
                () ->
                        glueCatalog.createFunction(
                                functionPath,
                                GlueCatalogTestUtils.getDummyCatalogFunction(),
                                false));
        Assertions.assertFalse(glueCatalog.isDatabaseEmpty(DATABASE_1));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropFunction(functionPath, false));
        Assertions.assertTrue(glueCatalog.isDatabaseEmpty(DATABASE_1));

        // both table and userDefinedFunction are present
        glueCatalog.createTable(tablePath, GlueCatalogTestUtils.getDummyCatalogTable(), false);
        glueCatalog.createFunction(
                functionPath, GlueCatalogTestUtils.getDummyCatalogFunction(), false);
        Assertions.assertFalse(glueCatalog.isDatabaseEmpty(DATABASE_1));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropTable(tablePath, false));
        Assertions.assertFalse(glueCatalog.isDatabaseEmpty(DATABASE_1));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropFunction(functionPath, false));
        Assertions.assertTrue(glueCatalog.isDatabaseEmpty(DATABASE_1));
    }

    // ------ Table
    @Test
    public void testCreateTable() throws TableNotExistException {

        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> glueCatalog.createTable(tablePath, getDummyCatalogTable(), false));
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(DATABASE_1, getDummyCatalogDatabase(), false));
        Assertions.assertDoesNotThrow(
                () ->
                        glueCatalog.createTable(
                                tablePath, GlueCatalogTestUtils.getDummyCatalogTable(), false));
        CatalogBaseTable table = glueCatalog.getTable(tablePath);
        Assertions.assertEquals(
                table.getUnresolvedSchema().getColumns().size(),
                getDummyCatalogTable().getUnresolvedSchema().getColumns().size());
        Assertions.assertEquals(table.getTableKind(), getDummyCatalogTable().getTableKind());
        ObjectPath tablePath2 = new ObjectPath(DATABASE_1, TABLE_2);
        CatalogBaseTable catalogBaseTable = getDummyCatalogTableWithPartition();
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createTable(tablePath2, catalogBaseTable, false));
        table = glueCatalog.getTable(tablePath2);
        Assertions.assertEquals(
                table.getUnresolvedSchema().getColumns().get(0).getName(),
                catalogBaseTable.getUnresolvedSchema().getColumns().get(0).getName());
        Assertions.assertEquals(
                table.getUnresolvedSchema().getColumns().get(1).getName(),
                catalogBaseTable.getUnresolvedSchema().getColumns().get(1).getName());
        Assertions.assertEquals(table.getTableKind(), catalogBaseTable.getTableKind());
    }

    @Test
    public void testCreateView() throws TableNotExistException {
        ObjectPath viewPath = new ObjectPath(DATABASE_1, VIEW_1);
        Assertions.assertThrows(TableNotExistException.class, () -> glueCatalog.getTable(viewPath));
        createDatabase(viewPath.getDatabaseName());
        createView(viewPath);
        CatalogBaseTable view = glueCatalog.getTable(viewPath);
        Assertions.assertNotNull(view);
        Assertions.assertEquals(getDummyTableParams(), view.getOptions());
        Assertions.assertEquals(CatalogTable.TableKind.VIEW.name(), view.getTableKind().name());
        ObjectPath tablePath = new ObjectPath(DATABASE_2, TABLE_2);
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        CatalogBaseTable table = glueCatalog.getTable(tablePath);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(getDummyTableParams(), table.getOptions());
        Assertions.assertEquals(CatalogTable.TableKind.TABLE.name(), table.getTableKind().name());
    }

    @Test
    public void testGetTable() throws TableNotExistException {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> glueCatalog.getTable(new ObjectPath(null, null)));
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        Assertions.assertThrows(
                TableNotExistException.class, () -> glueCatalog.getTable(tablePath));
        createDatabase(tablePath.getDatabaseName());
        Assertions.assertThrows(
                TableNotExistException.class, () -> glueCatalog.getTable(tablePath));
        createTable(tablePath);
        CatalogBaseTable table = glueCatalog.getTable(tablePath);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(
                CatalogBaseTable.TableKind.TABLE.name(), table.getTableKind().name());
        Assertions.assertEquals(getDummyTableParams(), table.getOptions());
    }

    @Test
    public void testGetView() throws TableNotExistException {

        ObjectPath viewPath = new ObjectPath(DATABASE_1, VIEW_1);
        createDatabase(viewPath.getDatabaseName());
        createView(viewPath);
        CatalogBaseTable view = glueCatalog.getTable(viewPath);
        Assertions.assertNotNull(view);
        Assertions.assertEquals(CatalogBaseTable.TableKind.VIEW.name(), view.getTableKind().name());
        Assertions.assertEquals(getDummyTableParams(), view.getOptions());
    }

    @Test
    public void testTableExists() {

        ObjectPath tablePath = new ObjectPath(DATABASE_2, TABLE_1);
        Assertions.assertThrows(NullPointerException.class, () -> glueCatalog.getTable(null));
        Assertions.assertThrows(
                TableNotExistException.class, () -> glueCatalog.getTable(tablePath));
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        Assertions.assertDoesNotThrow(() -> glueCatalog.getTable(tablePath));
        Assertions.assertThrows(
                TableNotExistException.class,
                () -> glueCatalog.getTable(new ObjectPath(DATABASE_2, TABLE_2)));
        Assertions.assertThrows(
                TableNotExistException.class,
                () -> glueCatalog.getTable(new ObjectPath(DATABASE_1, TABLE_2)));
        Assertions.assertTrue(glueCatalog.tableExists(tablePath));
        Assertions.assertFalse(glueCatalog.tableExists(new ObjectPath(DATABASE_1, TABLE_1)));
    }

    @Test
    public void testListTables() throws DatabaseNotExistException {
        createDatabase(DATABASE_1);
        createTable(new ObjectPath(DATABASE_1, TABLE_1));
        createTable(new ObjectPath(DATABASE_1, TABLE_2));
        createTable(new ObjectPath(DATABASE_1, TABLE_3));
        createTable(new ObjectPath(DATABASE_1, TABLE_4));
        createTable(new ObjectPath(DATABASE_1, TABLE_5));
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueCatalog.listTables(DATABASE_2));
        Assertions.assertEquals(5, glueCatalog.listTables(DATABASE_1).size());
        Assertions.assertEquals(
                Arrays.asList(TABLE_1, TABLE_2, TABLE_3, TABLE_4, TABLE_5),
                glueCatalog.listTables(DATABASE_1));
        createView(new ObjectPath(DATABASE_1, VIEW_1));
        Assertions.assertEquals(6, glueCatalog.listTables(DATABASE_1).size());
        Assertions.assertEquals(
                Arrays.asList(TABLE_1, TABLE_2, TABLE_3, TABLE_4, TABLE_5, VIEW_1),
                glueCatalog.listTables(DATABASE_1));
    }

    @Test
    public void testListTablesWithCombinationOfDifferentTableKind()
            throws DatabaseNotExistException {
        createDatabase(DATABASE_1);
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueCatalog.listTables(DATABASE_2));
        Assertions.assertDoesNotThrow(() -> glueCatalog.listTables(DATABASE_1));
        createTable(new ObjectPath(DATABASE_1, TABLE_1));
        createTable(new ObjectPath(DATABASE_1, TABLE_2));
        createTable(new ObjectPath(DATABASE_1, TABLE_3));
        createView(new ObjectPath(DATABASE_1, VIEW_2));
        createTable(new ObjectPath(DATABASE_1, TABLE_4));
        createTable(new ObjectPath(DATABASE_1, TABLE_5));
        createView(new ObjectPath(DATABASE_1, VIEW_1));
        Assertions.assertEquals(7, glueCatalog.listTables(DATABASE_1).size());
        Assertions.assertEquals(
                Arrays.asList(TABLE_1, TABLE_2, TABLE_3, TABLE_4, TABLE_5, VIEW_1, VIEW_2),
                glueCatalog.listTables(DATABASE_1));
    }

    @Test
    public void testListView() throws DatabaseNotExistException {
        createDatabase(DATABASE_1);
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueCatalog.listTables(DATABASE_2));
        Assertions.assertDoesNotThrow(() -> glueCatalog.listTables(DATABASE_1));
        createTable(new ObjectPath(DATABASE_1, TABLE_1));
        createTable(new ObjectPath(DATABASE_1, TABLE_2));
        createTable(new ObjectPath(DATABASE_1, TABLE_3));
        createView(new ObjectPath(DATABASE_1, VIEW_2));
        createTable(new ObjectPath(DATABASE_1, TABLE_4));
        createTable(new ObjectPath(DATABASE_1, TABLE_5));
        createView(new ObjectPath(DATABASE_1, VIEW_1));
        Assertions.assertEquals(2, glueCatalog.listViews(DATABASE_1).size());
        Assertions.assertNotSame(
                Arrays.asList(TABLE_1, TABLE_2, TABLE_3, TABLE_4, TABLE_5, VIEW_1, VIEW_2),
                glueCatalog.listViews(DATABASE_1));
        Assertions.assertEquals(Arrays.asList(VIEW_1, VIEW_2), glueCatalog.listViews(DATABASE_1));
        Assertions.assertNotSame(
                Arrays.asList(TABLE_1, TABLE_2, VIEW_1, VIEW_2),
                glueCatalog.listViews(DATABASE_1),
                "Should not contain any identifier of type table");
    }

    @Test
    public void testAlterTable() throws TableNotExistException {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        createDatabase(tablePath.getDatabaseName());
        Assertions.assertThrows(
                NullPointerException.class, () -> glueCatalog.alterTable(tablePath, null, false));
        createTable(tablePath);
        Assertions.assertDoesNotThrow(() -> glueCatalog.getTable(tablePath));
        CatalogBaseTable table = glueCatalog.getTable(tablePath);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(
                table.getTableKind().name(), CatalogBaseTable.TableKind.TABLE.name());
        Assertions.assertEquals(table.getOptions(), getDummyTableParams());
        Assertions.assertNotNull(table.getUnresolvedSchema());
        Map<String, String> modifiedOptions = table.getOptions();
        modifiedOptions.put("newKey", "newValue");
        Schema schema = table.getUnresolvedSchema();
        Assertions.assertNotNull(schema);

        Schema modifiedSchema =
                Schema.newBuilder().fromSchema(schema).column("col3", DataTypes.STRING()).build();
        List<DataType> dataTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());
        ResolvedSchema resolvedSchema =
                ResolvedSchema.physical(
                        modifiedSchema.getColumns().stream()
                                .map(Schema.UnresolvedColumn::getName)
                                .collect(Collectors.toList()),
                        dataTypes);
        ResolvedCatalogTable table1 =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                modifiedSchema,
                                "Changed Comment",
                                new ArrayList<>(),
                                modifiedOptions),
                        resolvedSchema);
        Assertions.assertDoesNotThrow(() -> glueCatalog.alterTable(tablePath, table1, false));
        CatalogBaseTable retrievedTable = glueCatalog.getTable(tablePath);
        Assertions.assertEquals(modifiedOptions, retrievedTable.getOptions());
        Assertions.assertEquals(
                modifiedSchema.getColumns().size(),
                retrievedTable.getUnresolvedSchema().getColumns().size());
    }

    @Test
    public void testDropTable() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_2);
        ObjectPath viewPath = new ObjectPath(DATABASE_1, VIEW_2);
        createDatabase(tablePath.getDatabaseName());
        Assertions.assertThrows(
                TableNotExistException.class, () -> glueCatalog.dropTable(tablePath, false));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropTable(tablePath, true));
        createTable(tablePath);
        createView(viewPath);
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropTable(tablePath, false));
        Assertions.assertThrows(
                TableNotExistException.class, () -> glueCatalog.getTable(tablePath));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropTable(tablePath, true));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropTable(viewPath, false));
        Assertions.assertThrows(TableNotExistException.class, () -> glueCatalog.getTable(viewPath));
    }

    @Test
    public void testRenameTable() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_2);
        ObjectPath viewPath = new ObjectPath(DATABASE_1, VIEW_2);
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        createView(viewPath);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> glueCatalog.renameTable(tablePath, TABLE_4, false));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> glueCatalog.renameTable(viewPath, VIEW_1, false));
    }

    // ------- Function
    @Test
    public void testCreateFunction() {
        ObjectPath functionPath = new ObjectPath(DATABASE_1, FUNCTION_1);
        createDatabase(functionPath.getDatabaseName());
        Assertions.assertFalse(glueCatalog.functionExists(functionPath));
        createFunction(functionPath, FunctionLanguage.JAVA, "TestClass");
        Assertions.assertTrue(glueCatalog.functionExists(functionPath));
    }

    @Test
    public void testNormalize() {
        ObjectPath functionPath = new ObjectPath(DATABASE_1, "Function-1");
        ObjectPath normalizeFunctionPath = glueCatalog.normalize(functionPath);
        Assertions.assertNotNull(normalizeFunctionPath);
        Assertions.assertEquals(DATABASE_1, normalizeFunctionPath.getDatabaseName());
        Assertions.assertEquals("function-1", normalizeFunctionPath.getObjectName());
    }

    @Test
    public void testAlterFunction() {
        ObjectPath functionPath = new ObjectPath(DATABASE_1, FUNCTION_1);
        createDatabase(functionPath.getDatabaseName());
        Assertions.assertFalse(glueCatalog.functionExists(functionPath));
        CatalogFunction catalogFunction =
                new CatalogFunctionImpl("ClassName", FunctionLanguage.JAVA);
        Assertions.assertThrows(
                FunctionNotExistException.class,
                () -> glueCatalog.alterFunction(functionPath, catalogFunction, true));

        createFunction(functionPath, FunctionLanguage.JAVA, "TestClass");
        Assertions.assertTrue(glueCatalog.functionExists(functionPath));
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.alterFunction(functionPath, catalogFunction, false));
    }

    @Test
    public void testDropFunction() {
        ObjectPath functionPath = new ObjectPath(DATABASE_1, FUNCTION_1);
        createDatabase(functionPath.getDatabaseName());
        Assertions.assertThrows(
                FunctionNotExistException.class,
                () -> glueCatalog.dropFunction(functionPath, false));
        createFunction(functionPath, FunctionLanguage.JAVA, "TestClass");
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropFunction(functionPath, false));
        Assertions.assertThrows(
                FunctionNotExistException.class,
                () -> glueCatalog.dropFunction(functionPath, false));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropFunction(functionPath, true));
    }

    @Test
    public void testListFunctions() throws DatabaseNotExistException {
        ObjectPath functionPath = new ObjectPath(DATABASE_1, FUNCTION_1);
        String className = GlueCatalogConstants.FLINK_SCALA_FUNCTION_PREFIX + "TestClass";
        createDatabase(DATABASE_1);
        createFunction(functionPath, FunctionLanguage.SCALA, className);
        Assertions.assertDoesNotThrow(() -> glueCatalog.listFunctions(DATABASE_1));
        List<String> udfList = glueCatalog.listFunctions(DATABASE_1);
        Assertions.assertNotNull(udfList);
        Assertions.assertEquals(1, udfList.size());
    }

    @Test
    public void testGetFunction() throws FunctionNotExistException {
        ObjectPath functionPath = new ObjectPath(DATABASE_1, FUNCTION_1);
        createDatabase(functionPath.getDatabaseName());
        String className = GlueCatalogConstants.FLINK_JAVA_FUNCTION_PREFIX + "TestClass";
        createFunction(functionPath, FunctionLanguage.JAVA, className);
        Assertions.assertThrows(
                ValidationException.class, () -> glueCatalog.getFunction(functionPath));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropFunction(functionPath, false));
        createFunction(functionPath, FunctionLanguage.JAVA, "TestClass");
        CatalogFunction catalogFunction = glueCatalog.getFunction(functionPath);
        Assertions.assertNotNull(catalogFunction);
        Assertions.assertEquals(FunctionLanguage.JAVA, catalogFunction.getFunctionLanguage());
        Assertions.assertEquals(3, catalogFunction.getFunctionResources().size());
        Assertions.assertEquals("TestClass", catalogFunction.getClassName());
    }

    @Test
    public void testFunctionExists() {
        ObjectPath functionPath = new ObjectPath(DATABASE_1, FUNCTION_1);
        Assertions.assertFalse(glueCatalog.functionExists(functionPath));
        createDatabase(functionPath.getDatabaseName());
        Assertions.assertFalse(glueCatalog.functionExists(functionPath));
        createFunction(functionPath, FunctionLanguage.JAVA, "TestClass");
        Assertions.assertTrue(glueCatalog.functionExists(functionPath));
    }

    // ------ Partition
    @Test
    public void testCreatePartition() throws PartitionNotExistException {

        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(getPartitionSpecParams());
        CatalogPartition catalogPartition =
                new CatalogPartitionImpl(GlueCatalogTestUtils.getCatalogPartitionParams(), COMMENT);
        Assertions.assertDoesNotThrow(
                () ->
                        glueCatalog.createPartition(
                                tablePath, partitionSpec, catalogPartition, false));

        CatalogPartition partition = glueCatalog.getPartition(tablePath, partitionSpec);
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(getPartitionSpecParams(), partition.getProperties());

        Assertions.assertThrows(
                NullPointerException.class,
                () -> glueCatalog.createPartition(null, partitionSpec, catalogPartition, false));

        Assertions.assertThrows(
                NullPointerException.class,
                () -> glueCatalog.createPartition(tablePath, null, catalogPartition, false));

        Assertions.assertThrows(
                NullPointerException.class,
                () -> glueCatalog.createPartition(tablePath, partitionSpec, null, false));

        Assertions.assertThrows(
                CatalogException.class,
                () ->
                        glueCatalog.getPartition(
                                tablePath, new CatalogPartitionSpec(new HashMap<>())));

        Assertions.assertThrows(
                NullPointerException.class,
                () -> glueCatalog.getPartition(tablePath, new CatalogPartitionSpec(null)));

        Assertions.assertThrows(
                CatalogException.class,
                () ->
                        glueCatalog.createPartition(
                                tablePath,
                                new CatalogPartitionSpec(new HashMap<>()),
                                catalogPartition,
                                false));

        Assertions.assertThrows(
                PartitionAlreadyExistsException.class,
                () ->
                        glueCatalog.createPartition(
                                tablePath,
                                partitionSpec,
                                new CatalogPartitionImpl(new HashMap<>(), COMMENT),
                                false));
    }

    @Test
    public void testListPartitions()
            throws TableNotPartitionedException, TableNotExistException,
                    PartitionSpecInvalidException {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_2);
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        Assertions.assertEquals(
                0,
                glueCatalog
                        .listPartitions(tablePath, new CatalogPartitionSpec(new HashMap<>()))
                        .size());
        createPartition(tablePath);
        Assertions.assertEquals(
                1,
                glueCatalog
                        .listPartitions(tablePath, new CatalogPartitionSpec(new HashMap<>()))
                        .size());
        Map<String, String> partSpec = new HashMap<>();
        partSpec.put(COLUMN_1, "v1");
        partSpec.put(COLUMN_2, "v2");
        Assertions.assertEquals(
                new CatalogPartitionSpec(partSpec),
                glueCatalog
                        .listPartitions(
                                tablePath, new CatalogPartitionSpec(getPartitionSpecParams()))
                        .get(0));
    }

    @Test
    public void testIsPartitionedTable() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        createDatabase(tablePath.getDatabaseName());
        createNonPartitionedTable(tablePath);
        Assertions.assertFalse(glueCatalog.isPartitionedTable(tablePath));
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropTable(tablePath, false));
        createTable(tablePath);
        createPartition(tablePath);
        Assertions.assertTrue(glueCatalog.isPartitionedTable(tablePath));
    }

    @Test
    public void testListPartitionsByFilter()
            throws TableNotPartitionedException, TableNotExistException {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        createPartition(tablePath);
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(getPartitionSpecParams());
        Assertions.assertDoesNotThrow(() -> glueCatalog.getPartition(tablePath, partitionSpec));
        List<Expression> expressions = new ArrayList<>();
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.listPartitionsByFilter(tablePath, expressions));
        List<CatalogPartitionSpec> partitionSpecs =
                glueCatalog.listPartitionsByFilter(tablePath, expressions);
        Assertions.assertNotNull(partitionSpecs);
        Assertions.assertEquals(1, partitionSpecs.size());
        Assertions.assertEquals(getPartitionSpecParams(), partitionSpecs.get(0).getPartitionSpec());
    }

    @Test
    public void testDropPartition() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(getPartitionSpecParams());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueCatalog.dropPartition(tablePath, partitionSpec, true));
        createDatabase(tablePath.getDatabaseName());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueCatalog.dropPartition(tablePath, partitionSpec, true));
        createTable(tablePath);
        createPartition(tablePath);
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.dropPartition(tablePath, partitionSpec, true));
        Assertions.assertThrows(
                CatalogException.class,
                () ->
                        glueCatalog.dropPartition(
                                tablePath, new CatalogPartitionSpec(new HashMap<>()), true));
    }

    @Test
    public void testAlterPartition() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        createPartition(tablePath);
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(getPartitionSpecParams());
        CatalogPartition newPartition = new CatalogPartitionImpl(getPartitionSpecParams(), COMMENT);

        Assertions.assertDoesNotThrow(
                () -> glueCatalog.alterPartition(tablePath, partitionSpec, newPartition, false));

        Assertions.assertDoesNotThrow(
                () -> glueCatalog.alterPartition(tablePath, partitionSpec, newPartition, true));

        Map<String, String> partitionSpecProperties = getPartitionSpecParams();
        partitionSpecProperties.put("test", "v3");

        Assertions.assertThrows(
                CatalogException.class,
                () ->
                        glueCatalog.alterPartition(
                                tablePath,
                                new CatalogPartitionSpec(partitionSpecProperties),
                                newPartition,
                                false));

        ObjectPath tablePath1 = new ObjectPath(DATABASE_1, TABLE_2);
        createNonPartitionedTable(tablePath1);
        // since table is not partition , test should throw Catalog Exception

        Assertions.assertThrows(
                CatalogException.class,
                () ->
                        glueCatalog.alterPartition(
                                tablePath,
                                new CatalogPartitionSpec(new HashMap<>()),
                                new CatalogPartitionImpl(new HashMap<>(), COMMENT),
                                false));

        Assertions.assertThrows(
                NullPointerException.class,
                () ->
                        glueCatalog.alterPartition(
                                tablePath,
                                new CatalogPartitionSpec(null),
                                new CatalogPartitionImpl(new HashMap<>(), COMMENT),
                                false));
        Assertions.assertThrows(
                NullPointerException.class,
                () ->
                        glueCatalog.alterPartition(
                                tablePath,
                                new CatalogPartitionSpec(new HashMap<>()),
                                new CatalogPartitionImpl(null, COMMENT),
                                false));
    }

    @Test
    public void testGetPartition() throws PartitionNotExistException {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        createPartition(tablePath);
        CatalogPartitionSpec partitionSpec =
                new CatalogPartitionSpec(GlueCatalogTestUtils.getPartitionSpecParams());
        CatalogPartition catalogPartition =
                new CatalogPartitionImpl(GlueCatalogTestUtils.getCatalogPartitionParams(), COMMENT);
        Assertions.assertNotNull(catalogPartition);

        Assertions.assertDoesNotThrow(() -> glueCatalog.getPartition(tablePath, partitionSpec));
        CatalogPartition partition = glueCatalog.getPartition(tablePath, partitionSpec);
        Assertions.assertNotNull(partition);
        Assertions.assertNull(partition.getComment());
        Assertions.assertEquals(
                GlueCatalogTestUtils.getPartitionSpecParams(), partition.getProperties());
    }

    @Test
    public void testPartitionExists() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(getPartitionSpecParams());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueCatalog.partitionExists(tablePath, partitionSpec));
        createDatabase(tablePath.getDatabaseName());
        createTable(tablePath);
        Assertions.assertFalse(glueCatalog.partitionExists(tablePath, partitionSpec));
        createPartition(tablePath);
        Assertions.assertTrue(glueCatalog.partitionExists(tablePath, partitionSpec));
        CatalogPartitionSpec partitionSpecWithNoPartition =
                new CatalogPartitionSpec(new HashMap<>());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueCatalog.partitionExists(tablePath, partitionSpecWithNoPartition));
        Map<String, String> data = new HashMap<>();
        data.put("col2", "zz1");

        CatalogPartitionSpec partSpecWithPartitionNotExist = new CatalogPartitionSpec(data);
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueCatalog.partitionExists(tablePath, partSpecWithPartitionNotExist));
    }

    // ---- stats

    @Test
    public void testAllStatisticsOperationNotSupported()
            throws PartitionNotExistException, TableNotExistException {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(getPartitionSpecParams());
        CatalogColumnStatistics columnStatistics = new CatalogColumnStatistics(new HashMap<>());
        CatalogTableStatistics catalogTableStatistics =
                new CatalogTableStatistics(0L, 0, 0L, 0L, new HashMap<>());

        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        glueCatalog.alterPartitionColumnStatistics(
                                tablePath, partitionSpec, columnStatistics, true));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        glueCatalog.alterPartitionColumnStatistics(
                                tablePath, partitionSpec, columnStatistics, false));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        glueCatalog.alterPartitionStatistics(
                                tablePath, partitionSpec, catalogTableStatistics, true));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        glueCatalog.alterPartitionStatistics(
                                tablePath, partitionSpec, catalogTableStatistics, false));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> glueCatalog.alterTableColumnStatistics(tablePath, columnStatistics, true));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> glueCatalog.alterTableColumnStatistics(tablePath, columnStatistics, false));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> glueCatalog.alterTableStatistics(tablePath, catalogTableStatistics, true));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> glueCatalog.alterTableStatistics(tablePath, catalogTableStatistics, false));
        Assertions.assertEquals(
                glueCatalog.getPartitionColumnStatistics(tablePath, partitionSpec),
                CatalogColumnStatistics.UNKNOWN);
        Assertions.assertEquals(
                glueCatalog.getPartitionStatistics(tablePath, partitionSpec),
                CatalogTableStatistics.UNKNOWN);
        Assertions.assertEquals(
                glueCatalog.getTableColumnStatistics(tablePath), CatalogColumnStatistics.UNKNOWN);
        Assertions.assertEquals(
                glueCatalog.getTableStatistics(tablePath), CatalogTableStatistics.UNKNOWN);
    }

    private void createDatabase(String databaseName) {
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(databaseName, getDummyCatalogDatabase(), false));
    }

    private void createTable(ObjectPath tablePath) {
        CatalogBaseTable baseTable = getDummyCatalogTableWithPartition();
        Assertions.assertDoesNotThrow(() -> glueCatalog.createTable(tablePath, baseTable, true));
    }

    private void createNonPartitionedTable(ObjectPath tablePath) {
        CatalogBaseTable baseTable = getDummyCatalogTable();
        Assertions.assertDoesNotThrow(() -> glueCatalog.createTable(tablePath, baseTable, true));
    }

    private void createView(ObjectPath tablePath) {
        Column column1 = Column.physical(COLUMN_1, DataTypes.STRING());
        Column column2 = Column.physical(COLUMN_2, DataTypes.STRING());
        ResolvedSchema schema = ResolvedSchema.of(Arrays.asList(column1, column2));
        CatalogView catalogView =
                CatalogView.of(
                        Schema.newBuilder()
                                .column(COLUMN_1, DataTypes.STRING())
                                .column(COLUMN_2, DataTypes.STRING())
                                .build(),
                        COMMENT,
                        "",
                        "",
                        getDummyTableParams());

        ResolvedCatalogView resolvedCatalogView = new ResolvedCatalogView(catalogView, schema);
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createTable(tablePath, resolvedCatalogView, true));
    }

    private void createFunction(
            ObjectPath functionPath, FunctionLanguage language, String className) {
        CatalogFunction catalogFunction =
                new CatalogFunctionImpl(
                        className, language, GlueCatalogTestUtils.dummyFlinkResourceUri());

        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createFunction(functionPath, catalogFunction, true));
    }

    private void createPartition(ObjectPath tablePath) {
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(getPartitionSpecParams());
        CatalogPartition catalogPartition =
                new CatalogPartitionImpl(GlueCatalogTestUtils.getCatalogPartitionParams(), COMMENT);
        Assertions.assertDoesNotThrow(
                () ->
                        glueCatalog.createPartition(
                                tablePath, partitionSpec, catalogPartition, false));
    }
}
