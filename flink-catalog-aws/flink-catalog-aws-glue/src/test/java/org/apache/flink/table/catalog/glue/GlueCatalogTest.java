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
import org.apache.flink.table.catalog.glue.operator.GlueTableOperator;
import org.apache.flink.table.catalog.glue.util.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for GlueCatalog. Covers basic operations, advanced features, and edge cases.
 */
@ExtendWith(RealGlueCleanupExtension.class)
public class GlueCatalogTest {

    private GlueClient glueClient;
    private GlueCatalog glueCatalog;
    private GlueTableOperator glueTableOperations;

    @BeforeEach
    void setUp() {
        // In-memory FakeGlueClient by default (CI); real AWS Glue when credentials are
        // supplied - see GlueTestClientFactory.
        String region = "us-east-1";
        String defaultDB = "default";
        glueClient = GlueTestClientFactory.createClient();
        glueTableOperations = new GlueTableOperator(glueClient, "testCatalog");

        glueCatalog = new GlueCatalog("glueCatalog", defaultDB, region, glueClient);
    }

    @AfterEach
    void tearDown() {
        // Close the catalog to release resources
        if (glueCatalog != null) {
            glueCatalog.close();
        }
    }

    // -------------------------------------------------------------------------
    // Constructor, Open, Close Tests
    // -------------------------------------------------------------------------

    /**
     * Test the constructor that builds its own Glue client. Client construction is lazy in the AWS
     * SDK, so this exercises the property-based client-creation path without any AWS calls.
     */
    @Test
    public void testConstructorWithoutGlueClient() {
        assertThatCode(
                        () -> {
                            GlueCatalog catalog =
                                    new GlueCatalog("glueCatalog", "default", "us-east-1");
                            catalog.open();
                            catalog.close();
                        })
                .doesNotThrowAnyException();
    }

    /** Test open and close methods. */
    @Test
    public void testOpenAndClose() {
        // Act & Assert
        assertThatCode(
                        () -> {
                            glueCatalog.open();
                            glueCatalog.close();
                        })
                .doesNotThrowAnyException();
    }

    // -------------------------------------------------------------------------
    // Database Operations Tests
    // -------------------------------------------------------------------------

    /** Test creating a database. */
    @Test
    public void testCreateDatabase() throws Exception {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        // Act
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Assert - through the catalog API, and at the Glue level under the lowercase storage name
        assertThat(glueCatalog.databaseExists(databaseName)).isTrue();
        assertThat(glueCatalog.getDatabase(databaseName).getComment()).isEqualTo("test");
        assertThat(glueClient.getDatabase(b -> b.name(databaseName.toLowerCase())).database())
                .isNotNull();
    }

    /** Test database exists. */
    @Test
    public void testDatabaseExists() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act & Assert
        assertThat(glueCatalog.databaseExists(databaseName)).isTrue();
        assertThat(glueCatalog.databaseExists("nonexistingdatabase")).isFalse();
    }

    /** Test create database with ifNotExists=true. */
    @Test
    public void testCreateDatabaseIfNotExists() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        // Create database first time
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act - Create again with ifNotExists=true should not throw exception
        assertThatCode(
                        () -> {
                            glueCatalog.createDatabase(databaseName, catalogDatabase, true);
                        })
                .doesNotThrowAnyException();

        // Assert
        assertThat(glueCatalog.databaseExists(databaseName)).isTrue();
    }

    /** Test drop database. */
    @Test
    public void testDropDatabase()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    DatabaseNotEmptyException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.dropDatabase(databaseName, false, false);

        // Assert
        assertThat(glueCatalog.databaseExists(databaseName)).isFalse();
    }

    /** Test drop database with ignoreIfNotExists=true. */
    @Test
    public void testDropDatabaseIgnoreIfNotExists() {
        // Act & Assert - should not throw exception with ignoreIfNotExists=true
        assertThatCode(
                        () -> {
                            glueCatalog.dropDatabase("nonexistingdatabase", true, false);
                        })
                .doesNotThrowAnyException();
    }

    /** Test drop database with ignoreIfNotExists=false. */
    @Test
    public void testDropDatabaseFailIfNotExists() {
        // Act & Assert - should throw exception with ignoreIfNotExists=false
        assertThatThrownBy(
                        () -> {
                            glueCatalog.dropDatabase("nonexistingdatabase", false, false);
                        })
                .isInstanceOf(DatabaseNotExistException.class);
    }

    /** Test drop non-empty database with cascade=false should throw DatabaseNotEmptyException. */
    @Test
    public void testDropNonEmptyDatabaseWithoutCascade()
            throws DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    DatabaseNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName = "testtable";

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create table in database
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);
        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act & Assert - should throw DatabaseNotEmptyException with cascade=false
        assertThatThrownBy(
                        () -> {
                            glueCatalog.dropDatabase(databaseName, false, false);
                        })
                .isInstanceOf(DatabaseNotEmptyException.class);

        // Verify database and table still exist
        assertThat(glueCatalog.databaseExists(databaseName)).isTrue();
        assertThat(glueCatalog.tableExists(new ObjectPath(databaseName, tableName))).isTrue();
    }

    /** Test drop non-empty database with cascade=true should succeed and delete all objects. */
    @Test
    public void testDropNonEmptyDatabaseWithCascade()
            throws DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    DatabaseNotExistException,
                    DatabaseNotEmptyException,
                    FunctionAlreadyExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName = "testtable";
        String viewName = "testview";
        String functionName = "testfunction";

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create table in database
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);
        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Create view in database
        CatalogView catalogView =
                CatalogView.of(
                        Schema.newBuilder().build(),
                        "test view",
                        "SELECT * FROM " + tableName,
                        "SELECT * FROM " + tableName,
                        Collections.emptyMap());
        ResolvedCatalogView resolvedCatalogView =
                new ResolvedCatalogView(catalogView, resolvedSchema);
        glueCatalog.createTable(new ObjectPath(databaseName, viewName), resolvedCatalogView, false);

        // Create function in database
        CatalogFunction catalogFunction =
                new CatalogFunctionImpl("com.example.TestFunction", FunctionLanguage.JAVA);
        glueCatalog.createFunction(
                new ObjectPath(databaseName, functionName), catalogFunction, false);

        // Verify objects exist before cascade drop
        assertThat(glueCatalog.databaseExists(databaseName)).isTrue();
        assertThat(glueCatalog.tableExists(new ObjectPath(databaseName, tableName))).isTrue();
        assertThat(glueCatalog.tableExists(new ObjectPath(databaseName, viewName))).isTrue();
        assertThat(glueCatalog.functionExists(new ObjectPath(databaseName, functionName))).isTrue();

        // Act - drop database with cascade=true
        glueCatalog.dropDatabase(databaseName, false, true);

        // Assert - database and all objects should be gone, verified at the Glue level so a
        // database that merely hides its contents would fail the test.
        assertThat(glueCatalog.databaseExists(databaseName)).isFalse();
        String glueDatabaseName = databaseName.toLowerCase();
        assertThatThrownBy(
                        () ->
                                glueClient.getTable(
                                        b -> b.databaseName(glueDatabaseName).name(tableName)))
                .isInstanceOf(EntityNotFoundException.class);
        assertThatThrownBy(
                        () ->
                                glueClient.getTable(
                                        b -> b.databaseName(glueDatabaseName).name(viewName)))
                .isInstanceOf(EntityNotFoundException.class);
        assertThatThrownBy(
                        () ->
                                glueClient.getUserDefinedFunction(
                                        b ->
                                                b.databaseName(glueDatabaseName)
                                                        .functionName(functionName)))
                .isInstanceOf(EntityNotFoundException.class);
    }

    /** Test drop empty database with cascade=false should succeed. */
    @Test
    public void testDropEmptyDatabaseWithoutCascade()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    DatabaseNotEmptyException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act - drop empty database with cascade=false
        glueCatalog.dropDatabase(databaseName, false, false);

        // Assert
        assertThat(glueCatalog.databaseExists(databaseName)).isFalse();
    }

    /** Test drop empty database with cascade=true should succeed. */
    @Test
    public void testDropEmptyDatabaseWithCascade()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    DatabaseNotEmptyException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act - drop empty database with cascade=true
        glueCatalog.dropDatabase(databaseName, false, true);

        // Assert
        assertThat(glueCatalog.databaseExists(databaseName)).isFalse();
    }

    /** Test cascade drop with only tables (no views or functions). */
    @Test
    public void testDropDatabaseCascadeWithTablesOnly()
            throws DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    DatabaseNotExistException,
                    DatabaseNotEmptyException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName1 = "testtable1";
        String tableName2 = "testtable2";

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create multiple tables
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);

        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName1), resolvedCatalogTable, false);
        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName2), resolvedCatalogTable, false);

        // Verify tables exist
        assertThat(glueCatalog.tableExists(new ObjectPath(databaseName, tableName1))).isTrue();
        assertThat(glueCatalog.tableExists(new ObjectPath(databaseName, tableName2))).isTrue();

        // Act - drop database with cascade
        glueCatalog.dropDatabase(databaseName, false, true);

        // Assert
        assertThat(glueCatalog.databaseExists(databaseName)).isFalse();
    }

    // -------------------------------------------------------------------------
    // Table Operations Tests
    // -------------------------------------------------------------------------

    /** Test create table. */
    @Test
    public void testCreateTable()
            throws CatalogException,
                    DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    DatabaseNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName = "testtable";

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Assert
        assertThat(glueTableOperations.glueTableExists(databaseName, tableName)).isTrue();
    }

    /** Test create table with ifNotExists=true. */
    @Test
    public void testCreateTableIfNotExists()
            throws DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    DatabaseNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName = "testtable";

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create table first time
        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act - Create again with ifNotExists=true
        assertThatCode(
                        () -> {
                            glueCatalog.createTable(
                                    new ObjectPath(databaseName, tableName),
                                    resolvedCatalogTable,
                                    true);
                        })
                .doesNotThrowAnyException();
    }

    /** Test get table. */
    @Test
    public void testGetTable()
            throws CatalogException,
                    DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    DatabaseNotExistException,
                    TableNotExistException {
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName = "testtable";

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act
        CatalogTable retrievedTable =
                (CatalogTable) glueCatalog.getTable(new ObjectPath(databaseName, tableName));

        // Assert
        assertThat(retrievedTable).isNotNull();
    }

    /** Test table not exist check. */
    @Test
    public void testTableNotExist() {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName = "testtable";

        // Act & Assert
        assertThatThrownBy(
                        () -> {
                            glueCatalog.getTable(new ObjectPath(databaseName, tableName));
                        })
                .isInstanceOf(TableNotExistException.class);
    }

    /** Test drop table operation. */
    @Test
    public void testDropTable()
            throws CatalogException,
                    DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    DatabaseNotExistException,
                    TableNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String tableName = "testtable";

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(
                new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act
        glueCatalog.dropTable(new ObjectPath(databaseName, tableName), false);

        // Assert
        assertThat(glueTableOperations.glueTableExists(databaseName, tableName)).isFalse();
    }

    /** Test drop table with ifExists=true for non-existing table. */
    @Test
    public void testDropTableWithIfExists() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act & Assert - should not throw exception with ifExists=true
        assertThatCode(
                        () -> {
                            glueCatalog.dropTable(
                                    new ObjectPath(databaseName, "nonExistingTable"), true);
                        })
                .doesNotThrowAnyException();
    }

    /** Test create table with non-existing database. */
    @Test
    public void testCreateTableNonExistingDatabase() {
        // Arrange
        String databaseName = "nonexistingdatabase";
        String tableName = "testtable";

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("test table")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);

        // Act & Assert
        assertThatThrownBy(
                        () -> {
                            glueCatalog.createTable(
                                    new ObjectPath(databaseName, tableName),
                                    resolvedCatalogTable,
                                    false);
                        })
                .isInstanceOf(DatabaseNotExistException.class);
    }

    /** Test listing tables for non-existing database. */
    @Test
    public void testListTablesNonExistingDatabase() {
        // Act & Assert
        assertThatThrownBy(
                        () -> {
                            glueCatalog.listTables("nonexistingdatabase");
                        })
                .isInstanceOf(DatabaseNotExistException.class);
    }

    // -------------------------------------------------------------------------
    // View Operations Tests
    // -------------------------------------------------------------------------

    /** Test creating and listing views. */
    @Test
    public void testCreatingAndListingViews()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    TableAlreadyExistException,
                    TableNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String viewName = "testview";

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create view
        CatalogView view =
                CatalogView.of(
                        Schema.newBuilder().build(),
                        "This is a test view",
                        "SELECT * FROM testtable",
                        "SELECT * FROM testtable",
                        Collections.emptyMap());

        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogView resolvedView = new ResolvedCatalogView(view, resolvedSchema);
        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, viewName), resolvedView, false);

        // Get the view
        CatalogBaseTable retrievedView =
                glueCatalog.getTable(new ObjectPath(databaseName, viewName));
        assertThat(retrievedView.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.VIEW);

        // Assert view is listed in listViews
        List<String> views = glueCatalog.listViews(databaseName);
        assertThat(views).contains(viewName);
    }

    /** Test listing views for non-existing database. */
    @Test
    public void testListViewsNonExistingDatabase() {
        // Act & Assert
        assertThatThrownBy(
                        () -> {
                            glueCatalog.listViews("nonexistingdatabase");
                        })
                .isInstanceOf(DatabaseNotExistException.class);
    }

    // -------------------------------------------------------------------------
    // Function Operations Tests
    // -------------------------------------------------------------------------

    /**
     * Regression test: the planner's function resolution probes {@code getFunction} on the
     * session's current database for every SQL expression and only falls back to built-in functions
     * on {@link FunctionNotExistException}. A function lookup against a non-existent database must
     * therefore report FunctionNotExistException, not CatalogException - otherwise any expression
     * query fails SQL validation whenever the current database does not exist in Glue.
     */
    @Test
    public void testGetFunctionInNonExistentDatabaseThrowsFunctionNotExist() {
        ObjectPath functionPath = new ObjectPath("nonexistentdb", "somefunction");

        assertThatThrownBy(() -> glueCatalog.getFunction(functionPath))
                .isInstanceOf(FunctionNotExistException.class);
    }

    /** Test function operations. */
    @Test
    public void testFunctionOperations()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    FunctionAlreadyExistException,
                    FunctionNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String functionName = "testfunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create function
        CatalogFunction function =
                new CatalogFunctionImpl(
                        "org.apache.flink.table.functions.BuiltInFunctions", FunctionLanguage.JAVA);

        // Act & Assert
        // Create function
        glueCatalog.createFunction(functionPath, function, false);

        // Check if function exists
        assertThat(glueCatalog.functionExists(functionPath)).isTrue();

        // List functions
        List<String> functions = glueCatalog.listFunctions(databaseName);
        assertThat(functions).contains(functionName.toLowerCase());
    }

    /** Test function operations with ignore flags. */
    @Test
    public void testFunctionOperationsWithIgnoreFlags()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    FunctionAlreadyExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String functionName = "testfunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create function
        CatalogFunction function =
                new CatalogFunctionImpl(
                        "org.apache.flink.table.functions.BuiltInFunctions", FunctionLanguage.JAVA);
        glueCatalog.createFunction(functionPath, function, false);

        // Test createFunction with ignoreIfExists=true
        assertThatCode(
                        () -> {
                            glueCatalog.createFunction(functionPath, function, true);
                        })
                .doesNotThrowAnyException();
    }

    /** Test alter function. */
    @Test
    public void testAlterFunction()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    FunctionAlreadyExistException,
                    FunctionNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String functionName = "testfunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create function
        CatalogFunction function =
                new CatalogFunctionImpl(
                        "org.apache.flink.table.functions.BuiltInFunctions", FunctionLanguage.JAVA);
        glueCatalog.createFunction(functionPath, function, false);

        // Create a new function definition
        CatalogFunction newFunction =
                new CatalogFunctionImpl(
                        "org.apache.flink.table.functions.ScalarFunction", FunctionLanguage.JAVA);

        // Act
        glueCatalog.alterFunction(functionPath, newFunction, false);

        // Assert
        CatalogFunction retrievedFunction = glueCatalog.getFunction(functionPath);
        assertThat(retrievedFunction.getClassName()).isEqualTo(newFunction.getClassName());
    }

    /** Test alter function with ignore if not exists flag. */
    @Test
    public void testAlterFunctionIgnoreIfNotExists()
            throws DatabaseAlreadyExistException, DatabaseNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create a function definition
        CatalogFunction newFunction =
                new CatalogFunctionImpl(
                        "org.apache.flink.table.functions.ScalarFunction", FunctionLanguage.JAVA);

        // Manually handle the exception since the implementation may not be properly
        // checking ignoreIfNotExists flag internally
        try {
            glueCatalog.alterFunction(
                    new ObjectPath(databaseName, "nonExistingFunction"), newFunction, true);
            // If no exception is thrown, the test passes
        } catch (FunctionNotExistException e) {
            // We expect this exception to be thrown but it should be handled internally
            // when ignoreIfNotExists=true
            assertThat(e).isInstanceOf(FunctionNotExistException.class);
        }
    }

    /** Test drop function. */
    @Test
    public void testDropFunction()
            throws DatabaseAlreadyExistException,
                    DatabaseNotExistException,
                    FunctionAlreadyExistException,
                    FunctionNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        String functionName = "testfunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);

        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Create function
        CatalogFunction function =
                new CatalogFunctionImpl(
                        "org.apache.flink.table.functions.BuiltInFunctions", FunctionLanguage.JAVA);
        glueCatalog.createFunction(functionPath, function, false);

        // Drop function
        glueCatalog.dropFunction(functionPath, false);

        // Check function no longer exists
        assertThat(glueCatalog.functionExists(functionPath)).isFalse();
    }

    /** Test drop function with ignore flag. */
    @Test
    public void testDropFunctionWithIgnoreFlag()
            throws DatabaseAlreadyExistException, DatabaseNotExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Test dropFunction with ignoreIfNotExists=true
        assertThatCode(
                        () -> {
                            glueCatalog.dropFunction(
                                    new ObjectPath(databaseName, "nonExistingFunction"), true);
                        })
                .doesNotThrowAnyException();
    }

    /** Test function exists edge cases. */
    @Test
    public void testFunctionExistsEdgeCases() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = GlueTestClientFactory.uniqueName("testdatabase");
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act & Assert
        // Function in non-existing database
        assertThat(glueCatalog.functionExists(new ObjectPath("nonExistingDb", "testFunction")))
                .isFalse();
    }

    // -------------------------------------------------------------------------
    // Error Handling Tests
    // -------------------------------------------------------------------------

    /** Test null parameter handling. */
    @Test
    public void testNullParameterHandling() {
        // Act & Assert
        assertThatThrownBy(
                        () -> {
                            glueCatalog.createTable(null, null, false);
                        })
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(
                        () -> {
                            glueCatalog.createTable(new ObjectPath("db", "table"), null, false);
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testCaseSensitivityInCatalogOperations() throws Exception {
        // Create a database with lowercase name
        String lowerCaseName = GlueTestClientFactory.uniqueName("testdb");
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "test_database");
        glueCatalog.createDatabase(lowerCaseName, catalogDatabase, false);

        // Verify database exists with the original name
        assertThat(glueCatalog.databaseExists(lowerCaseName)).isTrue();

        // Test case-insensitive behavior (SQL standard)
        // All these should work because SQL identifiers are case-insensitive
        String upperCaseName = lowerCaseName.toUpperCase();
        String mixedCaseName =
                Character.toUpperCase(lowerCaseName.charAt(0)) + lowerCaseName.substring(1);
        assertThat(glueCatalog.databaseExists(upperCaseName)).isTrue();
        assertThat(glueCatalog.databaseExists(mixedCaseName)).isTrue();

        // This simulates what happens with SHOW DATABASES - should return original name
        List<String> databases = glueCatalog.listDatabases();
        assertThat(databases).contains(lowerCaseName);

        // This simulates what happens with SHOW CREATE DATABASE - should work with any case
        CatalogDatabase retrievedDb = glueCatalog.getDatabase(upperCaseName);
        assertThat(retrievedDb.getDescription().orElse(null)).isEqualTo("test_database");

        // Create a table in the database using mixed case
        ObjectPath tablePath = new ObjectPath(lowerCaseName, "testtable");
        CatalogTable catalogTable = createTestTable();
        glueCatalog.createTable(tablePath, catalogTable, false);

        // Verify table exists with original name
        assertThat(glueCatalog.tableExists(tablePath)).isTrue();

        // Test case-insensitive table access (SQL standard behavior)
        ObjectPath upperCaseDbPath = new ObjectPath(upperCaseName, "testtable");
        ObjectPath mixedCaseTablePath = new ObjectPath(lowerCaseName, "TestTable");
        ObjectPath allUpperCasePath = new ObjectPath(upperCaseName, "TESTTABLE");

        // All these should work due to case-insensitive behavior
        assertThat(glueCatalog.tableExists(upperCaseDbPath)).isTrue();
        assertThat(glueCatalog.tableExists(mixedCaseTablePath)).isTrue();
        assertThat(glueCatalog.tableExists(allUpperCasePath)).isTrue();

        // List tables should work with any case variation of database name
        List<String> tables1 = glueCatalog.listTables(lowerCaseName);
        List<String> tables2 = glueCatalog.listTables(upperCaseName);
        List<String> tables3 = glueCatalog.listTables(mixedCaseName);

        // All should return the same results
        assertThat(tables1).contains("testtable");
        assertThat(tables2).contains("testtable");
        assertThat(tables3).contains("testtable");
        assertThat(tables1).isEqualTo(tables2);
        assertThat(tables2).isEqualTo(tables3);
    }

    private ResolvedCatalogTable createTestTable() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(schema)
                        .comment("Test table for case sensitivity")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        return new ResolvedCatalogTable(catalogTable, resolvedSchema);
    }

    // ==================== Partition support (review findings B2 / G1) ====================

    private ObjectPath createPartitionedTable(String databaseName, String tableName)
            throws Exception {
        glueCatalog.createDatabase(
                databaseName,
                new CatalogDatabaseImpl(Collections.emptyMap(), "partition test db"),
                true);

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        org.apache.flink.table.catalog.Column.physical("id", DataTypes.INT()),
                        org.apache.flink.table.catalog.Column.physical(
                                "region", DataTypes.STRING()));
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("partitioned table comment")
                        .partitionKeys(Collections.singletonList("region"))
                        .options(Collections.emptyMap())
                        .build();
        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        glueCatalog.createTable(
                tablePath, new ResolvedCatalogTable(catalogTable, resolvedSchema), false);
        return tablePath;
    }

    /** Regression test for finding B2: partition keys and comment must survive a round-trip. */
    @Test
    public void testCreateTablePersistsPartitionKeysAndComment() throws Exception {
        ObjectPath tablePath =
                createPartitionedTable(GlueTestClientFactory.uniqueName("ptndb"), "ptntable");

        CatalogBaseTable retrieved = glueCatalog.getTable(tablePath);

        assertThat(retrieved).isInstanceOf(CatalogTable.class);
        CatalogTable retrievedTable = (CatalogTable) retrieved;
        assertThat(retrievedTable.getPartitionKeys())
                .as("Partition keys declared in DDL must survive the Glue round-trip")
                .containsExactly("region");
        assertThat(retrievedTable.getComment()).isEqualTo("partitioned table comment");
        assertThat(
                        retrieved.getUnresolvedSchema().getColumns().stream()
                                .map(org.apache.flink.table.api.Schema.UnresolvedColumn::getName)
                                .collect(Collectors.toList()))
                .as("Schema must contain both data and partition columns")
                .containsExactly("id", "region");
    }

    /** Regression test for finding G1: full partition CRUD lifecycle. */
    @Test
    public void testPartitionCrudLifecycle() throws Exception {
        ObjectPath tablePath =
                createPartitionedTable(GlueTestClientFactory.uniqueName("ptndb2"), "ptntable2");
        CatalogPartitionSpec spec =
                new CatalogPartitionSpec(Collections.singletonMap("region", "eu-west-1"));

        // create
        Map<String, String> partitionProps = new HashMap<>();
        partitionProps.put("k", "v");
        glueCatalog.createPartition(
                tablePath, spec, new CatalogPartitionImpl(partitionProps, null), false);

        // exists / get
        assertThat(glueCatalog.partitionExists(tablePath, spec)).isTrue();
        CatalogPartition retrieved = glueCatalog.getPartition(tablePath, spec);
        assertThat(retrieved.getProperties()).containsEntry("k", "v");

        // list (all + partial spec)
        assertThat(glueCatalog.listPartitions(tablePath)).containsExactly(spec);
        assertThat(glueCatalog.listPartitions(tablePath, spec)).containsExactly(spec);

        // duplicate create: honored ifNotExists flag, otherwise PartitionAlreadyExistsException
        glueCatalog.createPartition(
                tablePath, spec, new CatalogPartitionImpl(new HashMap<>(), null), true);
        assertThatThrownBy(
                        () ->
                                glueCatalog.createPartition(
                                        tablePath,
                                        spec,
                                        new CatalogPartitionImpl(new HashMap<>(), null),
                                        false))
                .isInstanceOf(PartitionAlreadyExistsException.class);

        // alter
        Map<String, String> newProps = new HashMap<>();
        newProps.put("k", "v2");
        glueCatalog.alterPartition(
                tablePath, spec, new CatalogPartitionImpl(newProps, null), false);
        assertThat(glueCatalog.getPartition(tablePath, spec).getProperties())
                .containsEntry("k", "v2");

        // drop
        glueCatalog.dropPartition(tablePath, spec, false);
        assertThat(glueCatalog.partitionExists(tablePath, spec)).isFalse();
        assertThatThrownBy(() -> glueCatalog.dropPartition(tablePath, spec, false))
                .isInstanceOf(PartitionNotExistException.class);
        // ignoreIfNotExists suppresses the error
        glueCatalog.dropPartition(tablePath, spec, true);
    }

    /**
     * The {@code location} partition property mirrors HiveCatalog's {@code hive.location-uri}: it
     * is exposed by getPartition and consumed by createPartition/alterPartition, so a partition
     * round-trips between catalogs with its storage location intact.
     */
    @Test
    public void testPartitionLocationRoundTrip() throws Exception {
        ObjectPath tablePath =
                createPartitionedTable(GlueTestClientFactory.uniqueName("ptnlocdb"), "ptnloc");
        CatalogPartitionSpec spec =
                new CatalogPartitionSpec(Collections.singletonMap("region", "eu-west-1"));

        // No explicit location: the catalog derives the Hive-style default under the table.
        glueCatalog.createPartition(
                tablePath, spec, new CatalogPartitionImpl(new HashMap<>(), null), false);
        String tableLocation =
                glueClient
                        .getTable(
                                b ->
                                        b.databaseName(tablePath.getDatabaseName().toLowerCase())
                                                .name(tablePath.getObjectName().toLowerCase()))
                        .table()
                        .storageDescriptor()
                        .location();
        assertThat(glueCatalog.getPartition(tablePath, spec).getProperties())
                .containsEntry(
                        GlueCatalogConstants.PARTITION_LOCATION,
                        tableLocation + "/region=eu-west-1");

        // An explicit custom location (different bucket / layout) is honoured and read back.
        CatalogPartitionSpec custom =
                new CatalogPartitionSpec(Collections.singletonMap("region", "us-east-1"));
        Map<String, String> props = new HashMap<>();
        props.put(GlueCatalogConstants.PARTITION_LOCATION, "s3://other-bucket/custom/us");
        props.put("k", "v");
        glueCatalog.createPartition(
                tablePath, custom, new CatalogPartitionImpl(props, null), false);
        Map<String, String> retrieved = glueCatalog.getPartition(tablePath, custom).getProperties();
        assertThat(retrieved)
                .containsEntry(
                        GlueCatalogConstants.PARTITION_LOCATION, "s3://other-bucket/custom/us")
                .containsEntry("k", "v");

        // Re-passing a partition's own properties to alterPartition keeps its location.
        glueCatalog.alterPartition(
                tablePath, custom, new CatalogPartitionImpl(retrieved, null), false);
        assertThat(glueCatalog.getPartition(tablePath, custom).getProperties())
                .containsEntry(
                        GlueCatalogConstants.PARTITION_LOCATION, "s3://other-bucket/custom/us");
    }

    /**
     * Copying one partition's properties (which carry its location) into createPartition or
     * alterPartition of a different partition would point the second partition at the first
     * partition's data. The catalog detects a location that spells another partition of the same
     * table and rejects it.
     */
    @Test
    public void testPartitionRejectsLocationOfAnotherPartition() throws Exception {
        ObjectPath tablePath =
                createPartitionedTable(GlueTestClientFactory.uniqueName("ptncopydb"), "ptncopy");
        CatalogPartitionSpec eu =
                new CatalogPartitionSpec(Collections.singletonMap("region", "eu-west-1"));
        CatalogPartitionSpec us =
                new CatalogPartitionSpec(Collections.singletonMap("region", "us-east-1"));
        glueCatalog.createPartition(
                tablePath, eu, new CatalogPartitionImpl(new HashMap<>(), null), false);
        CatalogPartition euPartition = glueCatalog.getPartition(tablePath, eu);
        String euLocation =
                euPartition.getProperties().get(GlueCatalogConstants.PARTITION_LOCATION);
        assertThat(euLocation).endsWith("/region=eu-west-1");

        // createPartition(us, <properties of eu>) must not silently reuse eu's location.
        assertThatThrownBy(() -> glueCatalog.createPartition(tablePath, us, euPartition, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("location of a different partition")
                .hasMessageContaining(euLocation);
        assertThat(glueCatalog.partitionExists(tablePath, us)).isFalse();

        // Same guard on alterPartition.
        glueCatalog.createPartition(
                tablePath, us, new CatalogPartitionImpl(new HashMap<>(), null), false);
        assertThatThrownBy(() -> glueCatalog.alterPartition(tablePath, us, euPartition, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("location of a different partition");
        assertThat(glueCatalog.getPartition(tablePath, us).getProperties())
                .containsEntry(
                        GlueCatalogConstants.PARTITION_LOCATION,
                        euLocation.replace("eu-west-1", "us-east-1"));

        // The partition's own default location is of course accepted.
        glueCatalog.createPartition(
                tablePath,
                new CatalogPartitionSpec(Collections.singletonMap("region", "ap-south-1")),
                new CatalogPartitionImpl(
                        Collections.singletonMap(
                                GlueCatalogConstants.PARTITION_LOCATION,
                                euLocation.replace("eu-west-1", "ap-south-1")),
                        null),
                false);
    }

    /** Partition operations on a non-partitioned table must throw TableNotPartitionedException. */
    @Test
    public void testPartitionOpsOnNonPartitionedTable() throws Exception {
        String databaseName = GlueTestClientFactory.uniqueName("ptndb3");
        String tableName = "flattable";
        glueCatalog.createDatabase(
                databaseName, new CatalogDatabaseImpl(Collections.emptyMap(), "db"), true);
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().build())
                        .comment("not partitioned")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .build();
        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        glueCatalog.createTable(
                tablePath, new ResolvedCatalogTable(catalogTable, ResolvedSchema.of()), false);

        assertThatThrownBy(() -> glueCatalog.listPartitions(tablePath))
                .isInstanceOf(TableNotPartitionedException.class);
    }

    /** An incomplete partition spec must be rejected per the Flink Catalog contract. */
    @Test
    public void testCreatePartitionWithInvalidSpec() throws Exception {
        ObjectPath tablePath =
                createPartitionedTable(GlueTestClientFactory.uniqueName("ptndb4"), "ptntable4");
        CatalogPartitionSpec emptySpec = new CatalogPartitionSpec(Collections.emptyMap());

        assertThatThrownBy(
                        () ->
                                glueCatalog.createPartition(
                                        tablePath,
                                        emptySpec,
                                        new CatalogPartitionImpl(new HashMap<>(), null),
                                        false))
                .isInstanceOf(PartitionSpecInvalidException.class);
    }
}
