package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.catalog.glue.operations.FakeGlueClient;
import org.apache.flink.table.catalog.glue.operations.GlueDatabaseOperations;
import org.apache.flink.table.catalog.glue.operations.GlueTableOperations;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;

/**
 * Comprehensive tests for GlueCatalog.
 * Covers basic operations, advanced features, and edge cases.
 */
public class GlueCatalogTest {

    private FakeGlueClient fakeGlueClient;
    private GlueCatalog glueCatalog;
    private GlueTableOperations glueTableOperations;
    private GlueDatabaseOperations glueDatabaseOperations;

    @BeforeEach
    void setUp() {
        // Reset the state of FakeGlueClient before each test
        FakeGlueClient.reset();
        String region = "us-east-1";
        String defaultDB = "default";
        fakeGlueClient = new FakeGlueClient();
        glueTableOperations = new GlueTableOperations(fakeGlueClient, "testCatalog");
        glueDatabaseOperations = new GlueDatabaseOperations(fakeGlueClient, "testCatalog");

        glueCatalog = new GlueCatalog("glueCatalog", defaultDB, region, fakeGlueClient);
    }

    //-------------------------------------------------------------------------
    // Constructor, Open, Close Tests
    //-------------------------------------------------------------------------

    /**
     * Test constructor without explicit GlueClient
     */
    @Test
    public void testConstructorWithoutGlueClient() {
        // Act
        GlueCatalog catalog = new GlueCatalog("glueCatalog", "default", "us-east-1");
        
        // Assert
        assertNotNull(catalog);
        // Verify it can be opened and closed without exceptions
        assertDoesNotThrow(() -> {
            catalog.open();
            catalog.close();
        });
    }

    /**
     * Test open and close methods
     */
    @Test
    public void testOpenAndClose() {
        // Act & Assert
        assertDoesNotThrow(() -> {
            glueCatalog.open();
            glueCatalog.close();
        });
    }

    //-------------------------------------------------------------------------
    // Database Operations Tests
    //-------------------------------------------------------------------------

    /**
     * Test creating a database
     */
    @Test
    public void testCreateDatabase() throws CatalogException, DatabaseAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        // Act
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Assert
        assertTrue(glueDatabaseOperations.glueDatabaseExists(databaseName));
    }

    /**
     * Test database exists
     */
    @Test
    public void testDatabaseExists() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act & Assert
        assertTrue(glueCatalog.databaseExists(databaseName));
        assertFalse(glueCatalog.databaseExists("nonExistingDatabase"));
    }

    /**
     * Test create database with ifNotExists=true
     */
    @Test
    public void testCreateDatabaseIfNotExists() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        
        // Create database first time
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Act - Create again with ifNotExists=true should not throw exception
        assertDoesNotThrow(() -> {
            glueCatalog.createDatabase(databaseName, catalogDatabase, true);
        });
        
        // Assert
        assertTrue(glueCatalog.databaseExists(databaseName));
    }

    /**
     * Test drop database
     */
    @Test
    public void testDropDatabase() throws DatabaseAlreadyExistException, DatabaseNotExistException, DatabaseNotEmptyException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Act
        glueCatalog.dropDatabase(databaseName, false, false);
        
        // Assert
        assertFalse(glueCatalog.databaseExists(databaseName));
    }

    /**
     * Test drop database with ignoreIfNotExists=true
     */
    @Test
    public void testDropDatabaseIgnoreIfNotExists() {
        // Act & Assert - should not throw exception with ignoreIfNotExists=true
        assertDoesNotThrow(() -> {
            glueCatalog.dropDatabase("nonExistingDatabase", true, false);
        });
    }

    /**
     * Test drop database with ignoreIfNotExists=false
     */
    @Test
    public void testDropDatabaseFailIfNotExists() {
        // Act & Assert - should throw exception with ignoreIfNotExists=false
        assertThrows(DatabaseNotExistException.class, () -> {
            glueCatalog.dropDatabase("nonExistingDatabase", false, false);
        });
    }

    //-------------------------------------------------------------------------
    // Table Operations Tests
    //-------------------------------------------------------------------------

    /**
     * Test create table
     */
    @Test
    public void testCreateTable() throws CatalogException, DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        String tableName = "testTable";

        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema =  ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable, resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Assert
        assertTrue(glueTableOperations.glueTableExists(databaseName, tableName));
    }

    /**
     * Test create table with ifNotExists=true
     */
    @Test
    public void testCreateTableIfNotExists() throws DatabaseAlreadyExistException, 
            TableAlreadyExistException, DatabaseNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        String tableName = "testTable";
        
        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable, resolvedSchema);
        
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Create table first time
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);
        
        // Act - Create again with ifNotExists=true
        assertDoesNotThrow(() -> {
            glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, true);
        });
    }

    /**
     * Test get table
     */
    @Test
    public void testGetTable() throws CatalogException, DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
        String databaseName = "testDatabase";
        String tableName = "testTable";

        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema =  ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable, resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act
        CatalogTable retrievedTable = (CatalogTable) glueCatalog.getTable(new ObjectPath(databaseName, tableName));

        // Assert
        assertNotNull(retrievedTable, "Table should be retrieved");
    }

    /**
     * Test table not exist
     */
    @Test
    public void testTableNotExist() {
        // Arrange
        String databaseName = "testDatabase";
        String tableName = "testTable";

        // Act & Assert
        assertThrows(TableNotExistException.class, () -> {
            glueCatalog.getTable(new ObjectPath(databaseName, tableName));
        });
    }

    /**
     * Test drop table
     */
    @Test
    public void testDropTable() throws CatalogException, DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
        String databaseName = "testDatabase";
        String tableName = "testTable";

        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema =  ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable, resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act
        glueCatalog.dropTable(new ObjectPath(databaseName, tableName), false);

        // Assert
        assertFalse(glueTableOperations.glueTableExists(databaseName, tableName));
    }

    /**
     * Test drop table with ifExists=true for non-existing table
     */
    @Test
    public void testDropTableWithIfExists() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Act & Assert - should not throw exception with ifExists=true
        assertDoesNotThrow(() -> {
            glueCatalog.dropTable(new ObjectPath(databaseName, "nonExistingTable"), true);
        });
    }

    /**
     * Test error handling when creating table in non-existing database
     */
    @Test
    public void testCreateTableNonExistingDatabase() {
        // Arrange
        String databaseName = "nonExistingDatabase";
        String tableName = "testTable";
        
        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable, resolvedSchema);
        
        // Act & Assert
        assertThrows(DatabaseNotExistException.class, () -> {
            glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);
        });
    }

    /**
     * Test list tables when database does not exist
     */
    @Test
    public void testListTablesNonExistingDatabase() {
        // Act & Assert
        assertThrows(DatabaseNotExistException.class, () -> {
            glueCatalog.listTables("nonExistingDatabase");
        });
    }

    //-------------------------------------------------------------------------
    // View Operations Tests
    //-------------------------------------------------------------------------

    /**
     * Test creating and listing views
     */
    @Test
    public void testCreatingAndListingViews() throws DatabaseAlreadyExistException, DatabaseNotExistException,
            TableAlreadyExistException, TableNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        String viewName = "testView";
        
        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Create view
        CatalogView view = CatalogView.of(
                Schema.newBuilder().build(),
                "Test View",
                "SELECT * FROM sourceTable",
                "SELECT * FROM sourceTable",
                Collections.emptyMap()
        );
        ResolvedSchema resolvedSchema = ResolvedSchema.of();
        ResolvedCatalogView resolvedView = new ResolvedCatalogView(view, resolvedSchema);
        
        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, viewName), resolvedView, false);
        
        // Assert view can be retrieved
        CatalogBaseTable retrievedView = glueCatalog.getTable(new ObjectPath(databaseName, viewName));
        assertEquals(CatalogBaseTable.TableKind.VIEW, retrievedView.getTableKind());
        
        // Assert view is listed in listViews
        List<String> views = glueCatalog.listViews(databaseName);
        assertTrue(views.contains(viewName), "View should be in the list of views");
    }

    /**
     * Test listViews when database does not exist
     */
    @Test
    public void testListViewsNonExistingDatabase() {
        // Act & Assert
        assertThrows(DatabaseNotExistException.class, () -> {
            glueCatalog.listViews("nonExistingDatabase");
        });
    }

    //-------------------------------------------------------------------------
    // Function Operations Tests
    //-------------------------------------------------------------------------

    /**
     * Test normalize method for object paths
     */
    @Test
    public void testNormalize() {
        // Arrange
        ObjectPath originalPath = new ObjectPath("testDb", "TestFunction");
        
        // Act
        ObjectPath normalizedPath = glueCatalog.normalize(originalPath);
        
        // Assert
        assertEquals("testDb", normalizedPath.getDatabaseName());
        assertEquals(FunctionIdentifier.normalizeName("TestFunction"), normalizedPath.getObjectName());
    }
    
    /**
     * Test function operations separately to avoid dependency issues
     */
    @Test
    public void testFunctionOperations() throws DatabaseAlreadyExistException, DatabaseNotExistException,
            FunctionAlreadyExistException, FunctionNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        String functionName = "testFunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);
        
        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Create function
        CatalogFunction function = new CatalogFunctionImpl(
                "org.apache.flink.table.functions.BuiltInFunctions",
                FunctionLanguage.JAVA
        );
        
        // Act & Assert
        // Create function
        glueCatalog.createFunction(functionPath, function, false);
        
        // Check if function exists
        assertTrue(glueCatalog.functionExists(functionPath));
        
        // List functions
        List<String> functions = glueCatalog.listFunctions(databaseName);
        assertTrue(functions.contains(functionName.toLowerCase()));
    }

    /**
     * Test function operations with ignoreIfExists flag
     */
    @Test
    public void testFunctionOperationsWithIgnoreFlags() throws DatabaseAlreadyExistException, 
            DatabaseNotExistException, FunctionAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        String functionName = "testFunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);
        
        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Create function
        CatalogFunction function = new CatalogFunctionImpl(
                "org.apache.flink.table.functions.BuiltInFunctions",
                FunctionLanguage.JAVA
        );
        glueCatalog.createFunction(functionPath, function, false);
        
        // Test createFunction with ignoreIfExists=true
        assertDoesNotThrow(() -> {
            glueCatalog.createFunction(functionPath, function, true);
        });
    }

    /**
     * Test alter function
     */
    @Test
    public void testAlterFunction() throws DatabaseAlreadyExistException, DatabaseNotExistException,
            FunctionAlreadyExistException, FunctionNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        String functionName = "testFunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);
        
        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Create function
        CatalogFunction function = new CatalogFunctionImpl(
                "org.apache.flink.table.functions.BuiltInFunctions",
                FunctionLanguage.JAVA
        );
        glueCatalog.createFunction(functionPath, function, false);
        
        // Create a new function definition
        CatalogFunction newFunction = new CatalogFunctionImpl(
                "org.apache.flink.table.functions.ScalarFunction",
                FunctionLanguage.JAVA
        );
        
        // Act
        glueCatalog.alterFunction(functionPath, newFunction, false);
        
        // Assert
        CatalogFunction retrievedFunction = glueCatalog.getFunction(functionPath);
        assertEquals(newFunction.getClassName(), retrievedFunction.getClassName());
    }

    /**
     * Test alter function with ignoreIfNotExists flag
     */
    @Test
    public void testAlterFunctionIgnoreIfNotExists() throws DatabaseAlreadyExistException, DatabaseNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Create a function definition
        CatalogFunction newFunction = new CatalogFunctionImpl(
                "org.apache.flink.table.functions.ScalarFunction",
                FunctionLanguage.JAVA
        );
        
        // Manually handle the exception since the implementation may not be properly 
        // checking ignoreIfNotExists flag internally
        try {
            glueCatalog.alterFunction(
                    new ObjectPath(databaseName, "nonExistingFunction"),
                    newFunction,
                    true
            );
            // If no exception is thrown, the test passes
        } catch (FunctionNotExistException e) {
            // We expect this exception to be thrown but it should be handled internally
            // when ignoreIfNotExists=true
            fail("FunctionNotExistException should not be thrown when ignoreIfNotExists=true");
        }
    }

    /**
     * Test drop function
     */
    @Test
    public void testDropFunction() throws DatabaseAlreadyExistException, DatabaseNotExistException,
            FunctionAlreadyExistException, FunctionNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        String functionName = "testFunction";
        ObjectPath functionPath = new ObjectPath(databaseName, functionName);
        
        // Create database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Create function
        CatalogFunction function = new CatalogFunctionImpl(
                "org.apache.flink.table.functions.BuiltInFunctions",
                FunctionLanguage.JAVA
        );
        glueCatalog.createFunction(functionPath, function, false);
        
        // Drop function
        glueCatalog.dropFunction(functionPath, false);
        
        // Check function no longer exists
        assertFalse(glueCatalog.functionExists(functionPath));
    }

    /**
     * Test drop function with ignoreIfNotExists flag
     */
    @Test
    public void testDropFunctionWithIgnoreFlag() throws DatabaseAlreadyExistException, 
            DatabaseNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Test dropFunction with ignoreIfNotExists=true
        assertDoesNotThrow(() -> {
            glueCatalog.dropFunction(
                    new ObjectPath(databaseName, "nonExistingFunction"), 
                    true
            );
        });
    }

    /**
     * Test function exists edge cases
     */
    @Test
    public void testFunctionExistsEdgeCases() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        
        // Act & Assert
        // Function in non-existing database
        assertFalse(glueCatalog.functionExists(new ObjectPath("nonExistingDb", "testFunction")));
    }

    //-------------------------------------------------------------------------
    // Error Handling Tests
    //-------------------------------------------------------------------------

    /**
     * Test error handling for null parameters
     */
    @Test
    public void testNullParameterHandling() {
        // Act & Assert
        assertThrows(NullPointerException.class, () -> {
            glueCatalog.createTable(null, null, false);
        });
        
        assertThrows(NullPointerException.class, () -> {
            glueCatalog.createTable(new ObjectPath("db", "table"), null, false);
        });
        
        assertThrows(NullPointerException.class, () -> {
            glueCatalog.normalize(null);
        });
    }
}
