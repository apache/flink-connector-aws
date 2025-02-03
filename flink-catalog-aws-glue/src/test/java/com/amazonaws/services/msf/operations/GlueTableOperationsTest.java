package com.amazonaws.services.msf.operations;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.glue.model.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GlueTableOperationsTest {

    private FakeGlueClient fakeGlueClient;
    private GlueTableOperations glueTableOperations;

    @BeforeEach
    void setUp() {
        // Reset the state of FakeGlueClient before each test
        FakeGlueClient.reset();

        fakeGlueClient = new FakeGlueClient();
        glueTableOperations = new GlueTableOperations(fakeGlueClient, "testCatalog");
    }

    @Test
    void testTableExists() throws CatalogException {
        // Arrange: Create a table
        String tableName = "testTable";
        String databaseName = "testDB";
        StorageDescriptor storageDescriptor = StorageDescriptor.builder().build();
        TableInput tableInput = TableInput.builder()
                        .name(tableName).storageDescriptor(storageDescriptor)
                .parameters(Collections.emptyMap())
                .tableType("EXTERNAL_TABLE")
                .build();

        glueTableOperations.createTable(databaseName, tableInput);

        // Act & Assert: Check if the table exists
        assertTrue(glueTableOperations.tableExists(databaseName, tableName), "Table should exist");
    }

    @Test
    void testTableDoesNotExist() {
        // Act & Assert: Check if a non-existent table exists
        assertFalse(glueTableOperations.tableExists("testDB", "nonExistentTable"), "Table should not exist");
    }

    @Test
    void testListTables() throws CatalogException {
        // Arrange: Create tables
        String databaseName = "testDB";

        // Creating StorageDescriptors using builder pattern
        StorageDescriptor storageDescriptor1 = StorageDescriptor.builder().build();
        StorageDescriptor storageDescriptor2 = StorageDescriptor.builder().build();

        TableInput tableInput1 = TableInput.builder()
                .name("table1")
                .storageDescriptor(storageDescriptor1)
                .parameters(Collections.emptyMap())
                .tableType("EXTERNAL_TABLE")
                .build();

        TableInput tableInput2 = TableInput.builder()
                .name("table2")
                .storageDescriptor(storageDescriptor2)
                .parameters(Collections.emptyMap())
                .tableType("EXTERNAL_TABLE")
                .build();

        glueTableOperations.createTable(databaseName, tableInput1);
        glueTableOperations.createTable(databaseName, tableInput2);

        // Act: List tables
        List<String> tableNames = glueTableOperations.listTables(databaseName);

        // Assert: Verify that tables are listed
        assertTrue(tableNames.contains("table1"), "Table table1 should be in the list");
        assertTrue(tableNames.contains("table2"), "Table table2 should be in the list");
    }

    @Test
    void testCreateTable() throws CatalogException, TableNotExistException {
        // Arrange: Prepare a table input
        String tableName = "newTable";
        String databaseName = "testDB";

        // Creating StorageDescriptor using builder pattern
        StorageDescriptor storageDescriptor = StorageDescriptor.builder().build();

        // Create TableInput using builder pattern
        TableInput tableInput = TableInput.builder()
                .name(tableName)
                .storageDescriptor(storageDescriptor)
                .parameters(Collections.emptyMap())
                .tableType("EXTERNAL_TABLE")
                .build();

        // Act: Create the table
        glueTableOperations.createTable(databaseName, tableInput);

        // Assert: Verify table is created
        assertTrue(glueTableOperations.tableExists(databaseName, tableName), "Table should exist");
    }
    @Test
    void testGetGlueTable() throws TableNotExistException, CatalogException {
        // Arrange: Create a table
        String tableName = "testTable";
        String databaseName = "testDB";

        // Creating StorageDescriptor using builder pattern
        StorageDescriptor storageDescriptor = StorageDescriptor.builder().build();

        // Create TableInput using builder
        TableInput tableInput = TableInput.builder()
                .name(tableName)
                .storageDescriptor(storageDescriptor)
                .parameters(Collections.emptyMap())
                .tableType("EXTERNAL_TABLE")
                .build();

        glueTableOperations.createTable(databaseName, tableInput);

        // Act: Retrieve the table
        Table retrievedTable = glueTableOperations.getGlueTable(databaseName, tableName);

        // Assert: Check that the table is retrieved correctly
        assertNotNull(retrievedTable, "Table should be retrieved successfully");
        assertEquals(tableName, retrievedTable.name(), "Table name should match");
    }

    @Test
    void testGetTableNotFound() {
        // Act & Assert: Trying to get a non-existent table should throw TableNotExistException
        assertThrows(TableNotExistException.class, () -> glueTableOperations.getGlueTable("testDB", "nonExistentTable"));
    }

    @Test
    void testDropTable() throws CatalogException {
        // Arrange: Create a table
        String tableName = "testTable";
        String databaseName = "testDB";

        // Creating StorageDescriptor using builder pattern
        StorageDescriptor storageDescriptor = StorageDescriptor.builder().build();

        // Create TableInput using builder
        TableInput tableInput = TableInput.builder()
                .name(tableName)
                .storageDescriptor(storageDescriptor)
                .parameters(Collections.emptyMap())
                .tableType("EXTERNAL_TABLE")
                .build();

        glueTableOperations.createTable(databaseName, tableInput);

        // Act: Drop the table
        glueTableOperations.dropTable(databaseName, tableName);

        // Assert: Verify the table is dropped
        assertFalse(glueTableOperations.tableExists(databaseName,tableName), "Table should be dropped");
    }

    @Test
    void testDropTableNotFound() {
        // Act & Assert: Trying to drop a non-existent table should throw CatalogException
        assertThrows(CatalogException.class, () -> glueTableOperations.dropTable("testDB", "nonExistentTable"));
    }
}
