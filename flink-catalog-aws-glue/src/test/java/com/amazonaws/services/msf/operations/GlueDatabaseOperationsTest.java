package com.amazonaws.services.msf.operations;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import software.amazon.awssdk.services.glue.model.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

class GlueDatabaseOperationsTest {

    private FakeGlueClient fakeGlueClient;
    private GlueDatabaseOperations glueDatabaseOperations;

    @BeforeEach
    void setUp() {
        // Reset the state of the FakeGlueClient before each test
        FakeGlueClient.reset();
        fakeGlueClient = new FakeGlueClient();
        glueDatabaseOperations = new GlueDatabaseOperations(fakeGlueClient, "testCatalog");

    }

    @Test
    void testCreateDatabase() {
        // Arrange
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        // Act
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);

        // Assert
        assertTrue(FakeGlueClient.databaseStore.containsKey("db1"), "Database should be created");
        assertEquals("db1", FakeGlueClient.databaseStore.get("db1").name(), "DatabaseName should match");
    }

    @Test
    void testCreateDatabaseAlreadyExists() {
        // Arrange
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "Description");

        glueDatabaseOperations.createDatabase("db1", catalogDatabase);

        // Act & Assert
        assertThrows(AlreadyExistsException.class, () -> {
            glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        }, "Should throw AlreadyExistsException for an existing database");
    }

    @Test
    void testDropDatabase() {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "Description");

        glueDatabaseOperations.createDatabase("db1", catalogDatabase);

        // Act & Assert
        assertDoesNotThrow(() -> glueDatabaseOperations.dropGlueDatabase("db1"),
                "Should not throw any exception when dropping an existing database");

        assertNull(FakeGlueClient.databaseStore.get("db1"), "Database Shouldnt be there");

    }

    @Test
    void testDropDatabaseNotFound() {
        // Act & Assert
        assertThrows(DatabaseNotExistException.class, () -> glueDatabaseOperations.dropGlueDatabase("db1"),
                "Should throw DatabaseNotExistException when trying to drop a non-existent database");
    }

    @Test
    void testListDatabases() {
        // Arrange: Create a couple of databases
        CatalogDatabase catalogDatabase1 = new CatalogDatabaseImpl(Collections.emptyMap(), "test1");
        CatalogDatabase catalogDatabase2 = new CatalogDatabaseImpl(Collections.emptyMap(), "test2");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase1);
        glueDatabaseOperations.createDatabase("db2", catalogDatabase2);

        // Act: List databases
        List<String> databaseNames = glueDatabaseOperations.listDatabases();

        // Assert: Verify that the databases are listed correctly
        assertTrue(databaseNames.contains("db1"), "Database db1 should be in the list");
        assertTrue(databaseNames.contains("db2"), "Database db2 should be in the list");
    }

    @Test
    void testGetDatabase() throws DatabaseNotExistException {
        // Arrange: Create a database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "comment");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);

        // Act: Retrieve the database
        CatalogDatabase retrievedDatabase = glueDatabaseOperations.getDatabase("db1");

        // Assert: Check that the database is retrieved correctly
        assertNotNull(retrievedDatabase, "Database should be retrieved successfully");
        assertEquals("Optional[comment]", retrievedDatabase.getComment(), "comment should match");
    }

    @Test
    void testGetDatabaseNotFound() {
        // Act & Assert: Trying to get a non-existent database should throw DatabaseNotExistException
        assertThrows(DatabaseNotExistException.class, () -> glueDatabaseOperations.getDatabase("db1"),
                "Should throw DatabaseNotExistException when trying to get a non-existent database");
    }

    @Test
    void testGlueDatabaseExists() {
        // Arrange: Create a database
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);

        // Act & Assert: Check if the database exists
        assertTrue(glueDatabaseOperations.glueDatabaseExists("db1"), "Database should exist");
    }

    @Test
    void testGlueDatabaseDoesNotExist() {
        // Act & Assert: Check if a non-existent database exists
        assertFalse(glueDatabaseOperations.glueDatabaseExists("nonExistentDB"), "Database should not exist");
    }

}
