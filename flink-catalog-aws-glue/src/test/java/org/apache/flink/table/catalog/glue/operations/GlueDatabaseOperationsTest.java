package org.apache.flink.table.catalog.glue.operations;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the GlueDatabaseOperations class.
 * These tests verify the functionality for database operations
 * such as create, drop, get, and list in the AWS Glue service.
 */
class GlueDatabaseOperationsTest {

    private FakeGlueClient fakeGlueClient;
    private GlueDatabaseOperations glueDatabaseOperations;

    @BeforeEach
    void setUp() {
        FakeGlueClient.reset();
        fakeGlueClient = new FakeGlueClient();
        glueDatabaseOperations = new GlueDatabaseOperations(fakeGlueClient, "testCatalog");
    }

    @Test
    void testCreateDatabase() throws DatabaseAlreadyExistException, DatabaseNotExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists("db1"));
        Assertions.assertEquals("test", glueDatabaseOperations.getDatabase("db1").getDescription().orElse(null));
    }

    @Test
    void testCreateDatabaseAlreadyExists() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        Assertions.assertThrows(
                DatabaseAlreadyExistException.class,
                () -> glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testCreateDatabaseInvalidInput() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeGlueClient.setNextException(
                InvalidInputException.builder().message("Invalid database name").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testCreateDatabaseResourceLimitExceeded() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeGlueClient.setNextException(
                ResourceNumberLimitExceededException.builder()
                        .message("Resource limit exceeded")
                        .build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testCreateDatabaseTimeout() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeGlueClient.setNextException(
                OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testDropDatabase() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        Assertions.assertDoesNotThrow(() -> glueDatabaseOperations.dropGlueDatabase("db1"));
        Assertions.assertFalse(glueDatabaseOperations.glueDatabaseExists("db1"));
    }

    @Test
    void testDropDatabaseNotFound() {
        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> glueDatabaseOperations.dropGlueDatabase("db1"));
    }

    @Test
    void testDropDatabaseInvalidInput() {
        fakeGlueClient.setNextException(
                InvalidInputException.builder().message("Invalid database name").build());
        Assertions.assertThrows(CatalogException.class, () -> glueDatabaseOperations.dropGlueDatabase("db1"));
    }

    @Test
    void testDropDatabaseTimeout() {
        fakeGlueClient.setNextException(
                OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(CatalogException.class, () -> glueDatabaseOperations.dropGlueDatabase("db1"));
    }

    @Test
    void testListDatabases() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase1 = new CatalogDatabaseImpl(Collections.emptyMap(), "test1");
        CatalogDatabase catalogDatabase2 = new CatalogDatabaseImpl(Collections.emptyMap(), "test2");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase1);
        glueDatabaseOperations.createDatabase("db2", catalogDatabase2);

        List<String> databaseNames = glueDatabaseOperations.listDatabases();
        Assertions.assertTrue(databaseNames.contains("db1"));
        Assertions.assertTrue(databaseNames.contains("db2"));
    }

    @Test
    void testListDatabasesTimeout() {
        fakeGlueClient.setNextException(
                OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(CatalogException.class, () -> glueDatabaseOperations.listDatabases());
    }

    @Test
    void testListDatabasesResourceLimitExceeded() {
        fakeGlueClient.setNextException(
                ResourceNumberLimitExceededException.builder()
                        .message("Resource limit exceeded")
                        .build());
        Assertions.assertThrows(CatalogException.class, () -> glueDatabaseOperations.listDatabases());
    }

    @Test
    void testGetDatabase() throws DatabaseNotExistException, DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "comment");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        CatalogDatabase retrievedDatabase = glueDatabaseOperations.getDatabase("db1");
        Assertions.assertNotNull(retrievedDatabase);
        Assertions.assertEquals("comment", retrievedDatabase.getComment());
    }

    @Test
    void testGetDatabaseNotFound() {
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueDatabaseOperations.getDatabase("db1"));
    }

    @Test
    void testGetDatabaseInvalidInput() {
        fakeGlueClient.setNextException(
                InvalidInputException.builder().message("Invalid database name").build());
        Assertions.assertThrows(CatalogException.class, () -> glueDatabaseOperations.getDatabase("db1"));
    }

    @Test
    void testGetDatabaseTimeout() {
        fakeGlueClient.setNextException(
                OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(CatalogException.class, () -> glueDatabaseOperations.getDatabase("db1"));
    }

    @Test
    void testGlueDatabaseExists() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists("db1"));
    }

    @Test
    void testGlueDatabaseDoesNotExist() {
        Assertions.assertFalse(glueDatabaseOperations.glueDatabaseExists("nonExistentDB"));
    }

    @Test
    void testGlueDatabaseExistsInvalidInput() {
        fakeGlueClient.setNextException(
                InvalidInputException.builder().message("Invalid database name").build());
        Assertions.assertThrows(
                CatalogException.class, () -> glueDatabaseOperations.glueDatabaseExists("db1"));
    }

    @Test
    void testGlueDatabaseExistsTimeout() {
        fakeGlueClient.setNextException(
                OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(
                CatalogException.class, () -> glueDatabaseOperations.glueDatabaseExists("db1"));
    }
}
