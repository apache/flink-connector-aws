package org.apache.flink.table.catalog.glue.operator;

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
    private GlueDatabaseOperator glueDatabaseOperations;

    @BeforeEach
    void setUp() {
        FakeGlueClient.reset();
        fakeGlueClient = new FakeGlueClient();
        glueDatabaseOperations = new GlueDatabaseOperator(fakeGlueClient, "testCatalog");
    }

    @Test
    void testCreateDatabase() throws DatabaseAlreadyExistException, DatabaseNotExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists("db1"));
        Assertions.assertEquals("test", glueDatabaseOperations.getDatabase("db1").getDescription().orElse(null));
    }

    @Test
    void testCreateDatabaseWithUppercaseLetters() throws DatabaseAlreadyExistException, DatabaseNotExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        // Uppercase letters should now be accepted with case preservation
        Assertions.assertDoesNotThrow(() -> glueDatabaseOperations.createDatabase("TestDB", catalogDatabase));

        // Verify database was created and exists
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists("TestDB"));

        // Verify the database can be retrieved
        CatalogDatabase retrieved = glueDatabaseOperations.getDatabase("TestDB");
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals("test", retrieved.getDescription().orElse(null));
    }

    @Test
    void testCreateDatabaseWithHyphens() {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        CatalogException exception = Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase("db-1", catalogDatabase));
        Assertions.assertTrue(
                exception.getMessage().contains("letters, numbers, and underscores"),
                "Exception message should mention allowed characters");
    }

    @Test
    void testCreateDatabaseWithSpecialCharacters() {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        CatalogException exception = Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase("db.1", catalogDatabase));
        Assertions.assertTrue(
                exception.getMessage().contains("letters, numbers, and underscores"),
                "Exception message should mention allowed characters");
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
        // exists() methods should return false on errors, not throw exceptions
        Assertions.assertFalse(glueDatabaseOperations.glueDatabaseExists("db1"));
    }

    @Test
    void testGlueDatabaseExistsTimeout() {
        fakeGlueClient.setNextException(
                OperationTimeoutException.builder().message("Operation timed out").build());
        // exists() methods should return false on errors, not throw exceptions
        Assertions.assertFalse(glueDatabaseOperations.glueDatabaseExists("db1"));
    }

    @Test
    void testCaseSensitivityInDatabaseOperations() throws Exception {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test_database");

        // Test creating databases with different cases - use unique names to avoid conflicts
        String lowerCaseName = "testdb_case_lower";
        String mixedCaseName = "TestDB_Case_Mixed";

        // Create database with lowercase name
        glueDatabaseOperations.createDatabase(lowerCaseName, catalogDatabase);
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists(lowerCaseName));

        // Create database with mixed case name - should be allowed now with case preservation
        CatalogDatabase catalogDatabase2 = new CatalogDatabaseImpl(Collections.emptyMap(), "mixed_case_database");
        Assertions.assertDoesNotThrow(() -> glueDatabaseOperations.createDatabase(mixedCaseName, catalogDatabase2));
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists(mixedCaseName));

        // Verify both databases exist and can be retrieved
        CatalogDatabase retrievedLower = glueDatabaseOperations.getDatabase(lowerCaseName);
        Assertions.assertEquals("test_database", retrievedLower.getDescription().orElse(null));

        CatalogDatabase retrievedMixed = glueDatabaseOperations.getDatabase(mixedCaseName);
        Assertions.assertEquals("mixed_case_database", retrievedMixed.getDescription().orElse(null));

        // List databases should show both with original case preserved
        List<String> databases = glueDatabaseOperations.listDatabases();
        Assertions.assertTrue(databases.contains(lowerCaseName), "Lowercase database should appear in list");
        Assertions.assertTrue(databases.contains(mixedCaseName), "Mixed-case database should appear with original case");
    }
}
