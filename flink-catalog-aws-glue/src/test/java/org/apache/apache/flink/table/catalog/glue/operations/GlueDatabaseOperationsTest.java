package org.apache.apache.flink.table.catalog.glue.operations;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.*;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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
    void testCreateDatabase() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        assertTrue(FakeGlueClient.databaseStore.containsKey("db1"));
        assertEquals("db1", FakeGlueClient.databaseStore.get("db1").name());
    }

    @Test
    void testCreateDatabaseAlreadyExists() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        assertThrows(DatabaseAlreadyExistException.class, () -> 
            glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testCreateDatabaseInvalidInput() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid database name").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testCreateDatabaseResourceLimitExceeded() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeGlueClient.setNextException(ResourceNumberLimitExceededException.builder().message("Resource limit exceeded").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testCreateDatabaseTimeout() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeGlueClient.setNextException(OperationTimeoutException.builder().message("Operation timed out").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.createDatabase("db1", catalogDatabase));
    }

    @Test
    void testDropDatabase() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        assertDoesNotThrow(() -> glueDatabaseOperations.dropGlueDatabase("db1"));
        assertNull(FakeGlueClient.databaseStore.get("db1"));
    }

    @Test
    void testDropDatabaseNotFound() {
        assertThrows(DatabaseNotExistException.class, () -> 
            glueDatabaseOperations.dropGlueDatabase("db1"));
    }

    @Test
    void testDropDatabaseInvalidInput() {
        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid database name").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.dropGlueDatabase("db1"));
    }

    @Test
    void testDropDatabaseTimeout() {
        fakeGlueClient.setNextException(OperationTimeoutException.builder().message("Operation timed out").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.dropGlueDatabase("db1"));
    }

    @Test
    void testListDatabases() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase1 = new CatalogDatabaseImpl(Collections.emptyMap(), "test1");
        CatalogDatabase catalogDatabase2 = new CatalogDatabaseImpl(Collections.emptyMap(), "test2");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase1);
        glueDatabaseOperations.createDatabase("db2", catalogDatabase2);

        List<String> databaseNames = glueDatabaseOperations.listDatabases();
        assertTrue(databaseNames.contains("db1"));
        assertTrue(databaseNames.contains("db2"));
    }

    @Test
    void testListDatabasesTimeout() {
        fakeGlueClient.setNextException(OperationTimeoutException.builder().message("Operation timed out").build());
        assertThrows(CatalogException.class, () -> glueDatabaseOperations.listDatabases());
    }

    @Test
    void testListDatabasesResourceLimitExceeded() {
        fakeGlueClient.setNextException(ResourceNumberLimitExceededException.builder().message("Resource limit exceeded").build());
        assertThrows(CatalogException.class, () -> glueDatabaseOperations.listDatabases());
    }

    @Test
    void testGetDatabase() throws DatabaseNotExistException, DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "comment");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        CatalogDatabase retrievedDatabase = glueDatabaseOperations.getDatabase("db1");
        assertNotNull(retrievedDatabase);
        assertEquals("Optional[comment]", retrievedDatabase.getComment());
    }

    @Test
    void testGetDatabaseNotFound() {
        assertThrows(DatabaseNotExistException.class, () -> 
            glueDatabaseOperations.getDatabase("db1"));
    }

    @Test
    void testGetDatabaseInvalidInput() {
        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid database name").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.getDatabase("db1"));
    }

    @Test
    void testGetDatabaseTimeout() {
        fakeGlueClient.setNextException(OperationTimeoutException.builder().message("Operation timed out").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.getDatabase("db1"));
    }

    @Test
    void testGlueDatabaseExists() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase("db1", catalogDatabase);
        assertTrue(glueDatabaseOperations.glueDatabaseExists("db1"));
    }

    @Test
    void testGlueDatabaseDoesNotExist() {
        assertFalse(glueDatabaseOperations.glueDatabaseExists("nonExistentDB"));
    }

    @Test
    void testGlueDatabaseExistsInvalidInput() {
        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid database name").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.glueDatabaseExists("db1"));
    }

    @Test
    void testGlueDatabaseExistsTimeout() {
        fakeGlueClient.setNextException(OperationTimeoutException.builder().message("Operation timed out").build());
        assertThrows(CatalogException.class, () -> 
            glueDatabaseOperations.glueDatabaseExists("db1"));
    }
}
