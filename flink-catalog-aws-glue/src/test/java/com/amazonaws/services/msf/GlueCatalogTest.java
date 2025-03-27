package com.amazonaws.services.msf;

import com.amazonaws.services.msf.operations.FakeGlueClient;
import com.amazonaws.services.msf.operations.GlueDatabaseOperations;
import com.amazonaws.services.msf.operations.GlueTableOperations;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;


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

        glueCatalog = new GlueCatalog("glueCatalog",defaultDB,region,fakeGlueClient);
    }

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

    @Test
    public void testDatabaseAlreadyExists() throws DatabaseAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act & Assert
        assertThrows(DatabaseAlreadyExistException.class, () -> {
            glueCatalog.createDatabase(databaseName, catalogDatabase, false);
        });
    }


    @Test
    public void testGetDatabase() throws CatalogException, DatabaseAlreadyExistException, DatabaseNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        CatalogDatabase retrievedDatabase = glueCatalog.getDatabase(databaseName);

        // Assert
        assertNotNull(retrievedDatabase, "Database should be retrieved");
    }

    @Test
    public void testDatabaseNotExist() {
        // Act & Assert
        assertThrows(DatabaseNotExistException.class, () -> {
            glueCatalog.getDatabase("nonExistingDatabase");
        });
    }

    @Test
    public void testListDatabases() throws CatalogException, DatabaseAlreadyExistException {
        // Arrange
        String databaseName = "testDatabase";
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        List<String> databases = glueCatalog.listDatabases();

        // Assert
        assertTrue(databases.contains(databaseName), "Database should be listed");
    }

    @Test
    public void testCreateTable() throws CatalogException, DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException {
        // Arrange
        String databaseName = "testDatabase";
        String tableName = "testTable";

        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema =  ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable,resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Assert
        assertTrue(glueTableOperations.tableExists(databaseName, tableName), "Table should be created");
    }

    @Test
    public void testTableAlreadyExists() throws CatalogException, DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException {
        String databaseName = "testDatabase";
        String tableName = "testTable";

        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema =  ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable,resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act & Assert
        assertThrows(TableAlreadyExistException.class, () -> {
            glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);
        });
    }

    @Test
    public void testGetTable() throws CatalogException, DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
        String databaseName = "testDatabase";
        String tableName = "testTable";

        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema =  ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable,resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act
        CatalogTable retrievedTable = (CatalogTable) glueCatalog.getTable(new ObjectPath(databaseName, tableName));

        // Assert
        assertNotNull(retrievedTable, "Table should be retrieved");
    }

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

    @Test
    public void testDropTable() throws CatalogException, DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
        String databaseName = "testDatabase";
        String tableName = "testTable";

        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().build())
                .build();
        ResolvedSchema resolvedSchema =  ResolvedSchema.of();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable,resolvedSchema);

        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");

        glueCatalog.createDatabase(databaseName, catalogDatabase, false);

        // Act
        glueCatalog.createTable(new ObjectPath(databaseName, tableName), resolvedCatalogTable, false);

        // Act
        glueCatalog.dropTable(new ObjectPath(databaseName, tableName), false);

        // Assert
        assertFalse(glueTableOperations.tableExists(databaseName, tableName), "Table should be dropped");
    }
}
