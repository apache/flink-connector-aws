package org.apache.flink.table.catalog.glue.operations;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class GlueTableOperationsTest {

    private static final String CATALOG_NAME = "test-catalog";
    private static final String DATABASE_NAME = "test-db";
    private static final String TABLE_NAME = "test-table";

    private FakeGlueClient fakeGlueClient;
    private GlueTableOperations glueTableOperations;

    @BeforeEach
    void setUp() {
        FakeGlueClient.reset();
        fakeGlueClient = new FakeGlueClient();
        glueTableOperations = new GlueTableOperations(fakeGlueClient, CATALOG_NAME);
    }

    @Test
    void testTableExists() {
        // Create a test table
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();
        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(tableInput)
                .build());

        assertTrue(glueTableOperations.glueTableExists(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testTableExistsWhenNotFound() {
        assertFalse(glueTableOperations.glueTableExists(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testListTables() {
        // Create test tables
        TableInput table1 = TableInput.builder().name("table1").build();
        TableInput table2 = TableInput.builder().name("table2").build();
        
        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(table1)
                .build());
        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(table2)
                .build());

        List<String> result = glueTableOperations.listTables(DATABASE_NAME);
        assertEquals(2, result.size());
        assertTrue(result.contains("table1"));
        assertTrue(result.contains("table2"));
    }

    @Test
    void testListTablesWithInvalidInput() {
        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid input").build());
        assertThrows(CatalogException.class, () -> glueTableOperations.listTables(DATABASE_NAME));
    }

    @Test
    void testCreateTable() {
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();

        assertDoesNotThrow(() -> glueTableOperations.createTable(DATABASE_NAME, tableInput));
        assertTrue(glueTableOperations.glueTableExists(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testCreateTableAlreadyExists() {
        // First create the table
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();
        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(tableInput)
                .build());

        // Try to create it again
        assertThrows(CatalogException.class, () -> glueTableOperations.createTable(DATABASE_NAME, tableInput));
    }

    @Test
    void testCreateTableInvalidInput() {
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();

        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid input").build());
        assertThrows(CatalogException.class, () -> glueTableOperations.createTable(DATABASE_NAME, tableInput));
    }

    @Test
    void testCreateTableResourceLimitExceeded() {
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();

        fakeGlueClient.setNextException(ResourceNumberLimitExceededException.builder().message("Resource limit exceeded").build());
        assertThrows(CatalogException.class, () -> glueTableOperations.createTable(DATABASE_NAME, tableInput));
    }

    @Test
    void testCreateTableTimeout() {
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();

        fakeGlueClient.setNextException(OperationTimeoutException.builder().message("Operation timed out").build());
        assertThrows(CatalogException.class, () -> glueTableOperations.createTable(DATABASE_NAME, tableInput));
    }

    @Test
    void testGetGlueTable() throws TableNotExistException {
        // Create a test table
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();
        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(tableInput)
                .build());

        Table result = glueTableOperations.getGlueTable(DATABASE_NAME, TABLE_NAME);
        assertEquals(TABLE_NAME, result.name());
    }

    @Test
    void testGetGlueTableNotFound() {
        assertThrows(TableNotExistException.class, () -> glueTableOperations.getGlueTable(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testGetGlueTableInvalidInput() {
        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid input").build());
        assertThrows(CatalogException.class, () -> glueTableOperations.getGlueTable(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testDropTable() {
        // First create the table
        TableInput tableInput = TableInput.builder()
                .name(TABLE_NAME)
                .build();
        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(tableInput)
                .build());

        // Then drop it
        assertDoesNotThrow(() -> glueTableOperations.dropTable(DATABASE_NAME, TABLE_NAME));
        assertFalse(glueTableOperations.glueTableExists(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testDropTableNotFound() {
        assertThrows(TableNotExistException.class, () -> glueTableOperations.dropTable(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testDropTableInvalidInput() {
        fakeGlueClient.setNextException(InvalidInputException.builder().message("Invalid input").build());
        assertThrows(CatalogException.class, () -> glueTableOperations.dropTable(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testDropTableTimeout() {
        fakeGlueClient.setNextException(OperationTimeoutException.builder().message("Operation timed out").build());
        assertThrows(CatalogException.class, () -> glueTableOperations.dropTable(DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testCreateView() {
        TableInput viewInput = TableInput.builder()
                .name("test-view")
                .tableType("VIEW")
                .viewOriginalText("SELECT * FROM source_table")
                .viewExpandedText("SELECT * FROM database.source_table")
                .build();

        assertDoesNotThrow(() -> glueTableOperations.createTable(DATABASE_NAME, viewInput));
        assertTrue(glueTableOperations.glueTableExists(DATABASE_NAME, "test-view"));
    }

    @Test
    void testGetView() throws TableNotExistException {
        // First create a view
        TableInput viewInput = TableInput.builder()
                .name("test-view")
                .tableType("VIEW")
                .viewOriginalText("SELECT * FROM source_table")
                .viewExpandedText("SELECT * FROM database.source_table")
                .build();

        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(viewInput)
                .build());

        Table result = glueTableOperations.getGlueTable(DATABASE_NAME, "test-view");
        assertEquals("test-view", result.name());
        assertEquals("VIEW", result.tableType());
        assertEquals("SELECT * FROM source_table", result.viewOriginalText());
        assertEquals("SELECT * FROM database.source_table", result.viewExpandedText());
    }

    @Test
    void testCreateViewAlreadyExists() {
        // First create the view
        TableInput viewInput = TableInput.builder()
                .name("test-view")
                .tableType("VIEW")
                .viewOriginalText("SELECT * FROM source_table")
                .viewExpandedText("SELECT * FROM database.source_table")
                .build();

        fakeGlueClient.createTable(CreateTableRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableInput(viewInput)
                .build());

        // Try to create it again
        assertThrows(CatalogException.class, () -> glueTableOperations.createTable(DATABASE_NAME, viewInput));
    }
}
