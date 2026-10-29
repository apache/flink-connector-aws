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

package org.apache.flink.table.catalog.glue.operator;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Unit tests for the GlueTableOperations class. These tests verify that table operations such as
 * create, drop, get, and list are correctly executed against the AWS Glue service.
 */
@ExtendWith(RealGlueCleanupExtension.class)
public class GlueTableOperationsTest {

    private static final String CATALOG_NAME = "testcatalog";
    private String databaseName;
    private static final String TABLE_NAME = "testtable";

    private GlueClient glueClient;
    private GlueTableOperator glueTableOperations;

    @BeforeEach
    void setUp() {
        glueClient = GlueTestClientFactory.createClient();
        glueTableOperations = new GlueTableOperator(glueClient, CATALOG_NAME);
        databaseName = GlueTestClientFactory.uniqueName("testdb");
        // Real Glue rejects table operations in a non-existent database (the in-memory
        // fake is lenient), so the test database must actually exist.
        glueClient.createDatabase(b -> b.databaseInput(db -> db.name(databaseName)));
    }

    @Test
    void testTableExists() {
        // Create a test table
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(tableInput)
                        .build());

        Assertions.assertTrue(glueTableOperations.glueTableExists(databaseName, TABLE_NAME));
    }

    @Test
    void testTableExistsWhenNotFound() {
        Assertions.assertFalse(glueTableOperations.glueTableExists(databaseName, TABLE_NAME));
    }

    @Test
    void testListTables() {
        // Create test tables
        TableInput table1 = TableInput.builder().name("table1").build();
        TableInput table2 = TableInput.builder().name("table2").build();

        glueClient.createTable(
                CreateTableRequest.builder().databaseName(databaseName).tableInput(table1).build());
        glueClient.createTable(
                CreateTableRequest.builder().databaseName(databaseName).tableInput(table2).build());

        List<String> result = glueTableOperations.listTables(databaseName);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains("table1"));
        Assertions.assertTrue(result.contains("table2"));
    }

    @Test
    void testListTablesWithInvalidInput() {
        fakeClient()
                .setNextException(InvalidInputException.builder().message("Invalid input").build());
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class, () -> glueTableOperations.listTables(databaseName));
        Assertions.assertTrue(
                exception.getMessage().contains(databaseName)
                        && exception.getMessage().contains("Invalid input"),
                "Message must name the database and carry the Glue error: "
                        + exception.getMessage());
        Assertions.assertInstanceOf(InvalidInputException.class, exception.getCause());
    }

    @Test
    void testCreateTable() {
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();

        Assertions.assertDoesNotThrow(
                () -> glueTableOperations.createTable(databaseName, tableInput));
        Assertions.assertTrue(glueTableOperations.glueTableExists(databaseName, TABLE_NAME));
    }

    @Test
    void testCreateTableWithUppercaseLetters() throws Exception {
        // buildTableInput produces the Glue storage form: lowercase name + declared name kept
        // as a table parameter.
        TableInput tableInput =
                glueTableOperations.buildTableInput(
                        "TestTable",
                        CatalogBaseTable.TableKind.TABLE,
                        null,
                        Collections.emptyList(),
                        StorageDescriptor.builder().build(),
                        Collections.emptyMap());
        Assertions.assertEquals("testtable", tableInput.name());

        glueTableOperations.createTable(databaseName, tableInput);

        // Stored under the lowercase Glue name, but the declared case is preserved and every
        // spelling resolves to the same table.
        Table stored = glueTableOperations.getGlueTable(databaseName, "testtable");
        Assertions.assertEquals("testtable", stored.name());
        Assertions.assertEquals("TestTable", glueTableOperations.getOriginalTableName(stored));
        Assertions.assertTrue(glueTableOperations.glueTableExists(databaseName, "TestTable"));
        Assertions.assertTrue(glueTableOperations.glueTableExists(databaseName, "TESTTABLE"));
        Assertions.assertEquals(
                Collections.singletonList("TestTable"),
                glueTableOperations.listTables(databaseName));
    }

    @Test
    void testCreateTableWithHyphens() {
        TableInput tableInput = TableInput.builder().name("test-table").build();

        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class,
                        () -> glueTableOperations.createTable(databaseName, tableInput));

        Assertions.assertTrue(
                exception.getMessage().contains("letters, numbers, and underscores"),
                "Exception message should mention allowed characters");
    }

    @Test
    void testCreateTableWithSpecialCharacters() {
        TableInput tableInput = TableInput.builder().name("test.table").build();

        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class,
                        () -> glueTableOperations.createTable(databaseName, tableInput));

        Assertions.assertTrue(
                exception.getMessage().contains("letters, numbers, and underscores"),
                "Exception message should mention allowed characters");
    }

    @Test
    void testBuildTableInputWithInvalidName() {
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class,
                        () ->
                                glueTableOperations.buildTableInput(
                                        "Invalid-Name",
                                        CatalogBaseTable.TableKind.TABLE,
                                        null,
                                        null,
                                        null,
                                        null));

        Assertions.assertTrue(
                exception.getMessage().contains("letters, numbers, and underscores"),
                "Exception message should mention allowed characters");
    }

    @Test
    void testCreateTableAlreadyExists() {
        // First create the table
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(tableInput)
                        .build());

        // Try to create it again: the typed Flink exception, not a generic CatalogException.
        Assertions.assertThrows(
                TableAlreadyExistException.class,
                () -> glueTableOperations.createTable(databaseName, tableInput));
    }

    @Test
    void testCreateTableInvalidInput() {
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();

        fakeClient()
                .setNextException(InvalidInputException.builder().message("Invalid input").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueTableOperations.createTable(databaseName, tableInput));
    }

    @Test
    void testCreateTableResourceLimitExceeded() {
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();

        fakeClient()
                .setNextException(
                        ResourceNumberLimitExceededException.builder()
                                .message("Resource limit exceeded")
                                .build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueTableOperations.createTable(databaseName, tableInput));
    }

    @Test
    void testCreateTableTimeout() {
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();

        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueTableOperations.createTable(databaseName, tableInput));
    }

    @Test
    void testGetGlueTable() throws TableNotExistException {
        // Create a test table
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(tableInput)
                        .build());

        Table result = glueTableOperations.getGlueTable(databaseName, TABLE_NAME);
        Assertions.assertEquals(TABLE_NAME, result.name());
    }

    @Test
    void testGetGlueTableNotFound() {
        Assertions.assertThrows(
                TableNotExistException.class,
                () -> glueTableOperations.getGlueTable(databaseName, TABLE_NAME));
    }

    @Test
    void testGetGlueTableInvalidInput() {
        fakeClient()
                .setNextException(InvalidInputException.builder().message("Invalid input").build());
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class,
                        () -> glueTableOperations.getGlueTable(databaseName, TABLE_NAME));
        Assertions.assertTrue(
                exception.getMessage().contains("Invalid table reference")
                        && exception.getMessage().contains(databaseName + "." + TABLE_NAME),
                "Message must say what was invalid: " + exception.getMessage());
    }

    @Test
    void testDropTable() {
        // First create the table
        TableInput tableInput = TableInput.builder().name(TABLE_NAME).build();
        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(tableInput)
                        .build());

        // Then drop it
        Assertions.assertDoesNotThrow(
                () -> glueTableOperations.dropTable(databaseName, TABLE_NAME));
        Assertions.assertFalse(glueTableOperations.glueTableExists(databaseName, TABLE_NAME));
    }

    @Test
    void testDropTableNotFound() {
        Assertions.assertThrows(
                TableNotExistException.class,
                () -> glueTableOperations.dropTable(databaseName, TABLE_NAME));
    }

    @Test
    void testDropTableInvalidInput() {
        fakeClient()
                .setNextException(InvalidInputException.builder().message("Invalid input").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueTableOperations.dropTable(databaseName, TABLE_NAME));
    }

    @Test
    void testDropTableTimeout() {
        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueTableOperations.dropTable(databaseName, TABLE_NAME));
    }

    @Test
    void testCreateView() {
        TableInput viewInput =
                TableInput.builder()
                        .name("testview")
                        .tableType("VIEW")
                        .viewOriginalText("SELECT * FROM source_table")
                        .viewExpandedText("SELECT * FROM database.source_table")
                        .build();

        Assertions.assertDoesNotThrow(
                () -> glueTableOperations.createTable(databaseName, viewInput));
        Assertions.assertTrue(glueTableOperations.glueTableExists(databaseName, "testview"));
    }

    @Test
    void testGetView() throws TableNotExistException {
        // First create a view
        TableInput viewInput =
                TableInput.builder()
                        .name("testview")
                        .tableType("VIEW")
                        .viewOriginalText("SELECT * FROM source_table")
                        .viewExpandedText("SELECT * FROM database.source_table")
                        .build();

        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(viewInput)
                        .build());

        Table result = glueTableOperations.getGlueTable(databaseName, "testview");
        Assertions.assertEquals("testview", result.name());
        Assertions.assertEquals("VIEW", result.tableType());
        Assertions.assertEquals("SELECT * FROM source_table", result.viewOriginalText());
        Assertions.assertEquals("SELECT * FROM database.source_table", result.viewExpandedText());
    }

    @Test
    void testCreateViewAlreadyExists() {
        // First create the view
        TableInput viewInput =
                TableInput.builder()
                        .name("testview")
                        .tableType("VIEW")
                        .viewOriginalText("SELECT * FROM source_table")
                        .viewExpandedText("SELECT * FROM database.source_table")
                        .build();

        glueClient.createTable(
                CreateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(viewInput)
                        .build());

        // Try to create it again
        Assertions.assertThrows(
                TableAlreadyExistException.class,
                () -> glueTableOperations.createTable(databaseName, viewInput));
    }

    private FakeGlueClient fakeClient() {
        assumeThat(glueClient)
                .as("Fault-injection tests require the in-memory FakeGlueClient")
                .isInstanceOf(FakeGlueClient.class);
        return (FakeGlueClient) glueClient;
    }
}
