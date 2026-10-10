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

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Unit tests for the GlueDatabaseOperations class. These tests verify the functionality for
 * database operations such as create, drop, get, and list in the AWS Glue service.
 */
@ExtendWith(RealGlueCleanupExtension.class)
class GlueDatabaseOperationsTest {

    private GlueClient glueClient;
    private GlueDatabaseOperator glueDatabaseOperations;
    private String db1;
    private String db2;
    private String testDbUpper;

    @BeforeEach
    void setUp() {
        glueClient = GlueTestClientFactory.createClient();
        glueDatabaseOperations = new GlueDatabaseOperator(glueClient, "testCatalog");
        db1 = GlueTestClientFactory.uniqueName("db1");
        db2 = GlueTestClientFactory.uniqueName("db2");
        testDbUpper = GlueTestClientFactory.uniqueName("TestDB");
    }

    @Test
    void testCreateDatabase() throws DatabaseAlreadyExistException, DatabaseNotExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase(db1, catalogDatabase);
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists(db1));
        Assertions.assertEquals(
                "test", glueDatabaseOperations.getDatabase(db1).getDescription().orElse(null));
    }

    @Test
    void testCreateDatabaseWithUppercaseLetters()
            throws DatabaseAlreadyExistException, DatabaseNotExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        // Uppercase letters should now be accepted with case preservation
        Assertions.assertDoesNotThrow(
                () -> glueDatabaseOperations.createDatabase(testDbUpper, catalogDatabase));

        // Verify database was created and exists
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists(testDbUpper));

        // Verify the database can be retrieved
        CatalogDatabase retrieved = glueDatabaseOperations.getDatabase(testDbUpper);
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals("test", retrieved.getDescription().orElse(null));
    }

    @Test
    void testCreateDatabaseWithHyphens() {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class,
                        () -> glueDatabaseOperations.createDatabase("db-1", catalogDatabase));
        Assertions.assertTrue(
                exception.getMessage().contains("letters, numbers, and underscores"),
                "Exception message should mention allowed characters");
    }

    @Test
    void testCreateDatabaseWithSpecialCharacters() {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        CatalogException exception =
                Assertions.assertThrows(
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
        glueDatabaseOperations.createDatabase(db1, catalogDatabase);
        Assertions.assertThrows(
                DatabaseAlreadyExistException.class,
                () -> glueDatabaseOperations.createDatabase(db1, catalogDatabase));
    }

    @Test
    void testCreateDatabaseInvalidInput() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeClient()
                .setNextException(
                        InvalidInputException.builder().message("Invalid database name").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase(db1, catalogDatabase));
    }

    @Test
    void testCreateDatabaseResourceLimitExceeded() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeClient()
                .setNextException(
                        ResourceNumberLimitExceededException.builder()
                                .message("Resource limit exceeded")
                                .build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase(db1, catalogDatabase));
    }

    @Test
    void testCreateDatabaseTimeout() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(
                CatalogException.class,
                () -> glueDatabaseOperations.createDatabase(db1, catalogDatabase));
    }

    @Test
    void testDropDatabase() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "Description");
        glueDatabaseOperations.createDatabase(db1, catalogDatabase);
        Assertions.assertDoesNotThrow(() -> glueDatabaseOperations.dropGlueDatabase(db1));
        Assertions.assertFalse(glueDatabaseOperations.glueDatabaseExists(db1));
    }

    @Test
    void testDropDatabaseNotFound() {
        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> glueDatabaseOperations.dropGlueDatabase(db1));
    }

    @Test
    void testDropDatabaseInvalidInput() {
        fakeClient()
                .setNextException(
                        InvalidInputException.builder().message("Invalid database name").build());
        Assertions.assertThrows(
                CatalogException.class, () -> glueDatabaseOperations.dropGlueDatabase(db1));
    }

    @Test
    void testDropDatabaseTimeout() {
        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(
                CatalogException.class, () -> glueDatabaseOperations.dropGlueDatabase(db1));
    }

    @Test
    void testListDatabases() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase1 = new CatalogDatabaseImpl(Collections.emptyMap(), "test1");
        CatalogDatabase catalogDatabase2 = new CatalogDatabaseImpl(Collections.emptyMap(), "test2");
        glueDatabaseOperations.createDatabase(db1, catalogDatabase1);
        glueDatabaseOperations.createDatabase(db2, catalogDatabase2);

        List<String> databaseNames = glueDatabaseOperations.listDatabases();
        Assertions.assertTrue(databaseNames.contains(db1));
        Assertions.assertTrue(databaseNames.contains(db2));
    }

    @Test
    void testListDatabasesTimeout() {
        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        Assertions.assertThrows(
                CatalogException.class, () -> glueDatabaseOperations.listDatabases());
    }

    @Test
    void testListDatabasesResourceLimitExceeded() {
        fakeClient()
                .setNextException(
                        ResourceNumberLimitExceededException.builder()
                                .message("Resource limit exceeded")
                                .build());
        Assertions.assertThrows(
                CatalogException.class, () -> glueDatabaseOperations.listDatabases());
    }

    @Test
    void testGetDatabase() throws DatabaseNotExistException, DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "comment");
        glueDatabaseOperations.createDatabase(db1, catalogDatabase);
        CatalogDatabase retrievedDatabase = glueDatabaseOperations.getDatabase(db1);
        Assertions.assertNotNull(retrievedDatabase);
        Assertions.assertEquals("comment", retrievedDatabase.getComment());
    }

    @Test
    void testGetDatabaseNotFound() {
        Assertions.assertThrows(
                DatabaseNotExistException.class, () -> glueDatabaseOperations.getDatabase(db1));
    }

    @Test
    void testGetDatabaseInvalidInput() {
        fakeClient()
                .setNextException(
                        InvalidInputException.builder().message("Invalid database name").build());
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class, () -> glueDatabaseOperations.getDatabase(db1));
        Assertions.assertEquals(
                "Invalid database name '" + db1 + "': Invalid database name",
                exception.getMessage());
        Assertions.assertInstanceOf(InvalidInputException.class, exception.getCause());
    }

    @Test
    void testGetDatabaseTimeout() {
        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class, () -> glueDatabaseOperations.getDatabase(db1));
        Assertions.assertEquals(
                "Timed out looking up database '" + db1 + "'", exception.getMessage());
        Assertions.assertInstanceOf(OperationTimeoutException.class, exception.getCause());
    }

    @Test
    void testGlueDatabaseExists() throws DatabaseAlreadyExistException {
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(Collections.emptyMap(), "test");
        glueDatabaseOperations.createDatabase(db1, catalogDatabase);
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists(db1));
    }

    @Test
    void testGlueDatabaseDoesNotExist() {
        Assertions.assertFalse(glueDatabaseOperations.glueDatabaseExists("nonExistentDB"));
    }

    @Test
    void testGlueDatabaseExistsInvalidInput() {
        fakeClient()
                .setNextException(
                        InvalidInputException.builder().message("Invalid database name").build());
        // A Glue error is not "the database does not exist": it must surface, with the
        // database named and the Glue error preserved, instead of being swallowed as false.
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class,
                        () -> glueDatabaseOperations.glueDatabaseExists(db1));
        Assertions.assertTrue(
                exception.getMessage().contains("Invalid database name '" + db1 + "'"),
                exception.getMessage());
        Assertions.assertInstanceOf(InvalidInputException.class, exception.getCause());
    }

    @Test
    void testGlueDatabaseExistsTimeout() {
        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        // A timeout must not be reported as "database does not exist".
        CatalogException exception =
                Assertions.assertThrows(
                        CatalogException.class,
                        () -> glueDatabaseOperations.glueDatabaseExists(db1));
        Assertions.assertTrue(
                exception.getMessage().contains("Timed out looking up database '" + db1 + "'"),
                exception.getMessage());
        Assertions.assertInstanceOf(OperationTimeoutException.class, exception.getCause());
    }

    @Test
    void testCaseSensitivityInDatabaseOperations() throws Exception {
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(Collections.emptyMap(), "test_database");

        // Test creating databases with different cases - use unique names to avoid conflicts
        String lowerCaseName = GlueTestClientFactory.uniqueName("testdb_case_lower");
        String mixedCaseName = GlueTestClientFactory.uniqueName("TestDB_Case_Mixed");

        // Create database with lowercase name
        glueDatabaseOperations.createDatabase(lowerCaseName, catalogDatabase);
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists(lowerCaseName));

        // Create database with mixed case name - should be allowed now with case preservation
        CatalogDatabase catalogDatabase2 =
                new CatalogDatabaseImpl(Collections.emptyMap(), "mixed_case_database");
        Assertions.assertDoesNotThrow(
                () -> glueDatabaseOperations.createDatabase(mixedCaseName, catalogDatabase2));
        Assertions.assertTrue(glueDatabaseOperations.glueDatabaseExists(mixedCaseName));

        // Verify both databases exist and can be retrieved
        CatalogDatabase retrievedLower = glueDatabaseOperations.getDatabase(lowerCaseName);
        Assertions.assertEquals("test_database", retrievedLower.getDescription().orElse(null));

        CatalogDatabase retrievedMixed = glueDatabaseOperations.getDatabase(mixedCaseName);
        Assertions.assertEquals(
                "mixed_case_database", retrievedMixed.getDescription().orElse(null));

        // List databases should show both with original case preserved
        List<String> databases = glueDatabaseOperations.listDatabases();
        Assertions.assertTrue(
                databases.contains(lowerCaseName), "Lowercase database should appear in list");
        Assertions.assertTrue(
                databases.contains(mixedCaseName),
                "Mixed-case database should appear with original case");
    }

    private FakeGlueClient fakeClient() {
        assumeThat(glueClient)
                .as("Fault-injection tests require the in-memory FakeGlueClient")
                .isInstanceOf(FakeGlueClient.class);
        return (FakeGlueClient) glueClient;
    }
}
