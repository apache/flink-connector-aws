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

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A mock implementation of the AWS Glue client for testing purposes. This class simulates the
 * behavior of the real AWS Glue service without making actual API calls. It manages in-memory
 * storage of databases, tables, and functions for testing the Glue catalog implementation.
 */
public class FakeGlueClient implements GlueClient {

    /** Page size used to emulate the real service's paginated list responses. */
    public static final int PAGE_SIZE = 50;

    // Static map to maintain database state across tests
    private static final Map<String, Database> DATABASE_STORE = new HashMap<>();
    private static Map<String, Map<String, Table>> tableStore =
            new HashMap<>(); // Map for tables by database name
    private static Map<String, Map<String, UserDefinedFunction>> functionStore =
            new HashMap<>(); // Map for functions by database name

    private RuntimeException nextException;

    /**
     * Sets an exception to be thrown on the next API call. This method is used to simulate AWS
     * service errors.
     *
     * @param exception The exception to throw on the next call.
     */
    public void setNextException(RuntimeException exception) {
        this.nextException = exception;
    }

    /** Throws the next exception if one is set, then clears it. */
    private void throwNextExceptionIfExists() {
        if (nextException != null) {
            RuntimeException ex = nextException;
            nextException = null;
            throw ex;
        }
    }

    @Override
    public void close() {
        // No actual AWS call needed, so leave it empty
    }

    @Override
    public String serviceName() {
        return "FakeGlue";
    }

    /**
     * Resets all stores to empty state. Call this method before each test to ensure a clean state.
     */
    public static void reset() {
        DATABASE_STORE.clear();
        tableStore.clear();
        functionStore.clear();
        partitionStore.clear();
    }

    // Partition store: database -> table -> partition values -> Partition
    private static final Map<String, Map<String, Map<List<String>, Partition>>> partitionStore =
            new HashMap<>();

    @Override
    public GetPartitionsResponse getPartitions(GetPartitionsRequest request) {
        throwNextExceptionIfExists();
        Map<List<String>, Partition> partitions =
                partitionStore
                        .getOrDefault(request.databaseName(), Collections.emptyMap())
                        .getOrDefault(request.tableName(), Collections.emptyMap());
        return GetPartitionsResponse.builder()
                .partitions(new ArrayList<>(partitions.values()))
                .build();
    }

    @Override
    public GetPartitionResponse getPartition(GetPartitionRequest request) {
        throwNextExceptionIfExists();
        Partition partition =
                partitionStore
                        .getOrDefault(request.databaseName(), Collections.emptyMap())
                        .getOrDefault(request.tableName(), Collections.emptyMap())
                        .get(request.partitionValues());
        if (partition == null) {
            throw EntityNotFoundException.builder().message("Partition not found").build();
        }
        return GetPartitionResponse.builder().partition(partition).build();
    }

    @Override
    public CreatePartitionResponse createPartition(CreatePartitionRequest request) {
        throwNextExceptionIfExists();
        PartitionInput input = request.partitionInput();
        // Real Glue refuses partitions on a table that has no storage location.
        Table table =
                tableStore
                        .getOrDefault(request.databaseName(), Collections.emptyMap())
                        .get(request.tableName());
        if (table != null
                && (table.storageDescriptor() == null
                        || table.storageDescriptor().location() == null
                        || table.storageDescriptor().location().isEmpty())) {
            throw InvalidInputException.builder()
                    .message("Cannot add partition to a table without location.")
                    .build();
        }
        Map<List<String>, Partition> tablePartitions =
                partitionStore
                        .computeIfAbsent(request.databaseName(), db -> new HashMap<>())
                        .computeIfAbsent(request.tableName(), tbl -> new HashMap<>());
        if (tablePartitions.containsKey(input.values())) {
            throw AlreadyExistsException.builder().message("Partition already exists").build();
        }
        Partition partition =
                Partition.builder()
                        .databaseName(request.databaseName())
                        .tableName(request.tableName())
                        .values(input.values())
                        .storageDescriptor(input.storageDescriptor())
                        .parameters(input.parameters())
                        .build();
        tablePartitions.put(input.values(), partition);
        return CreatePartitionResponse.builder().build();
    }

    @Override
    public UpdatePartitionResponse updatePartition(UpdatePartitionRequest request) {
        throwNextExceptionIfExists();
        Map<List<String>, Partition> tablePartitions =
                partitionStore
                        .getOrDefault(request.databaseName(), Collections.emptyMap())
                        .getOrDefault(request.tableName(), Collections.emptyMap());
        if (!tablePartitions.containsKey(request.partitionValueList())) {
            throw EntityNotFoundException.builder().message("Partition not found").build();
        }
        PartitionInput input = request.partitionInput();
        Partition updated =
                Partition.builder()
                        .databaseName(request.databaseName())
                        .tableName(request.tableName())
                        .values(input.values())
                        .storageDescriptor(input.storageDescriptor())
                        .parameters(input.parameters())
                        .build();
        tablePartitions.remove(request.partitionValueList());
        tablePartitions.put(input.values(), updated);
        return UpdatePartitionResponse.builder().build();
    }

    @Override
    public DeletePartitionResponse deletePartition(DeletePartitionRequest request) {
        throwNextExceptionIfExists();
        Map<List<String>, Partition> tablePartitions =
                partitionStore
                        .getOrDefault(request.databaseName(), Collections.emptyMap())
                        .getOrDefault(request.tableName(), Collections.emptyMap());
        if (tablePartitions.remove(request.partitionValues()) == null) {
            throw EntityNotFoundException.builder().message("Partition not found").build();
        }
        return DeletePartitionResponse.builder().build();
    }

    @Override
    public GetDatabasesResponse getDatabases(GetDatabasesRequest request) {
        throwNextExceptionIfExists();
        // Paginate like the real service so callers must handle nextToken correctly.
        List<Database> databases = new ArrayList<>(DATABASE_STORE.values());
        int start = request.nextToken() == null ? 0 : Integer.parseInt(request.nextToken());
        int end = Math.min(start + PAGE_SIZE, databases.size());
        return GetDatabasesResponse.builder()
                .databaseList(databases.subList(start, end))
                .nextToken(end < databases.size() ? String.valueOf(end) : null)
                .build();
    }

    @Override
    public GetDatabaseResponse getDatabase(GetDatabaseRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.name();
        Database db = DATABASE_STORE.get(databaseName);
        if (db == null) {
            throw EntityNotFoundException.builder()
                    .message("Database not found: " + databaseName)
                    .build();
        }
        return GetDatabaseResponse.builder().database(db).build();
    }

    @Override
    public CreateDatabaseResponse createDatabase(CreateDatabaseRequest request) {
        throwNextExceptionIfExists();
        DatabaseInput dbInput = request.databaseInput();
        String dbName = dbInput.name();

        // Check if the database already exists
        if (DATABASE_STORE.containsKey(dbName)) {
            throw AlreadyExistsException.builder()
                    .message("Database already exists: " + dbName)
                    .build();
        }

        // Create the database and add it to the store
        Database db =
                Database.builder()
                        .name(dbName)
                        .description(dbInput.description())
                        .parameters(dbInput.parameters())
                        .build();

        DATABASE_STORE.put(dbName, db);
        return CreateDatabaseResponse.builder().build(); // Simulate a successful creation
    }

    @Override
    public DeleteDatabaseResponse deleteDatabase(DeleteDatabaseRequest request) {
        throwNextExceptionIfExists();
        String dbName = request.name();

        // Check if the database exists
        if (!DATABASE_STORE.containsKey(dbName)) {
            throw EntityNotFoundException.builder()
                    .message("Database not found: " + dbName)
                    .build();
        }

        // Delete the database. Real Glue removes the database's tables, partitions and functions
        // with it, so the fake does the same.
        DATABASE_STORE.remove(dbName);
        tableStore.remove(dbName);
        partitionStore.remove(dbName);
        functionStore.remove(dbName);
        return DeleteDatabaseResponse.builder().build(); // Simulate a successful deletion
    }

    // Table-related methods
    @Override
    public GetTableResponse getTable(GetTableRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String tableName = request.name();

        if (!tableStore.containsKey(databaseName)) {
            throw EntityNotFoundException.builder().message("Table does not exist").build();
        }

        Table table = tableStore.get(databaseName).get(tableName);
        if (table == null) {
            throw EntityNotFoundException.builder().message("Table does not exist").build();
        }

        return GetTableResponse.builder().table(table).build();
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String tableName = request.tableInput().name();

        // Initialize the database's table store if it doesn't exist
        tableStore.computeIfAbsent(databaseName, k -> new HashMap<>());

        if (tableStore.get(databaseName).containsKey(tableName)) {
            throw AlreadyExistsException.builder().message("Table already exists").build();
        }

        Table.Builder tableBuilder =
                Table.builder()
                        .name(tableName)
                        .databaseName(databaseName)
                        .tableType(request.tableInput().tableType())
                        .parameters(request.tableInput().parameters())
                        .storageDescriptor(request.tableInput().storageDescriptor())
                        .owner(request.tableInput().owner())
                        .description(request.tableInput().description());

        // Persist partition keys like real Glue does (TableInput-level field)
        if (request.tableInput().hasPartitionKeys()) {
            tableBuilder.partitionKeys(request.tableInput().partitionKeys());
        }

        // Add view-specific fields if present
        if (request.tableInput().viewOriginalText() != null) {
            tableBuilder.viewOriginalText(request.tableInput().viewOriginalText());
        }
        if (request.tableInput().viewExpandedText() != null) {
            tableBuilder.viewExpandedText(request.tableInput().viewExpandedText());
        }

        Table table = tableBuilder.build();
        tableStore.get(databaseName).put(tableName, table);
        return CreateTableResponse.builder().build();
    }

    /**
     * Replaces the whole table definition like real Glue's UpdateTable: every field comes from the
     * new TableInput (a field absent from the input is cleared, not preserved).
     */
    @Override
    public UpdateTableResponse updateTable(UpdateTableRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String tableName = request.tableInput().name();
        Map<String, Table> tables = tableStore.get(databaseName);
        if (tables == null || !tables.containsKey(tableName)) {
            throw EntityNotFoundException.builder().message("Table does not exist").build();
        }
        Table.Builder tableBuilder =
                Table.builder()
                        .name(tableName)
                        .databaseName(databaseName)
                        .tableType(request.tableInput().tableType())
                        .parameters(request.tableInput().parameters())
                        .storageDescriptor(request.tableInput().storageDescriptor())
                        .owner(request.tableInput().owner())
                        .description(request.tableInput().description())
                        .viewOriginalText(request.tableInput().viewOriginalText())
                        .viewExpandedText(request.tableInput().viewExpandedText());
        if (request.tableInput().hasPartitionKeys()) {
            tableBuilder.partitionKeys(request.tableInput().partitionKeys());
        }
        tables.put(tableName, tableBuilder.build());
        return UpdateTableResponse.builder().build();
    }

    /**
     * Helper to ensure column parameters, including originalName, are preserved when creating
     * tables in the fake Glue client.
     */
    private StorageDescriptor preserveColumnParameters(StorageDescriptor storageDescriptor) {
        if (storageDescriptor == null || storageDescriptor.columns() == null) {
            return storageDescriptor;
        }

        List<Column> columns = storageDescriptor.columns();
        List<Column> columnsWithParams = new ArrayList<>();

        for (Column column : columns) {
            columnsWithParams.add(column);
        }

        return StorageDescriptor.builder()
                .columns(columnsWithParams)
                .location(storageDescriptor.location())
                .inputFormat(storageDescriptor.inputFormat())
                .outputFormat(storageDescriptor.outputFormat())
                .parameters(storageDescriptor.parameters())
                .build();
    }

    @Override
    public DeleteTableResponse deleteTable(DeleteTableRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String tableName = request.name();

        if (!tableStore.containsKey(databaseName)
                || !tableStore.get(databaseName).containsKey(tableName)) {
            throw EntityNotFoundException.builder().message("Table does not exist").build();
        }

        tableStore.get(databaseName).remove(tableName);
        return DeleteTableResponse.builder().build();
    }

    @Override
    public GetTablesResponse getTables(GetTablesRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        if (!tableStore.containsKey(databaseName)) {
            return GetTablesResponse.builder().tableList(Collections.emptyList()).build();
        }
        // Paginate like the real service so callers must handle nextToken correctly.
        List<Table> allTables = new ArrayList<>(tableStore.get(databaseName).values());
        int start = request.nextToken() == null ? 0 : Integer.parseInt(request.nextToken());
        int end = Math.min(start + PAGE_SIZE, allTables.size());
        return GetTablesResponse.builder()
                .tableList(allTables.subList(start, end))
                .nextToken(end < allTables.size() ? String.valueOf(end) : null)
                .build();
    }

    // Function-related methods
    @Override
    public CreateUserDefinedFunctionResponse createUserDefinedFunction(
            CreateUserDefinedFunctionRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String functionName = request.functionInput().functionName();

        // Check if the function already exists
        if (functionStore.containsKey(databaseName)
                && functionStore.get(databaseName).containsKey(functionName)) {
            throw AlreadyExistsException.builder()
                    .message("Function already exists: " + functionName)
                    .build();
        }

        UserDefinedFunction function =
                UserDefinedFunction.builder()
                        .functionName(functionName)
                        .className(request.functionInput().className())
                        .ownerName(request.functionInput().ownerName())
                        .ownerType(request.functionInput().ownerType())
                        .resourceUris(request.functionInput().resourceUris())
                        .databaseName(databaseName)
                        .catalogId(request.catalogId())
                        .build();

        // Add the function to the store
        functionStore
                .computeIfAbsent(databaseName, db -> new HashMap<>())
                .put(functionName, function);

        return CreateUserDefinedFunctionResponse.builder().build();
    }

    @Override
    public GetUserDefinedFunctionResponse getUserDefinedFunction(
            GetUserDefinedFunctionRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String functionName = request.functionName();

        // Check if the function exists
        if (!functionStore.containsKey(databaseName)
                || !functionStore.get(databaseName).containsKey(functionName)) {
            throw EntityNotFoundException.builder()
                    .message("Function not found: " + functionName)
                    .build();
        }

        UserDefinedFunction function = functionStore.get(databaseName).get(functionName);
        return GetUserDefinedFunctionResponse.builder().userDefinedFunction(function).build();
    }

    @Override
    public GetUserDefinedFunctionsResponse getUserDefinedFunctions(
            GetUserDefinedFunctionsRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();

        if (!functionStore.containsKey(databaseName)) {
            return GetUserDefinedFunctionsResponse.builder()
                    .userDefinedFunctions(Collections.emptyList())
                    .build();
        }

        List<UserDefinedFunction> functions =
                new ArrayList<>(functionStore.get(databaseName).values());
        return GetUserDefinedFunctionsResponse.builder().userDefinedFunctions(functions).build();
    }

    @Override
    public UpdateUserDefinedFunctionResponse updateUserDefinedFunction(
            UpdateUserDefinedFunctionRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String functionName = request.functionName();

        // Check if the function exists
        if (!functionStore.containsKey(databaseName)
                || !functionStore.get(databaseName).containsKey(functionName)) {
            throw EntityNotFoundException.builder()
                    .message("Function not found: " + functionName)
                    .build();
        }

        // Update the function
        UserDefinedFunction oldFunction = functionStore.get(databaseName).get(functionName);
        UserDefinedFunction newFunction =
                UserDefinedFunction.builder()
                        .functionName(functionName)
                        .className(request.functionInput().className())
                        .ownerName(request.functionInput().ownerName())
                        .ownerType(request.functionInput().ownerType())
                        .resourceUris(request.functionInput().resourceUris())
                        .databaseName(databaseName)
                        .catalogId(request.catalogId())
                        .build();

        functionStore.get(databaseName).put(functionName, newFunction);

        return UpdateUserDefinedFunctionResponse.builder().build();
    }

    @Override
    public DeleteUserDefinedFunctionResponse deleteUserDefinedFunction(
            DeleteUserDefinedFunctionRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String functionName = request.functionName();

        // Real Glue rejects deleting a function that does not exist.
        if (!functionStore.containsKey(databaseName)
                || functionStore.get(databaseName).remove(functionName) == null) {
            throw EntityNotFoundException.builder()
                    .message("Function not found: " + functionName)
                    .build();
        }

        return DeleteUserDefinedFunctionResponse.builder().build();
    }

    @Override
    public String toString() {
        return "FakeGlueClient{}";
    }
}
