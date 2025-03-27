package com.amazonaws.services.msf.operations;

import software.amazon.awssdk.services.glue.model.*;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.*;

public class FakeGlueClient implements GlueClient {

    // Static map to maintain database state across tests
    public static final Map<String, Database> databaseStore = new HashMap<>();
    public static Map<String, Map<String, Table>> tableStore = new HashMap<>(); // Map for tables by database name

    @Override
    public void close() {
        // No actual AWS call needed, so leave it empty
    }



    @Override
    public String serviceName() {
        return "FakeGlue";
    }

    // Reset the database store before each test
    public static void reset() {
        databaseStore.clear();
        tableStore.clear();
    }

    @Override
    public GetDatabasesResponse getDatabases(GetDatabasesRequest request) {
        List<Database> databases = new ArrayList<>(databaseStore.values());
        return GetDatabasesResponse.builder()
                .databaseList(databases)
                .build();
    }

    @Override
    public GetDatabaseResponse getDatabase(GetDatabaseRequest request) {
        String databaseName = request.name();
        Database db = databaseStore.get(databaseName);
        if (db == null) {
            throw EntityNotFoundException.builder().message("Database not found: " + databaseName).build();
        }
        return GetDatabaseResponse.builder().database(db).build();
    }

    @Override
    public CreateDatabaseResponse createDatabase(CreateDatabaseRequest request) {
        DatabaseInput dbInput = request.databaseInput();
        String dbName = dbInput.name();

        // Check if the database already exists
        if (databaseStore.containsKey(dbName)) {
            throw AlreadyExistsException.builder().message("Database already exists: " + dbName).build();
        }

        // Create the database and add it to the store
        Database db = Database.builder()
                .name(dbName)
                .description(dbInput.description())
                .parameters(dbInput.parameters())
                .build();

        databaseStore.put(dbName, db);
        return CreateDatabaseResponse.builder().build(); // Simulate a successful creation
    }

    @Override
    public DeleteDatabaseResponse deleteDatabase(DeleteDatabaseRequest request) {
        String dbName = request.name();

        // Check if the database exists
        if (!databaseStore.containsKey(dbName)) {
            throw EntityNotFoundException.builder().message("Database not found: " + dbName).build();
        }

        // Delete the database
        databaseStore.remove(dbName);
        return DeleteDatabaseResponse.builder().build(); // Simulate a successful deletion
    }

    // Table-related methods
    @Override
    public GetTableResponse getTable(GetTableRequest request) {
        Map<String, Table> tablesInDb = tableStore.get(request.databaseName());
        if (tablesInDb != null && tablesInDb.containsKey(request.name())) {
            Table table = tablesInDb.get(request.name());
            return GetTableResponse.builder().table(table).build();
        }
        throw EntityNotFoundException.builder().message("Table not found").build();
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest request) {
        TableInput tableInput = request.tableInput();
        Table table = Table.builder()
                .name(tableInput.name())
                .storageDescriptor(tableInput.storageDescriptor())
                .tableType("TABLE")
                .parameters(tableInput.parameters())
                .build();

        tableStore
                .computeIfAbsent(request.databaseName(), db -> new HashMap<>())
                .put(tableInput.name(), table);

        return CreateTableResponse.builder().build();
    }

    @Override
    public DeleteTableResponse deleteTable(DeleteTableRequest request) {
        Map<String, Table> tablesInDb = tableStore.get(request.databaseName());
        if (tablesInDb != null) {
            tablesInDb.remove(request.name());
        }
        return DeleteTableResponse.builder().build();
    }

    // Other methods like getTables (to list tables) can also be added similarly:
    @Override
    public GetTablesResponse getTables(GetTablesRequest request) {
        Map<String, Table> tablesInDb = tableStore.get(request.databaseName());
        if (tablesInDb != null) {
            return GetTablesResponse.builder()
                    .tableList(tablesInDb.values())
                    .build();
        }
        return GetTablesResponse.builder().build();
    }

    // Other required methods (can be left as empty or throwing UnsupportedOperationException)
    @Override
    public String toString() {
        return "FakeGlueClient{}";
    }
}
