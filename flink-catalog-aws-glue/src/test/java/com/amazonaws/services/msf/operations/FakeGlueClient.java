package com.amazonaws.services.msf.operations;

import software.amazon.awssdk.services.glue.model.*;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.*;

public class FakeGlueClient implements GlueClient {

    // Static map to maintain database state across tests
    public static final Map<String, Database> databaseStore = new HashMap<>();
    public static Map<String, Map<String, Table>> tableStore = new HashMap<>(); // Map for tables by database name
    public static Map<String, Map<String, UserDefinedFunction>> functionStore = new HashMap<>(); // Map for functions by database name

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
        functionStore.clear();
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
        String databaseName = request.databaseName();
        TableInput tableInput = request.tableInput();
        String tableType = tableInput.tableType() != null ? tableInput.tableType() : "TABLE";
        
        Table.Builder tableBuilder = Table.builder()
                .name(tableInput.name())
                .storageDescriptor(preserveColumnParameters(tableInput.storageDescriptor()))
                .tableType(tableType)
                .parameters(tableInput.parameters());
        
        // Add view-specific properties if it's a view
        if ("VIEW".equalsIgnoreCase(tableType)) {
            tableBuilder
                .viewOriginalText(tableInput.viewOriginalText())
                .viewExpandedText(tableInput.viewExpandedText());
        }
        
        Table table = tableBuilder.build();

        tableStore
                .computeIfAbsent(databaseName, db -> new HashMap<>())
                .put(tableInput.name(), table);

        return CreateTableResponse.builder().build();
    }

    /**
     * Helper to ensure column parameters, including originalName, are preserved
     * when creating tables in the fake Glue client.
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
        Map<String, Table> tablesInDb = tableStore.get(request.databaseName());
        if (tablesInDb != null) {
            tablesInDb.remove(request.name());
        }
        return DeleteTableResponse.builder().build();
    }

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
    
    // Function-related methods
    @Override
    public CreateUserDefinedFunctionResponse createUserDefinedFunction(CreateUserDefinedFunctionRequest request) {
        String databaseName = request.databaseName();
        String functionName = request.functionInput().functionName();
        
        // Check if the function already exists
        if (functionStore.containsKey(databaseName) && 
            functionStore.get(databaseName).containsKey(functionName)) {
            throw AlreadyExistsException.builder()
                    .message("Function already exists: " + functionName)
                    .build();
        }
        
        UserDefinedFunction function = UserDefinedFunction.builder()
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
    public GetUserDefinedFunctionResponse getUserDefinedFunction(GetUserDefinedFunctionRequest request) {
        String databaseName = request.databaseName();
        String functionName = request.functionName();
        
        // Check if the function exists
        if (!functionStore.containsKey(databaseName) || 
            !functionStore.get(databaseName).containsKey(functionName)) {
            throw EntityNotFoundException.builder()
                    .message("Function not found: " + functionName)
                    .build();
        }
        
        UserDefinedFunction function = functionStore.get(databaseName).get(functionName);
        return GetUserDefinedFunctionResponse.builder()
                .userDefinedFunction(function)
                .build();
    }
    
    @Override
    public GetUserDefinedFunctionsResponse getUserDefinedFunctions(GetUserDefinedFunctionsRequest request) {
        String databaseName = request.databaseName();
        
        if (!functionStore.containsKey(databaseName)) {
            return GetUserDefinedFunctionsResponse.builder()
                    .userDefinedFunctions(Collections.emptyList())
                    .build();
        }
        
        List<UserDefinedFunction> functions = new ArrayList<>(functionStore.get(databaseName).values());
        return GetUserDefinedFunctionsResponse.builder()
                .userDefinedFunctions(functions)
                .build();
    }
    
    @Override
    public UpdateUserDefinedFunctionResponse updateUserDefinedFunction(UpdateUserDefinedFunctionRequest request) {
        String databaseName = request.databaseName();
        String functionName = request.functionName();
        
        // Check if the function exists
        if (!functionStore.containsKey(databaseName) || 
            !functionStore.get(databaseName).containsKey(functionName)) {
            throw EntityNotFoundException.builder()
                    .message("Function not found: " + functionName)
                    .build();
        }
        
        // Update the function
        UserDefinedFunction oldFunction = functionStore.get(databaseName).get(functionName);
        UserDefinedFunction newFunction = UserDefinedFunction.builder()
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
    public DeleteUserDefinedFunctionResponse deleteUserDefinedFunction(DeleteUserDefinedFunctionRequest request) {
        String databaseName = request.databaseName();
        String functionName = request.functionName();
        
        // Check if the function exists
        if (functionStore.containsKey(databaseName)) {
            functionStore.get(databaseName).remove(functionName);
        }
        
        return DeleteUserDefinedFunctionResponse.builder().build();
    }

    @Override
    public String toString() {
        return "FakeGlueClient{}";
    }
}
