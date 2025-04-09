package com.amazonaws.services.msf.operations;

import software.amazon.awssdk.services.glue.model.*;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.*;

public class FakeGlueClient implements GlueClient {

    // Static map to maintain database state across tests
    public static final Map<String, Database> databaseStore = new HashMap<>();
    public static Map<String, Map<String, Table>> tableStore = new HashMap<>(); // Map for tables by database name
    public static Map<String, Map<String, UserDefinedFunction>> functionStore = new HashMap<>(); // Map for functions by database name
    
    private RuntimeException nextException;

    public void setNextException(RuntimeException exception) {
        this.nextException = exception;
    }

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

    // Reset the database store before each test
    public static void reset() {
        databaseStore.clear();
        tableStore.clear();
        functionStore.clear();
    }

    @Override
    public GetDatabasesResponse getDatabases(GetDatabasesRequest request) {
        throwNextExceptionIfExists();
        List<Database> databases = new ArrayList<>(databaseStore.values());
        return GetDatabasesResponse.builder()
                .databaseList(databases)
                .build();
    }

    @Override
    public GetDatabaseResponse getDatabase(GetDatabaseRequest request) {
        throwNextExceptionIfExists();
        String databaseName = request.name();
        Database db = databaseStore.get(databaseName);
        if (db == null) {
            throw EntityNotFoundException.builder().message("Database not found: " + databaseName).build();
        }
        return GetDatabaseResponse.builder().database(db).build();
    }

    @Override
    public CreateDatabaseResponse createDatabase(CreateDatabaseRequest request) {
        throwNextExceptionIfExists();
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
        throwNextExceptionIfExists();
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
        
        Table.Builder tableBuilder = Table.builder()
                .name(tableName)
                .databaseName(databaseName)
                .tableType(request.tableInput().tableType())
                .parameters(request.tableInput().parameters())
                .storageDescriptor(request.tableInput().storageDescriptor())
                .description(request.tableInput().description());

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
        throwNextExceptionIfExists();
        String databaseName = request.databaseName();
        String tableName = request.name();
        
        if (!tableStore.containsKey(databaseName) || !tableStore.get(databaseName).containsKey(tableName)) {
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
        return GetTablesResponse.builder().tableList(new ArrayList<>(tableStore.get(databaseName).values())).build();
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
