package com.amazonaws.services.msf;

import com.amazonaws.services.msf.operations.GlueDatabaseOperations;
import com.amazonaws.services.msf.operations.GlueFunctionsOperations;
import com.amazonaws.services.msf.operations.GlueTableOperations;
import com.amazonaws.services.msf.util.GlueFunctionsUtil;
import com.amazonaws.services.msf.util.GlueTableUtils;
import com.amazonaws.services.msf.util.GlueTypeConverter;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;
import org.apache.flink.table.functions.FunctionIdentifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * GlueCatalog is an implementation of the Flink AbstractCatalog that interacts with AWS Glue.
 * This class allows Flink to perform various catalog operations such as creating, deleting, and retrieving
 * databases and tables from Glue. It encapsulates AWS Glue's API and provides a Flink-compatible interface.
 *
 * <p>This catalog uses GlueClient to interact with AWS Glue services, and operations related to databases and
 * tables are delegated to respective helper classes like GlueDatabaseOperations and GlueTableOperations.</p>
 */
public class GlueCatalog extends AbstractCatalog {

    private final GlueClient glueClient;
    private final GlueTypeConverter glueTypeConverter = new GlueTypeConverter();
    private final GlueDatabaseOperations glueDatabaseOperations;
    private final GlueTableOperations glueTableOperations;
    private final GlueFunctionsOperations glueFunctionsOperations;

    private final GlueTableUtils glueTableUtils;

    /**
     * Constructs a GlueCatalog.
     *
     * @param name            the name of the catalog
     * @param defaultDatabase the default database for the catalog
     * @param region          the AWS region to be used for Glue operations
     * @param glueClient      Glue Client so we can decide which one to use for testing
     */
    public GlueCatalog(String name, String defaultDatabase, String region, GlueClient glueClient) {
        super(name, defaultDatabase);

        // Initialize GlueClient in the constructor
        if (glueClient != null) {
            this.glueClient = glueClient;
        } else {
            // If no GlueClient is provided, initialize it using the default region
            this.glueClient = GlueClient.builder()
                    .region(Region.of(region)) // Or any default region you prefer
                    .build();
        }
        this.glueTableUtils = new GlueTableUtils(glueTypeConverter);
        this.glueDatabaseOperations = new GlueDatabaseOperations(glueClient, getName());
        this.glueTableOperations = new GlueTableOperations(glueClient, getName());
        this.glueFunctionsOperations = new GlueFunctionsOperations(glueClient, getName());

    }

    public GlueCatalog(String name, String defaultDatabase, String region) {
        super(name, defaultDatabase);

            // If no GlueClient is provided, initialize it using the default region
                this.glueClient = GlueClient.builder()
                    .region(Region.of(region)) // Or any default region you prefer
                    .build();
        this.glueTableUtils = new GlueTableUtils(glueTypeConverter);
        this.glueDatabaseOperations = new GlueDatabaseOperations(glueClient, getName());
        this.glueTableOperations = new GlueTableOperations(glueClient, getName());
        this.glueFunctionsOperations = new GlueFunctionsOperations(glueClient, getName());

    }

    /**
     * Opens the GlueCatalog and initializes necessary resources.
     *
     * @throws CatalogException if an error occurs during the opening process
     */
    @Override
    public void open() throws CatalogException {
        System.out.println("GlueClient is ready: " + glueClient);
    }

    /**
     * Closes the GlueCatalog and releases resources.
     *
     * @throws CatalogException if an error occurs during the closing process
     */
    @Override
    public void close() throws CatalogException {
        if (glueClient != null) {
            glueClient.close();
        }
    }

    /**
     * Lists all the databases available in the Glue catalog.
     *
     * @return a list of database names
     * @throws CatalogException if an error occurs while listing the databases
     */
    @Override
    public List<String> listDatabases() throws CatalogException {
        return glueDatabaseOperations.listDatabases();
    }

    /**
     * Retrieves a specific database by its name.
     *
     * @param databaseName the name of the database to retrieve
     * @return the database if found
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          if an error occurs while retrieving the database
     */
    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        boolean databaseExists = databaseExists(databaseName);
        if (!databaseExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return glueDatabaseOperations.getDatabase(databaseName);
    }

    /**
     * Checks if a database exists in Glue.
     *
     * @param databaseName the name of the database
     * @return true if the database exists, false otherwise
     * @throws CatalogException if an error occurs while checking the database
     */
    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return glueDatabaseOperations.glueDatabaseExists(databaseName);
    }

    /**
     * Creates a new database in Glue.
     *
     * @param databaseName    the name of the database to create
     * @param catalogDatabase the catalog database object containing database metadata
     * @param ifNotExists     flag indicating whether to ignore the error if the database already exists
     * @throws DatabaseAlreadyExistException if the database already exists and ifNotExists is false
     * @throws CatalogException              if an error occurs while creating the database
     */
    @Override
    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean ifNotExists)
            throws DatabaseAlreadyExistException, CatalogException {
        boolean exists = databaseExists(databaseName);

        // Throw exception if the database already exists and ifNotExists is false
        if (exists && !ifNotExists) {
            throw new DatabaseAlreadyExistException(getName(), databaseName);
        }

        // Create the database if it doesn't exist
        if (!exists) {
            glueDatabaseOperations.createDatabase(databaseName, catalogDatabase);
        }
    }

    /**
     * Drops an existing database in Glue.
     *
     * @param databaseName      the name of the database to drop
     * @param ignoreIfNotExists flag to ignore the exception if the database doesn't exist
     * @param cascade           flag indicating whether to cascade the operation to drop related objects
     * @throws DatabaseNotExistException if the database does not exist and ignoreIfNotExists is false
     * @throws DatabaseNotEmptyException if the database contains objects and cascade is false
     * @throws CatalogException          if an error occurs while dropping the database
     */
    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        // Check if the database exists before dropping it
        if (databaseExists(databaseName)) {
            glueDatabaseOperations.dropGlueDatabase(databaseName);
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    /**
     * Lists all tables in a specified database.
     *
     * @param databaseName the name of the database
     * @return a list of table names in the database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          if an error occurs while listing the tables
     */
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        // Check if the database exists before listing tables
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // Delegate table listing to GlueTableOperations
        return glueTableOperations.listTables(databaseName);
    }

    /**
     * Retrieves a table from the catalog using its object path.
     *
     * @param objectPath the object path of the table to retrieve
     * @return the corresponding CatalogBaseTable for the specified table
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException       if an error occurs while retrieving the table
     */
    @Override
    public CatalogBaseTable getTable(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        String databaseName = objectPath.getDatabaseName();
        String tableName = objectPath.getObjectName();

        // Delegate to GlueTableOperations to retrieve the Glue table
        Table glueTable = glueTableOperations.getGlueTable(databaseName, tableName);
        Schema schema = glueTableUtils.getSchemaFromGlueTable(glueTable);

        // Extract partition keys and properties
        List<String> partitionKeys = glueTable.partitionKeys().stream()
                .map(software.amazon.awssdk.services.glue.model.Column::name)
                .collect(Collectors.toList());
        Map<String, String> properties = new HashMap<>(glueTable.parameters());

        if (glueTable.storageDescriptor().hasParameters()) {
            properties.putAll(glueTable.storageDescriptor().parameters());
        }

        // Return a CatalogTable with the schema, description, partition keys, and properties
        if (glueTable.tableType().equalsIgnoreCase(CatalogBaseTable.TableKind.TABLE.name())) {
            return CatalogTable.of(schema, glueTable.description(), partitionKeys, properties);
        } else {
            throw new CatalogException("Unknown table type: " + glueTable.tableType());
        }
    }

    /**
     * Checks if a table exists in the Glue catalog.
     *
     * @param objectPath the object path of the table to check
     * @return true if the table exists, false otherwise
     * @throws CatalogException if an error occurs while checking the table's existence
     */
    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        String databaseName = objectPath.getDatabaseName();
        String tableName = objectPath.getObjectName();

        // Delegate existence check to GlueTableOperations
        return glueTableOperations.tableExists(databaseName, tableName);
    }

    /**
     * Drops a table from the Glue catalog.
     *
     * @param objectPath the object path of the table to drop
     * @param ifExists   flag indicating whether to ignore the exception if the table does not exist
     * @throws TableNotExistException if the table does not exist and ifExists is false
     * @throws CatalogException       if an error occurs while dropping the table
     */
    @Override
    public void dropTable(ObjectPath objectPath, boolean ifExists) throws TableNotExistException, CatalogException {
        String databaseName = objectPath.getDatabaseName();
        String tableName = objectPath.getObjectName();

        // Check if the table exists
        if (!glueTableOperations.tableExists(databaseName, tableName)) {
            if (!ifExists) {
                throw new TableNotExistException(getName(), objectPath);
            }
            return; // Table doesn't exist, and IF EXISTS is true
        }

        // Delegate table deletion to GlueTableOperations
        glueTableOperations.dropTable(databaseName, tableName);
    }

    /**
     * Creates a table in the Glue catalog.
     *
     * @param objectPath       the object path of the table to create
     * @param catalogBaseTable the table definition containing the schema and properties
     * @param ifNotExists      flag indicating whether to ignore the exception if the table already exists
     * @throws TableAlreadyExistException if the table already exists and ifNotExists is false
     * @throws DatabaseNotExistException  if the database does not exist
     * @throws CatalogException           if an error occurs while creating the table
     */
    @Override
    public void createTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean ifNotExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        String databaseName = objectPath.getDatabaseName();
        String tableName = objectPath.getObjectName();

        // Check if the database exists
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // Check if the table already exists
        if (glueTableOperations.tableExists(databaseName, tableName)) {
            if (!ifNotExists) {
                throw new TableAlreadyExistException(getName(), objectPath);
            }
            return; // Table exists, and IF NOT EXISTS is true
        }

        // Convert CatalogTable to Glue's TableInput
        CatalogTable catalogTable = (CatalogTable) catalogBaseTable;
        Map<String, String> tableProperties = new HashMap<>(catalogTable.getOptions());
        String tableLocation = glueTableUtils.extractTableLocation(tableProperties, objectPath);

        // Resolve the schema and map Flink columns to Glue columns
        ResolvedCatalogBaseTable<?> resolvedTable = (ResolvedCatalogBaseTable<?>) catalogBaseTable;
        List<software.amazon.awssdk.services.glue.model.Column> glueColumns = resolvedTable.getResolvedSchema().getColumns()
                .stream()
                .map(glueTableUtils::mapFlinkColumnToGlueColumn)
                .collect(Collectors.toList());

        // Build the storage descriptor
        StorageDescriptor storageDescriptor = glueTableUtils.buildStorageDescriptor(tableProperties, glueColumns, tableLocation);

        // Prepare TableInput for Glue table creation
        TableInput tableInput = glueTableOperations.buildTableInput(tableName, glueColumns, catalogTable, storageDescriptor, tableProperties);

        // Delegate table creation to GlueTableOperations
        glueTableOperations.createTable(databaseName, tableInput);
    }

    @Override
    public List<String> listViews(String s) throws DatabaseNotExistException, CatalogException {
        // Not supported yet
        return List.of();
    }

    @Override
    public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b) throws DatabaseNotExistException, CatalogException {
        // Not supported yet
    }

    @Override
    public void renameTable(ObjectPath objectPath, String s, boolean b) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        // Not supported yet
    }

    @Override
    public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b) throws TableNotExistException, CatalogException {
        // Not supported yet
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        // Not supported yet
        return List.of();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        // Not supported yet
        return List.of();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath objectPath, List<Expression> list) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        // Not supported yet
        return List.of();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        // Not supported yet
        return null;
    }

    @Override
    public boolean partitionExists(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
        // Not supported yet
        return false;
    }

    @Override
    public void createPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        // Not supported yet
    }

    @Override
    public void dropPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b) throws PartitionNotExistException, CatalogException {
        // Not supported yet
    }

    @Override
    public void alterPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws PartitionNotExistException, CatalogException {
        // Not supported yet
    }

    public ObjectPath normalize(ObjectPath path) {
        return new ObjectPath(
                path.getDatabaseName(), FunctionIdentifier.normalizeName(path.getObjectName()));
    }

    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return glueFunctionsOperations.listGlueFunctions(databaseName);
    }

    @Override
    public CatalogFunction getFunction(ObjectPath objectPath) throws FunctionNotExistException, CatalogException {
        ObjectPath functionPath = normalize(objectPath);
        if (!functionExists(functionPath)) {
            throw new FunctionNotExistException(getName(), functionPath);
        } else {
            return glueFunctionsOperations.getGlueFunction(functionPath);
        }
    }

    @Override
    public boolean functionExists(ObjectPath objectPath) throws CatalogException {
        ObjectPath functionPath = normalize(objectPath);
        return databaseExists(functionPath.getDatabaseName())
                && glueFunctionsOperations.glueFunctionExists(functionPath);
    }


    @Override
    public void createFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        ObjectPath functionPath = normalize(objectPath);
        if (!databaseExists(functionPath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), functionPath.getDatabaseName());
        }
        if (!functionExists(functionPath)) {
            glueFunctionsOperations.createGlueFunction(functionPath, catalogFunction);
        } else {
            if (!ignoreIfExists) {
                throw new FunctionAlreadyExistException(getName(), functionPath);
            }
        }
    }

    @Override
    public void alterFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        ObjectPath functionPath = normalize(objectPath);
        CatalogFunction existingFunction = getFunction(functionPath);
        if (existingFunction != null) {
            if (existingFunction.getClass() != catalogFunction.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Function types don't match. Existing function is '%s' and new function is '%s'.",
                                existingFunction.getClass().getName(),
                                catalogFunction.getClass().getName()));
            }
            glueFunctionsOperations.alterGlueFunction(functionPath, catalogFunction);
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    @Override
    public void dropFunction(ObjectPath objectPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        ObjectPath functionPath = normalize(objectPath);
        if (functionExists(functionPath)) {
            glueFunctionsOperations.dropGlueFunction(functionPath);
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath objectPath, CatalogTableStatistics catalogTableStatistics, boolean b) throws TableNotExistException, CatalogException {

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath objectPath, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws TableNotExistException, CatalogException, TablePartitionedException {

    }

    @Override
    public void alterPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogTableStatistics catalogTableStatistics, boolean b) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws PartitionNotExistException, CatalogException {

    }
}


