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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.glue.operations.GlueDatabaseOperations;
import org.apache.flink.table.catalog.glue.operations.GlueFunctionsOperations;
import org.apache.flink.table.catalog.glue.operations.GlueTableOperations;
import org.apache.flink.table.catalog.glue.util.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueTableUtils;
import org.apache.flink.table.catalog.glue.util.GlueTypeConverter;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.FunctionIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * GlueCatalog is an implementation of the Flink AbstractCatalog that interacts with AWS Glue.
 * This class allows Flink to perform various catalog operations such as creating, deleting, and retrieving
 * databases and tables from Glue. It encapsulates AWS Glue's API and provides a Flink-compatible interface.
 *
 * <p>This catalog uses GlueClient to interact with AWS Glue services, and operations related to databases and
 * tables are delegated to respective helper classes like GlueDatabaseOperations and GlueTableOperations.</p>
 */
public class GlueCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GlueCatalog.class);

    private final GlueClient glueClient;
    private final GlueTypeConverter glueTypeConverter;
    private final GlueDatabaseOperations glueDatabaseOperations;
    private final GlueTableOperations glueTableOperations;
    private final GlueFunctionsOperations glueFunctionsOperations;
    private final GlueTableUtils glueTableUtils;

    /**
     * Constructs a GlueCatalog with a provided Glue client.
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
                    .region(Region.of(region))
                    .build();
        }
        this.glueTypeConverter = new GlueTypeConverter();
        this.glueTableUtils = new GlueTableUtils(glueTypeConverter);
        this.glueDatabaseOperations = new GlueDatabaseOperations(glueClient, getName());
        this.glueTableOperations = new GlueTableOperations(glueClient, getName());
        this.glueFunctionsOperations = new GlueFunctionsOperations(glueClient, getName());
    }

    /**
     * Constructs a GlueCatalog with default client.
     *
     * @param name            the name of the catalog
     * @param defaultDatabase the default database for the catalog
     * @param region          the AWS region to be used for Glue operations
     */
    public GlueCatalog(String name, String defaultDatabase, String region) {
        super(name, defaultDatabase);

        // Create a synchronized client builder to avoid concurrent modification exceptions
        this.glueClient = GlueClient.builder()
                .region(Region.of(region))
                .credentialsProvider(software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.create())
                .build();
        this.glueTypeConverter = new GlueTypeConverter();
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
        LOG.info("Opening GlueCatalog with client: {}", glueClient);
    }

    /**
     * Closes the GlueCatalog and releases resources.
     *
     * @throws CatalogException if an error occurs during the closing process
     */
    @Override
    public void close() throws CatalogException {
        if (glueClient != null) {
            LOG.info("Closing GlueCatalog client");
            int maxRetries = 3;
            int retryCount = 0;
            long retryDelayMs = 200;
            while (retryCount < maxRetries) {
                try {
                    glueClient.close();
                    LOG.info("Successfully closed GlueCatalog client");
                    return;
                } catch (RuntimeException e) {
                    retryCount++;
                    if (retryCount >= maxRetries) {
                        LOG.warn("Failed to close GlueCatalog client after {} retries", maxRetries, e);
                        throw new CatalogException("Failed to close GlueCatalog client", e);
                    }
                    LOG.warn("Failed to close GlueCatalog client (attempt {}/{}), retrying in {} ms",
                            retryCount, maxRetries, retryDelayMs, e);
                    try {
                        Thread.sleep(retryDelayMs);
                        // Exponential backoff
                        retryDelayMs *= 2;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new CatalogException("Interrupted while retrying to close GlueCatalog client", ie);
                    }
                }
            }
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

        if (exists && !ifNotExists) {
            throw new DatabaseAlreadyExistException(getName(), databaseName);
        }

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
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

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

        Table glueTable = glueTableOperations.getGlueTable(databaseName, tableName);
        return getCatalogBaseTableFromGlueTable(glueTable);
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
        return glueTableOperations.glueTableExists(databaseName, tableName);
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

        if (!glueTableOperations.glueTableExists(databaseName, tableName)) {
            if (!ifExists) {
                throw new TableNotExistException(getName(), objectPath);
            }
            return; // Table doesn't exist, and IF EXISTS is true
        }

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
    public void createTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean ifNotExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        // Validate that required parameters are not null
        if (objectPath == null) {
            throw new NullPointerException("ObjectPath cannot be null");
        }

        if (catalogBaseTable == null) {
            throw new NullPointerException("CatalogBaseTable cannot be null");
        }

        String databaseName = objectPath.getDatabaseName();
        String tableName = objectPath.getObjectName();

        // Check if the database exists
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // Check if the table already exists
        if (glueTableOperations.glueTableExists(databaseName, tableName)) {
            if (!ifNotExists) {
                throw new TableAlreadyExistException(getName(), objectPath);
            }
            return; // Table exists, and IF NOT EXISTS is true
        }

        // Get common properties
        Map<String, String> tableProperties = new HashMap<>(catalogBaseTable.getOptions());

        try {
            // Process based on table type
            if (catalogBaseTable.getTableKind() == CatalogBaseTable.TableKind.TABLE) {
                createRegularTable(objectPath, (CatalogTable) catalogBaseTable, tableProperties);
            } else if (catalogBaseTable.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
                createView(objectPath, (CatalogView) catalogBaseTable, tableProperties);
            } else {
                throw new CatalogException("Unsupported table kind: " + catalogBaseTable.getTableKind());
            }
            LOG.info("Successfully created {}.{} of kind {}", databaseName, tableName, catalogBaseTable.getTableKind());
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to create table %s.%s: %s", databaseName, tableName, e.getMessage()), e);
        }
    }

    /**
     * Lists all views in a specified database.
     *
     * @param databaseName the name of the database
     * @return a list of view names in the database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          if an error occurs while listing the views
     */
    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {

        // Check if the database exists before listing views
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            // Get all tables in the database
            List<Table> allTables = glueClient.getTables(builder -> builder.databaseName(databaseName))
                    .tableList();

            // Filter tables to only include those that are of type VIEW
            List<String> viewNames = allTables.stream()
                    .filter(table -> {
                        String tableType = table.tableType();
                        return tableType != null && tableType.equalsIgnoreCase(CatalogBaseTable.TableKind.VIEW.name());
                    })
                    .map(Table::name)
                    .collect(Collectors.toList());

            return viewNames;
        } catch (Exception e) {
            LOG.error("Failed to list views in database {}: {}", databaseName, e.getMessage());
            throw new CatalogException(
                    String.format("Error listing views in database %s: %s", databaseName, e.getMessage()), e);
        }
    }

    @Override
    public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Altering databases is not supported by the Glue Catalog.");
    }

    @Override
    public void renameTable(ObjectPath objectPath, String s, boolean b) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("Renaming tables is not supported by the Glue Catalog.");
    }

    @Override
    public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Altering tables is not supported by the Glue Catalog.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath objectPath, List<Expression> list) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    @Override
    public CatalogPartition getPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    @Override
    public boolean partitionExists(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    @Override
    public void createPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    @Override
    public void dropPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    @Override
    public void alterPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Table partitioning operations are not supported by the Glue Catalog.");
    }

    /**
     * Normalizes an object path according to catalog-specific normalization rules.
     * For functions, this ensures consistent case handling in function names.
     *
     * @param path the object path to normalize
     * @return the normalized object path
     */
    public ObjectPath normalize(ObjectPath path) {
        if (path == null) {
            throw new NullPointerException("ObjectPath cannot be null");
        }

        return new ObjectPath(
                path.getDatabaseName(),
                FunctionIdentifier.normalizeName(path.getObjectName()));
    }

    /**
     * Lists all functions in a specified database.
     *
     * @param databaseName the name of the database
     * @return a list of function names in the database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          if an error occurs while listing the functions
     */
    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            List<String> functions = glueFunctionsOperations.listGlueFunctions(databaseName);
            return functions;
        } catch (CatalogException e) {
            LOG.error("Failed to list functions in database {}: {}", databaseName, e.getMessage());
            throw new CatalogException(
                    String.format("Error listing functions in database %s: %s", databaseName, e.getMessage()), e);
        }
    }

    /**
     * Retrieves a function from the catalog.
     *
     * @param functionPath the object path of the function to retrieve
     * @return the corresponding CatalogFunction
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException          if an error occurs while retrieving the function
     */
    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {

        // Normalize the path for case-insensitive handling
        ObjectPath normalizedPath = normalize(functionPath);

        if (!databaseExists(normalizedPath.getDatabaseName())) {
            throw new CatalogException(getName());
        }

        try {
            return glueFunctionsOperations.getGlueFunction(normalizedPath);
        } catch (FunctionNotExistException e) {
            throw e;
        } catch (CatalogException e) {
            throw new CatalogException(
                    String.format("Failed to get function %s", normalizedPath.getFullName()), e);
        }
    }

    /**
     * Checks if a function exists in the catalog.
     *
     * @param functionPath the object path of the function to check
     * @return true if the function exists, false otherwise
     * @throws CatalogException if an error occurs while checking the function's existence
     */
    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {

        // Normalize the path for case-insensitive handling
        ObjectPath normalizedPath = normalize(functionPath);

        if (!databaseExists(normalizedPath.getDatabaseName())) {
            return false;
        }

        try {
            return glueFunctionsOperations.glueFunctionExists(normalizedPath);
        } catch (CatalogException e) {
            throw new CatalogException(
                    String.format("Failed to check if function %s exists", normalizedPath.getFullName()), e);
        }
    }

    /**
     * Creates a function in the catalog.
     *
     * @param functionPath      the object path of the function to create
     * @param function          the function definition
     * @param ignoreIfExists    flag indicating whether to ignore the exception if the function already exists
     * @throws FunctionAlreadyExistException if the function already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException     if the database does not exist
     * @throws CatalogException              if an error occurs while creating the function
     */
    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

        // Normalize the path for case-insensitive handling
        ObjectPath normalizedPath = normalize(functionPath);

        if (!databaseExists(normalizedPath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), normalizedPath.getDatabaseName());
        }

        boolean exists = functionExists(normalizedPath);

        if (exists && !ignoreIfExists) {
            throw new FunctionAlreadyExistException(getName(), normalizedPath);
        } else if (exists) {
            return;
        }

        try {
            glueFunctionsOperations.createGlueFunction(normalizedPath, function);
        } catch (FunctionAlreadyExistException e) {
            throw e;
        } catch (CatalogException e) {
            throw new CatalogException(
                    String.format("Failed to create function %s", normalizedPath.getFullName()), e);
        }
    }

    /**
     * Alters a function in the catalog.
     *
     * @param functionPath       the object path of the function to alter
     * @param newFunction        the new function definition
     * @param ignoreIfNotExists  flag indicating whether to ignore the exception if the function does not exist
     * @throws FunctionNotExistException if the function does not exist and ignoreIfNotExists is false
     * @throws CatalogException          if an error occurs while altering the function
     */
    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {

        // Normalize the path for case-insensitive handling
        ObjectPath normalizedPath = normalize(functionPath);

        try {
            // Check if function exists without throwing an exception first
            boolean functionExists = functionExists(normalizedPath);

            if (!functionExists) {
                if (ignoreIfNotExists) {
                    return;
                } else {
                    throw new FunctionNotExistException(getName(), normalizedPath);
                }
            }

            // Check for type compatibility of function
            CatalogFunction existingFunction = getFunction(normalizedPath);
            if (existingFunction.getClass() != newFunction.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Function types don't match. Existing function is '%s' and new function is '%s'.",
                                existingFunction.getClass().getName(),
                                newFunction.getClass().getName()));
            }

            // Proceed with alteration
            glueFunctionsOperations.alterGlueFunction(normalizedPath, newFunction);
        } catch (FunctionNotExistException e) {
            if (ignoreIfNotExists) {
            } else {
                throw e;
            }
        } catch (CatalogException e) {
            throw new CatalogException(
                    String.format("Failed to alter function %s", normalizedPath.getFullName()), e);
        }
    }

    /**
     * Drops a function from the catalog.
     *
     * @param functionPath        the object path of the function to drop
     * @param ignoreIfNotExists   flag indicating whether to ignore the exception if the function does not exist
     * @throws FunctionNotExistException if the function does not exist and ignoreIfNotExists is false
     * @throws CatalogException          if an error occurs while dropping the function
     */
    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {

        // Normalize the path for case-insensitive handling
        ObjectPath normalizedPath = normalize(functionPath);

        if (!databaseExists(normalizedPath.getDatabaseName())) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new FunctionNotExistException(getName(), normalizedPath);
        }

        try {
            boolean exists = functionExists(normalizedPath);
            if (!exists) {
                if (ignoreIfNotExists) {
                    return;
                } else {
                    throw new FunctionNotExistException(getName(), normalizedPath);
                }
            }

            // Function exists, proceed with dropping it
            glueFunctionsOperations.dropGlueFunction(normalizedPath);
        } catch (FunctionNotExistException e) {
            if (!ignoreIfNotExists) {
                throw e;
            }
        } catch (CatalogException e) {
            throw new CatalogException(
                    String.format("Failed to drop function %s", normalizedPath.getFullName()), e);
        }
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Table statistics are not supported by the Glue Catalog.");
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Table column statistics are not supported by the Glue Catalog.");
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Partition statistics are not supported by the Glue Catalog.");
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Partition column statistics are not supported by the Glue Catalog.");
    }

    @Override
    public void alterTableStatistics(ObjectPath objectPath, CatalogTableStatistics catalogTableStatistics, boolean b) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Altering table statistics is not supported by the Glue Catalog.");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath objectPath, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException("Altering table column statistics is not supported by the Glue Catalog.");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogTableStatistics catalogTableStatistics, boolean b) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Altering partition statistics is not supported by the Glue Catalog.");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Altering partition column statistics is not supported by the Glue Catalog.");
    }

    // ============================ Private Methods ============================
    /**
     * Converts an AWS Glue Table to a Flink CatalogBaseTable, supporting both tables and views.
     *
     * @param glueTable the AWS Glue table to convert
     * @return the corresponding Flink CatalogBaseTable (either CatalogTable or CatalogView)
     * @throws CatalogException if the table type is unknown or conversion fails
     */
    private CatalogBaseTable getCatalogBaseTableFromGlueTable(Table glueTable) {

        try {
            // Parse schema from Glue table structure
            Schema schemaInfo = glueTableUtils.getSchemaFromGlueTable(glueTable);

            // Extract partition keys
            List<String> partitionKeys = glueTable.partitionKeys() != null
                ? glueTable.partitionKeys().stream()
                    .map(software.amazon.awssdk.services.glue.model.Column::name)
                    .collect(Collectors.toList())
                : Collections.emptyList();

            // Collect all properties
            Map<String, String> properties = new HashMap<>();

            // Add table parameters
            if (glueTable.parameters() != null) {
                properties.putAll(glueTable.parameters());
            }

            // Add owner if present
            if (glueTable.owner() != null) {
                properties.put(GlueCatalogConstants.TABLE_OWNER, glueTable.owner());
            }

            // Add storage parameters if present
            if (glueTable.storageDescriptor() != null) {
                if (glueTable.storageDescriptor().hasParameters()) {
                    properties.putAll(glueTable.storageDescriptor().parameters());
                }

                // Add input/output formats if present
                if (glueTable.storageDescriptor().inputFormat() != null) {
                    properties.put(
                            GlueCatalogConstants.TABLE_INPUT_FORMAT,
                            glueTable.storageDescriptor().inputFormat());
                }

                if (glueTable.storageDescriptor().outputFormat() != null) {
                    properties.put(
                            GlueCatalogConstants.TABLE_OUTPUT_FORMAT,
                            glueTable.storageDescriptor().outputFormat());
                }
            }

            // Check table type and create appropriate catalog object
            String tableType = glueTable.tableType();
            if (tableType == null) {
                LOG.warn("Table type is null for table {}, defaulting to TABLE", glueTable.name());
                tableType = CatalogBaseTable.TableKind.TABLE.name();
            }

            if (tableType.equalsIgnoreCase(CatalogBaseTable.TableKind.TABLE.name())) {
                return CatalogTable.of(
                        schemaInfo,
                        glueTable.description(),
                        partitionKeys,
                        properties);
            } else if (tableType.equalsIgnoreCase(CatalogBaseTable.TableKind.VIEW.name())) {
                String originalQuery = glueTable.viewOriginalText();
                String expandedQuery = glueTable.viewExpandedText();

                if (originalQuery == null) {
                    throw new CatalogException(
                            String.format("View '%s' is missing its original query text", glueTable.name()));
                }

                // If expanded query is null, use original query
                if (expandedQuery == null) {
                    expandedQuery = originalQuery;
                }

                return CatalogView.of(
                        schemaInfo,
                        glueTable.description(),
                        originalQuery,
                        expandedQuery,
                        properties);
            } else {
                throw new CatalogException(
                        String.format("Unknown table type: %s from Glue Catalog.", tableType));
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to convert Glue table '%s' to Flink table: %s",
                            glueTable.name(), e.getMessage()), e);
        }
    }

    /**
     * Creates a regular table in the Glue catalog.
     *
     * @param objectPath      the object path of the table
     * @param catalogTable    the table definition
     * @param tableProperties the table properties
     * @throws CatalogException if an error occurs during table creation
     */
    private void createRegularTable(
            ObjectPath objectPath,
            CatalogTable catalogTable,
            Map<String, String> tableProperties) throws CatalogException {

        String databaseName = objectPath.getDatabaseName();
        String tableName = objectPath.getObjectName();

        // Extract table location
        String tableLocation = glueTableUtils.extractTableLocation(tableProperties, objectPath);

        // Resolve the schema and map Flink columns to Glue columns
        ResolvedCatalogBaseTable<?> resolvedTable = (ResolvedCatalogBaseTable<?>) catalogTable;
        List<software.amazon.awssdk.services.glue.model.Column> glueColumns = resolvedTable.getResolvedSchema().getColumns()
                .stream()
                .map(glueTableUtils::mapFlinkColumnToGlueColumn)
                .collect(Collectors.toList());

        StorageDescriptor storageDescriptor = glueTableUtils.buildStorageDescriptor(tableProperties, glueColumns, tableLocation);

        TableInput tableInput = glueTableOperations.buildTableInput(tableName, glueColumns, catalogTable, storageDescriptor, tableProperties);

        glueTableOperations.createTable(databaseName, tableInput);
    }

    /**
     * Creates a view in the Glue catalog.
     *
     * @param objectPath      the object path of the view
     * @param catalogView     the view definition
     * @param tableProperties the view properties
     * @throws CatalogException if an error occurs during view creation
     */
    private void createView(
            ObjectPath objectPath,
            CatalogView catalogView,
            Map<String, String> tableProperties) throws CatalogException {

        String databaseName = objectPath.getDatabaseName();
        String tableName = objectPath.getObjectName();

        // Resolve the schema and map Flink columns to Glue columns
        ResolvedCatalogBaseTable<?> resolvedView = (ResolvedCatalogBaseTable<?>) catalogView;
        List<software.amazon.awssdk.services.glue.model.Column> glueColumns = resolvedView.getResolvedSchema().getColumns()
                .stream()
                .map(glueTableUtils::mapFlinkColumnToGlueColumn)
                .collect(Collectors.toList());

        // Build a minimal storage descriptor for views
        StorageDescriptor storageDescriptor = StorageDescriptor.builder()
                .columns(glueColumns)
                .build();

        // Create view-specific TableInput
        TableInput viewInput = TableInput.builder()
                .name(tableName)
                .tableType(CatalogBaseTable.TableKind.VIEW.name())
                .viewOriginalText(catalogView.getOriginalQuery())
                .viewExpandedText(catalogView.getExpandedQuery())
                .storageDescriptor(storageDescriptor)
                .parameters(tableProperties)
                .description(catalogView.getComment())
                .build();

        // Create the view
        glueTableOperations.createTable(databaseName, viewInput);
    }
}
