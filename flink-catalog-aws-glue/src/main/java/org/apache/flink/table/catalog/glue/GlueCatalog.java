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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
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
import org.apache.flink.table.catalog.glue.util.AwsClientFactories;
import org.apache.flink.table.catalog.glue.util.AwsProperties;
import org.apache.flink.table.catalog.glue.util.GlueUtils;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.ResourceUri;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Glue catalog implementation that uses glue catalog.
 */
public class GlueCatalog extends AbstractCatalog {

    public GlueUtils glueUtils;
    public static final String DEFAULT_DB = "default";
    private static final Logger LOG = LoggerFactory.getLogger(GlueCatalog.class);

    public GlueCatalog(String name, String defaultDatabase, Map<String, String> glueProperties) {
        super(name, defaultDatabase);
        checkNotNull(glueProperties);
        initialize(glueProperties);
    }

    public void initialize(Map<String, String> catalogProperties) {
        // setLocationUri for the database level
        String locationUri = catalogProperties.getOrDefault(GlueUtils.LOCATION_URI, "");
        // initialize aws client factories
        AwsProperties awsProperties = new AwsProperties(catalogProperties);

        // create glue client
        GlueClient glueClient = AwsClientFactories.factory(awsProperties).glue();
        this.glueUtils = new GlueUtils(locationUri, awsProperties, glueClient);
    }

    /**
     * Open the catalog. Used for any required preparation in initialization phase.
     *
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void open() throws CatalogException {
    }

    /**
     * Close the catalog when it is no longer needed and release any resource that it might be
     * holding.
     *
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void close() throws CatalogException {
        try {
            glueUtils.glueClient.close();
        } catch (Exception e) {
            LOG.warn("Glue Client is not closed properly!");
        }
    }

    // ------ databases ------

    /**
     * Create a database.
     *
     * @param name           Name of the database to be created
     * @param database       The database definition
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *                       exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *                       nothing.
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *                                       is false
     * @throws CatalogException              in case of any runtime exception
     */
    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {

        checkArgument(!StringUtils.isNullOrWhitespaceOnly(name));
        checkNotNull(database);
        if (databaseExists(name)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
        } else {
            DatabaseInput databaseInput =
                    DatabaseInput.builder()
                            .name(name)
                            .description(database.getDescription().orElse(""))
                            .parameters(database.getProperties())
                            .locationUri(glueUtils.locationUri + "/" + name)
                            .build();
            CreateDatabaseRequest.Builder createDatabaseRequestBuilder =
                    CreateDatabaseRequest.builder()
                            .databaseInput(databaseInput)
                            .catalogId(glueUtils.awsProperties.getGlueCatalogId());

            try {
                CreateDatabaseResponse response =
                        glueUtils.glueClient.createDatabase(createDatabaseRequestBuilder.build());
                GlueUtils.validateGlueResponse(response);
                LOG.info("New Database created. ");
            } catch (GlueException e) {
                throw new DatabaseAlreadyExistException(getName(), name, e);
            }
        }
    }

    /**
     * Drop a database.
     *
     * @param name              Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *                          false, throw an exception, if set to true, do nothing.
     * @param cascade           Flag to specify behavior when the database contains table or function: if set
     *                          to true, delete all tables and functions in the database and then delete the database, if
     *                          set to false, throw an exception.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws DatabaseNotEmptyException if the given database is not empty and isRestrict is true
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(name));
        if (databaseExists(name)) {
            if (glueUtils.isDatabaseEmpty(name)) {
                glueUtils.deleteGlueDatabase(name);
            } else if (cascade) {
                // delete all tables in database
                glueUtils.deleteAllTablesInDatabase(name);
                // delete all functions in database
                glueUtils.deleteAllFunctionInDatabase(name);
            } else {
                throw new DatabaseNotEmptyException(getName(), name);
            }
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), name);
        }
    }

    /**
     * Modify an existing database.
     *
     * @param name              Name of the database to be modified
     * @param newDatabase       The new database definition
     * @param ignoreIfNotExists Flag to specify behavior when the given database does not exist: if
     *                          set to false, throw an exception, if set to true, do nothing.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(name));
        checkNotNull(newDatabase);

        CatalogDatabase existingDatabase = glueUtils.getExistingDatabase(name);

        if (existingDatabase != null) {
            if (existingDatabase.getClass() != newDatabase.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Database types don't match. Existing database is '%s' and new database is '%s'.",
                                existingDatabase.getClass().getName(),
                                newDatabase.getClass().getName()));
            }
            // update information in Glue Data catalog
            glueUtils.updateGlueDatabase(name, newDatabase);
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), name);
        }
    }

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            GetDatabasesRequest databasesRequest =
                    GetDatabasesRequest.builder()
                            .catalogId(glueUtils.awsProperties.getGlueCatalogId())
                            .build();
            GetDatabasesResponse response = glueUtils.glueClient.getDatabases(databasesRequest);
            GlueUtils.validateGlueResponse(response);
            List<String> glueDatabases =
                    response.databaseList().stream()
                            .map(Database::name)
                            .collect(Collectors.toList());
            String dbResultNextToken = response.nextToken();
            if (Optional.ofNullable(dbResultNextToken).isPresent()) {
                do {
                    databasesRequest =
                            GetDatabasesRequest.builder()
                                    .catalogId(glueUtils.awsProperties.getGlueCatalogId())
                                    .nextToken(dbResultNextToken)
                                    .build();
                    response = glueUtils.glueClient.getDatabases(databasesRequest);
                    GlueUtils.validateGlueResponse(response);
                    glueDatabases.addAll(
                            response.databaseList().stream()
                                    .map(Database::name)
                                    .collect(Collectors.toList()));
                    dbResultNextToken = response.nextToken();
                } while (Optional.ofNullable(dbResultNextToken).isPresent());
            }
            return glueDatabases;
        } catch (GlueException e) {
            throw new CatalogException("Exception listing databases from glue", e.getCause());
        }
    }

    /**
     * Get a database from this catalog.
     *
     * @param databaseName Name of the database
     * @return The requested database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        try {
            GetDatabaseRequest databasesRequest =
                    GetDatabaseRequest.builder()
                            .name(databaseName)
                            .catalogId(glueUtils.awsProperties.getGlueCatalogId())
                            .build();

            GetDatabaseResponse response = glueUtils.glueClient.getDatabase(databasesRequest);
            GlueUtils.validateGlueResponse(response);
            return new CatalogDatabaseImpl(
                    GlueUtils.getDatabaseProperties(response), response.database().description());

        } catch (GlueException e) {
            throw new CatalogException(e.awsErrorDetails().errorMessage(), e.getCause());
        }
    }

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            GetDatabaseRequest dbRequest =
                    GetDatabaseRequest.builder()
                            .name(databaseName)
                            .catalogId(glueUtils.awsProperties.getGlueCatalogId())
                            .build();
            GetDatabaseResponse response = glueUtils.glueClient.getDatabase(dbRequest);
            return response.sdkHttpResponse().isSuccessful() && response.database().name().equals(databaseName);
        } catch (GlueException e) {
            LOG.warn("Exception occurred in glue connection :\n" + e.getMessage());
            return false;
        }
    }

    // ------ tables ------

    /**
     * Creates a new table or view.
     *
     * <p>The framework will make sure to call this method with fully validated {@link
     * ResolvedCatalogTable} or {@link ResolvedCatalogView}. Those instances are easy to serialize
     * for a durable catalog implementation.
     *
     * @param tablePath      path of the table or view to be created
     * @param table          the table definition
     * @param ignoreIfExists flag to specify behavior when a table or view already exists at the
     *                       given path: if set to false, it throws a TableAlreadyExistException, if set to true, do
     *                       nothing.
     * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException  if the database in tablePath doesn't exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(table);

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
        } else {
            glueUtils.createGlueTable(tablePath, table);
            if (isPartitionedTable(tablePath)) {
                // update partition
                glueUtils.updateGlueTablePartition(tablePath, table);
                // update partition stats
                glueUtils.updateGluePartitionStats(tablePath, table);
                // update column stats
                glueUtils.updateGluePartitionColumnStats(tablePath, table);
            }
        }
    }

    private boolean isPartitionedTable(ObjectPath tablePath) {
        CatalogBaseTable table;
        try {
            table = getTable(tablePath);
        } catch (TableNotExistException e) {
            return false;
        }
        return (table instanceof CatalogTable) && ((CatalogTable) table).isPartitioned();
    }

    /**
     * Modifies an existing table or view. Note that the new and old {@link CatalogBaseTable} must
     * be of the same kind. For example, this doesn't allow altering a regular table to partitioned
     * table, or altering a view to a table, and vice versa.
     *
     * <p>The framework will make sure to call this method with fully validated {@link
     * ResolvedCatalogTable} or {@link ResolvedCatalogView}. Those instances are easy to serialize
     * for a durable catalog implementation.
     *
     * @param tablePath         path of the table or view to be modified
     * @param newTable          the new table definition
     * @param ignoreIfNotExists flag to specify behavior when the table or view does not exist: if
     *                          set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {

        checkNotNull(tablePath);
        checkNotNull(newTable);

        CatalogBaseTable existingTable = getTable(tablePath);

        if (existingTable != null) {
            if (existingTable.getTableKind() != newTable.getTableKind()) {
                throw new CatalogException(
                        String.format(
                                "Table types don't match. Existing table is '%s' and new table is '%s'.",
                                existingTable.getTableKind(), newTable.getTableKind()));
            }
            glueUtils.alterGlueTable(tablePath, newTable);
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    // ------ tables and views ------

    /**
     * Drop a table or view.
     *
     * @param tablePath         Path of the table or view to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist: if
     *                          set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table or view does not exist
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (tableExists(tablePath)) {
            glueUtils.deleteGlueTable(tablePath);
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    /**
     * Rename an existing table or view.
     *
     * @param tablePath         Path of the table or view to be renamed
     * @param newTableName      the new name of the table or view
     * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist: if
     *                          set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        checkNotNull(tablePath);
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(newTableName));

        if (tableExists(tablePath)) {
            ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
            if (tableExists(newTablePath)) {
                throw new TableAlreadyExistException(getName(), newTablePath);
            } else {
                // update table
                glueUtils.updateGlueTable(getTable(tablePath), newTablePath);
                // todo table statistics

                // todo table column statistics

                // todo partitions

                // todo partition statistics

            }
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    /**
     * Get names of all tables and views under this database. An empty list is returned if none
     * exists.
     *
     * @param databaseName fully qualified database name.
     * @return a list of the names of all tables and views in this database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return glueUtils.getGlueTableList(databaseName, "CatalogTable");
    }

    /**
     * Get names of all views under this database. An empty list is returned if none exists.
     *
     * @param databaseName the name of the given database
     * @return a list of the names of all views in the given database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return glueUtils.getGlueTableList(databaseName, "CatalogView");
    }

    /**
     * Returns a {@link CatalogTable} or {@link CatalogView} identified by the given {@link
     * ObjectPath}. The framework will resolve the metadata objects when necessary.
     *
     * @param tablePath Path of the table or view
     * @return The requested table or view
     * @throws TableNotExistException if the target does not exist
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        } else {
            Table glueTable = glueUtils.getGlueTable(tablePath);
            Tuple2<Schema, String> schemaInfo = glueUtils.getSchemaFromGlueTable(glueTable);
            List<String> patitionKeys = glueUtils.getPartitionKeysFromGlueTable(glueTable);
            return CatalogTable.of(
                    schemaInfo.f0, schemaInfo.f1, patitionKeys, glueTable.parameters());
        }
    }

    /**
     * Check if a table or view exists in this catalog.
     *
     * @param tablePath Path of the table or view
     * @return true if the given table exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        return databaseExists(tablePath.getDatabaseName()) && glueUtils.glueTableExists(tablePath);
    }

    // ------ functions ------

    /**
     * Create a function. Function name should be handled in a case insensitive way.
     *
     * @param path           path of the function
     * @param function       the function to be created
     * @param ignoreIfExists flag to specify behavior if a function with the given name already
     *                       exists: if set to false, it throws a FunctionAlreadyExistException, if set to true,
     *                       nothing happens.
     * @throws FunctionAlreadyExistException if the function already exist
     * @throws DatabaseNotExistException     if the given database does not exist
     * @throws CatalogException              in case of any runtime exception
     */
    @Override
    public void createFunction(ObjectPath path, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(path);
        checkNotNull(function);

        ObjectPath functionPath = normalize(path);
        if (!databaseExists(functionPath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), functionPath.getDatabaseName());
        }

        if (functionExists(functionPath)) {
            if (!ignoreIfExists) {
                throw new FunctionAlreadyExistException(getName(), functionPath);
            }
        } else {
            glueUtils.createGlueFunction(functionPath, function);
        }
    }

    private ObjectPath normalize(ObjectPath path) {
        return new ObjectPath(
                path.getDatabaseName(), FunctionIdentifier.normalizeName(path.getObjectName()));
    }

    /**
     * Modify an existing function. Function name should be handled in a case insensitive way.
     *
     * @param path              path of the function
     * @param newFunction       the function to be modified
     * @param ignoreIfNotExists flag to specify behavior if the function does not exist: if set to
     *                          false, throw an exception if set to true, nothing happens
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public void alterFunction(
            ObjectPath path, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path);
        checkNotNull(newFunction);

        ObjectPath functionPath = normalize(path);

        CatalogFunction existingFunction = getFunction(functionPath);

        if (existingFunction != null) {
            if (existingFunction.getClass() != newFunction.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Function types don't match. Existing function is '%s' and new function is '%s'.",
                                existingFunction.getClass().getName(),
                                newFunction.getClass().getName()));
            }

            glueUtils.alterFunctionInGlue(functionPath, newFunction.copy());
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    /**
     * Drop a function. Function name should be handled in a case insensitive way.
     *
     * @param path              path of the function to be dropped
     * @param ignoreIfNotExists plag to specify behavior if the function does not exist: if set to
     *                          false, throw an exception if set to true, nothing happens
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public void dropFunction(ObjectPath path, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path);

        ObjectPath functionPath = normalize(path);

        if (functionExists(functionPath)) {
            glueUtils.removeGlueFunction(functionPath);
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }


    /**
     * List the names of all functions in the given database. An empty list is returned if none is
     * registered.
     *
     * @param databaseName name of the database.
     * @return a list of the names of the functions in this database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public List<String> listFunctions(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return glueUtils.listGlueFunctions(databaseName);
    }

    /**
     * Get the function. Function name should be handled in a case insensitive way.
     *
     * @param path path of the function
     * @return the requested function
     * @throws FunctionNotExistException if the function does not exist in the catalog
     * @throws CatalogException          in case of any runtime exception
     */
    @Override
    public CatalogFunction getFunction(ObjectPath path)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path);

        ObjectPath functionPath = normalize(path);

        if (!functionExists(functionPath)) {
            throw new FunctionNotExistException(getName(), functionPath);
        } else {
            return getFunctionFromGlue(functionPath);
        }
    }

    CatalogFunction getFunctionFromGlue(ObjectPath functionPath) {
        GetUserDefinedFunctionRequest request = GetUserDefinedFunctionRequest.builder()
                .catalogId(glueUtils.awsProperties.getGlueCatalogId())
                .databaseName(functionPath.getDatabaseName())
                .functionName(functionPath.getObjectName())
                .build();
        GetUserDefinedFunctionResponse response = glueUtils.glueClient.getUserDefinedFunction(request);
        GlueUtils.validateGlueResponse(response);
        List<org.apache.flink.table.resource.ResourceUri> resourceUris = new LinkedList<>();
        for (ResourceUri resourceUri : response.userDefinedFunction().resourceUris()) {
            resourceUris.add(new org.apache.flink.table.resource.ResourceUri(
                    ResourceType.valueOf(resourceUri.resourceType().name()), resourceUri.uri()));
        }

        return new CatalogFunctionImpl(response.userDefinedFunction().className(),
                glueUtils.getFunctionalLanguage(response.userDefinedFunction()), resourceUris);
    }

    /**
     * Check whether a function exists or not. Function name should be handled in a case insensitive
     * way.
     *
     * @param path path of the function
     * @return true if the function exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public boolean functionExists(ObjectPath path) throws CatalogException {
        checkNotNull(path);

        ObjectPath functionPath = normalize(path);

        return databaseExists(functionPath.getDatabaseName()) && glueUtils.isGlueFunctionExists(functionPath);
    }

    /**
     * Create a partition.
     *
     * @param tablePath      path of the table.
     * @param partitionSpec  partition spec of the partition
     * @param partition      the partition to add.
     * @param ignoreIfExists flag to specify behavior if a table with the given name already exists:
     *                       if set to false, it throws a TableAlreadyExistException, if set to true, nothing happens.
     * @throws TableNotExistException          thrown if the target table does not exist
     * @throws TableNotPartitionedException    thrown if the target table is not partitioned
     * @throws PartitionSpecInvalidException   thrown if the given partition spec is invalid
     * @throws PartitionAlreadyExistsException thrown if the target partition already exists
     * @throws CatalogException                in case of any runtime exception
     */
    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, PartitionAlreadyExistsException,
            CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");
        checkNotNull(partition, "Partition cannot be null");

        Table glueTable = glueUtils.getGlueTable(tablePath);
        glueUtils.ensurePartitionedTable(tablePath, glueTable);

        try {
            glueUtils.glueClient.createPartition(
                    glueUtils.instantiateGluePartition(glueTable, partitionSpec, partition, getName()));
        } catch (com.amazonaws.services.glue.model.AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new PartitionAlreadyExistsException (getName(), tablePath, partitionSpec);
            }
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to create partition %s of table %s", partitionSpec, tablePath),
                    e);
        }

    }

    /**
     * Get CatalogPartitionSpec of all partitions of the table.
     *
     * @param tablePath path of the table
     * @return a list of CatalogPartitionSpec of the table
     * @throws TableNotExistException       thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException             in case of any runtime exception
     */
    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    /**
     * Get CatalogPartitionSpec of all partitions that is under the given CatalogPartitionSpec in
     * the table.
     *
     * @param tablePath     path of the table
     * @param partitionSpec the partition spec to list
     * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the
     * table
     * @throws TableNotExistException       thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException             in case of any runtime exception
     */
    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, CatalogException {
        CatalogBaseTable table = getTable(tablePath);
        List<CatalogPartitionSpec> partitionSpecs = new LinkedList<>();
        // todo provide correct implementation
        return partitionSpecs;
    }

    /**
     * Get CatalogPartitionSpec of partitions by expression filters in the table.
     *
     * <p>NOTE: For FieldReferenceExpression, the field index is based on schema of this table
     * instead of partition columns only.
     *
     * <p>The passed in predicates have been translated in conjunctive form.
     *
     * <p>If catalog does not support this interface at present, throw an {@link
     * UnsupportedOperationException} directly. If the catalog does not have a valid filter, throw
     * the {@link UnsupportedOperationException} directly. Planner will fallback to get all
     * partitions and filter by itself.
     *
     * @param tablePath path of the table
     * @param filters   filters to push down filter to catalog
     * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the
     * table
     * @throws TableNotExistException       thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException             in case of any runtime exception
     */
    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    /**
     * Get a partition of the given table. The given partition spec keys and values need to be
     * matched exactly for a result.
     *
     * @param tablePath     path of the table
     * @param partitionSpec partition spec of partition to get
     * @return the requested partition
     * @throws PartitionNotExistException thrown if the partition doesn't exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return null;
    }

    /**
     * Check whether a partition exists or not.
     *
     * @param tablePath     path of the table
     * @param partitionSpec partition spec of the partition to check
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    /**
     * Drop a partition.
     *
     * @param tablePath         path of the table.
     * @param partitionSpec     partition spec of the partition to drop
     * @param ignoreIfNotExists flag to specify behavior if the database does not exist: if set to
     *                          false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException thrown if the target partition does not exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
    }

    /**
     * Alter a partition.
     *
     * @param tablePath         path of the table
     * @param partitionSpec     partition spec of the partition
     * @param newPartition      new partition to replace the old one
     * @param ignoreIfNotExists flag to specify behavior if the database does not exist: if set to
     *                          false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException thrown if the target partition does not exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
    }

    /**
     * Get the statistics of a table.
     *
     * @param tablePath path of the table
     * @return statistics of the given table
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return null;
    }

    /**
     * Get the column statistics of a table.
     *
     * @param tablePath path of the table
     * @return column statistics of the given table
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return null;
    }

    /**
     * Get the statistics of a partition.
     *
     * @param tablePath     path of the table
     * @param partitionSpec partition spec of the partition
     * @return statistics of the given partition
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return null;
    }

    /**
     * Get the column statistics of a partition.
     *
     * @param tablePath     path of the table
     * @param partitionSpec partition spec of the partition
     * @return column statistics of the given partition
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return null;
    }

    /**
     * Update the statistics of a table.
     *
     * @param tablePath         path of the table
     * @param tableStatistics   new statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the table does not exist: if set to
     *                          false, throw an exception, if set to true, nothing happens.
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
    }

    /**
     * Update the column statistics of a table.
     *
     * @param tablePath         path of the table
     * @param columnStatistics  new column statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the table does not exist: if set to
     *                          false, throw an exception, if set to true, nothing happens.
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException       in case of any runtime exception
     */
    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
    }

    /**
     * Update the statistics of a table partition.
     *
     * @param tablePath           path of the table
     * @param partitionSpec       partition spec of the partition
     * @param partitionStatistics new statistics to update
     * @param ignoreIfNotExists   flag to specify behavior if the partition does not exist: if set to
     *                            false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
    }

    /**
     * Update the column statistics of a table partition.
     *
     * @param tablePath         path of the table
     * @param partitionSpec     partition spec of the partition @@param columnStatistics new column
     *                          statistics to update
     * @param columnStatistics
     * @param ignoreIfNotExists flag to specify behavior if the partition does not exist: if set to
     *                          false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException           in case of any runtime exception
     */
    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
    }
}
