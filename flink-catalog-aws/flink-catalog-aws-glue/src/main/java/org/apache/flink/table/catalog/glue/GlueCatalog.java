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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
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
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.operator.GlueDatabaseOperator;
import org.apache.flink.table.catalog.glue.operator.GlueFunctionOperator;
import org.apache.flink.table.catalog.glue.operator.GluePartitionOperator;
import org.apache.flink.table.catalog.glue.operator.GlueTableOperator;
import org.apache.flink.table.catalog.glue.util.GlueUtils;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Glue catalog implementation that uses AWS Glue Data Catalog as persistence at backend. */
@PublicEvolving
public class GlueCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GlueCatalog.class);

    /** instance of GlueOperator to facilitate glue related actions. */
    public GlueDatabaseOperator glueDatabaseOperator;

    public GlueTableOperator glueTableOperator;
    public GluePartitionOperator gluePartitionOperator;
    public GlueFunctionOperator glueFunctionOperator;

    public GlueClient glueClient;

    /** Default database name if not passed as part of catalog. */
    public static final String DEFAULT_DB = "default";

    public GlueCatalog(
            String catalogName,
            String databaseName,
            ReadableConfig catalogConfig,
            Properties glueClientProperties) {
        super(catalogName, databaseName);
        checkNotNull(catalogConfig, "Catalog config cannot be null.");
        String glueCatalogId =
                String.valueOf(
                        catalogConfig.getOptional(GlueCatalogOptions.GLUE_CATALOG_ID).orElse(null));
        glueClient = createClient(glueClientProperties);
        this.glueDatabaseOperator = new GlueDatabaseOperator(getName(), glueClient, glueCatalogId);
        this.glueTableOperator = new GlueTableOperator(getName(), glueClient, glueCatalogId);
        this.gluePartitionOperator =
                new GluePartitionOperator(getName(), glueClient, glueCatalogId);
        this.glueFunctionOperator = new GlueFunctionOperator(getName(), glueClient, glueCatalogId);
    }

    private static GlueClient createClient(Properties glueClientProperties) {
        return AWSClientUtil.createAwsSyncClient(
                glueClientProperties,
                AWSGeneralUtil.createSyncHttpClient(
                        glueClientProperties, ApacheHttpClient.builder()),
                GlueClient.builder(),
                GlueCatalogConstants.BASE_GLUE_USER_AGENT_PREFIX_FORMAT,
                GlueCatalogConstants.GLUE_CLIENT_USER_AGENT_PREFIX);
    }

    @VisibleForTesting
    public GlueCatalog(
            String catalogName,
            String databaseName,
            GlueClient glueClient,
            GlueDatabaseOperator glueDatabaseOperator,
            GlueTableOperator glueTableOperator,
            GluePartitionOperator gluePartitionOperator,
            GlueFunctionOperator glueFunctionOperator) {
        super(catalogName, databaseName);
        this.glueClient = glueClient;
        this.glueDatabaseOperator = glueDatabaseOperator;
        this.glueTableOperator = glueTableOperator;
        this.gluePartitionOperator = gluePartitionOperator;
        this.glueFunctionOperator = glueFunctionOperator;
    }

    /**
     * Open the catalog. Used for any required preparation in initialization phase.
     *
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void open() throws CatalogException {}

    /**
     * Close the catalog when it is no longer needed and release any resource that it might be
     * holding.
     *
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void close() throws CatalogException {
        try {
            glueClient.close();
        } catch (Exception e) {
            throw new CatalogException("Glue Client is not closed properly!", e);
        }
    }

    // ------ databases ------

    /**
     * Create a database.
     *
     * @param databaseName Name of the database to be created
     * @param database The database definition
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *     nothing.
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *     is false
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        checkNotNull(database, "Database cannot be null.");
        databaseName = GlueUtils.getGlueConventionalName(databaseName);
        if (databaseExists(databaseName) && !ignoreIfExists) {
            throw new DatabaseAlreadyExistException(getName(), databaseName);
        } else {
            glueDatabaseOperator.createGlueDatabase(databaseName, database);
            LOG.info("Created Database {}.", databaseName);
        }
    }

    /**
     * Drop a database.
     *
     * @param databaseName Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @param cascade Flag to specify behavior when the database contains table or function: if set
     *     to true, delete all tables and functions in the database and then delete the database, if
     *     set to false, throw an exception.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws DatabaseNotEmptyException if the given database is not empty and isRestrict is true
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        databaseName = GlueUtils.getGlueConventionalName(databaseName);
        if (databaseExists(databaseName)) {
            if (cascade) {
                List<String> tables = listTables(databaseName);
                if (!tables.isEmpty()) {
                    glueDatabaseOperator.deleteTablesFromDatabase(databaseName, tables);
                    LOG.info("{} Tables deleted from Database {}.", tables.size(), databaseName);
                }
                List<String> functions = listFunctions(databaseName);
                if (!functions.isEmpty()) {
                    glueDatabaseOperator.deleteFunctionsFromDatabase(databaseName, functions);
                    LOG.info(
                            "{} Functions deleted from Database {}.",
                            functions.size(),
                            databaseName);
                }
            }
            if (!isDatabaseEmpty(databaseName)) {
                throw new DatabaseNotEmptyException(getName(), databaseName);
            }
            glueDatabaseOperator.dropGlueDatabase(databaseName);
            LOG.info("Dropped Database: {}.", databaseName);
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    /**
     * Modify existing database.
     *
     * @param name Name of the database to be modified
     * @param newDatabase The new database definition
     * @param ignoreIfNotExists Flag to specify behavior when the given database does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(name),
                "Database name cannot be null or empty.");
        checkNotNull(newDatabase, "Database cannot be null.");
        name = GlueUtils.getGlueConventionalName(name);
        try {
            CatalogDatabase existingDatabase = glueDatabaseOperator.getDatabase(name);
            if (existingDatabase != null) {
                if (existingDatabase.getClass() != newDatabase.getClass()) {
                    throw new CatalogException(
                            String.format(
                                    "Database types don't match. Existing database is '%s' and new database is '%s'.",
                                    existingDatabase.getClass().getName(),
                                    newDatabase.getClass().getName()));
                }
                glueDatabaseOperator.updateGlueDatabase(name, newDatabase);
            }
        } catch (DatabaseNotExistException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
        }
    }

    /**
     * Get list of databases in catalog.
     *
     * @return a list of the names of all databases
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<String> listDatabases() throws CatalogException {
        return glueDatabaseOperator.listGlueDatabases();
    }

    /**
     * Get a database from this catalog.
     *
     * @param databaseName Name of the database
     * @return The requested database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        databaseName = GlueUtils.getGlueConventionalName(databaseName);
        return glueDatabaseOperator.getDatabase(databaseName);
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
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        try {
            return getDatabase(databaseName) != null;
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    /**
     * Check if database is empty. i.e. it should not contain 1. table 2. functions
     *
     * @param databaseName name of database.
     * @return boolean True/False based on the content of database.
     * @throws CatalogException Any Exception thrown due to glue error
     */
    public boolean isDatabaseEmpty(String databaseName) throws CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "Database name cannot be null or empty.");
        GlueUtils.validate(databaseName);
        GetTablesRequest tablesRequest =
                GetTablesRequest.builder()
                        .catalogId(glueTableOperator.getGlueCatalogId())
                        .databaseName(databaseName)
                        .maxResults(1)
                        .build();
        GetUserDefinedFunctionsRequest functionsRequest =
                GetUserDefinedFunctionsRequest.builder()
                        .databaseName(databaseName)
                        .catalogId(glueFunctionOperator.getGlueCatalogId())
                        .maxResults(1)
                        .build();
        try {
            GetTablesResponse tableResponse = glueClient.getTables(tablesRequest);
            GetUserDefinedFunctionsResponse functionResponse =
                    glueClient.getUserDefinedFunctions(functionsRequest);
            if (tableResponse.sdkHttpResponse().isSuccessful()
                    && functionResponse.sdkHttpResponse().isSuccessful()) {
                return tableResponse.tableList().isEmpty()
                        && functionResponse.userDefinedFunctions().isEmpty();
            } else {
                String errorMessage =
                        String.format(
                                "Error checking if database '%s' is empty. Glue API requests failed with the following IDs:\n"
                                        + "1. GetTables: %s\n"
                                        + "2. GetUserDefinedFunctions: %s\n"
                                        + "Please check the Glue service logs for more details.",
                                databaseName,
                                tableResponse.responseMetadata().requestId(),
                                functionResponse.responseMetadata().requestId());
                throw new CatalogException(errorMessage);
            }
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
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
     * @param tablePath path of the table or view to be created
     * @param table the table definition
     * @param ignoreIfExists flag to specify behavior when a table or view already exists at the
     *     given path: if set to false, it throws a TableAlreadyExistException, if set to true, do
     *     nothing.
     * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException if the database in tablePath doesn't exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkNotNull(table, "table cannot be null");
        checkArgument(table instanceof ResolvedCatalogBaseTable, "table should be resolved");
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
        } else {
            glueTableOperator.createGlueTable(tablePath, table);
        }
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
     * @param tablePath path of the table or view to be modified
     * @param newTable the new table definition
     * @param ignoreIfNotExists flag to specify behavior when the table or view does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null");
        checkNotNull(newTable, "Table cannot be null.");
        CatalogBaseTable existingTable = getTable(tablePath);
        if (existingTable != null) {
            if (existingTable.getTableKind() != newTable.getTableKind()) {
                throw new CatalogException(
                        String.format(
                                "Table types don't match. Existing table is '%s' and new table is '%s'.",
                                existingTable.getTableKind(), newTable.getTableKind()));
            }
            glueTableOperator.alterGlueTable(tablePath, newTable);
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    // ------ tables and views ------

    /**
     * Drop a table or view.
     *
     * @param tablePath Path of the table or view to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table or view does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null");
        if (tableExists(tablePath)) {
            glueTableOperator.dropGlueTable(tablePath);
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    /**
     * Rename an existing table or view.
     *
     * @param tablePath Path of the table or view to be renamed
     * @param newTableName the new name of the table or view
     * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {

        checkNotNull(tablePath, "TablePath cannot be null");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(newTableName),
                "Table name cannot be null or empty.");

        if (tableExists(tablePath)) {
            ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
            if (tableExists(newTablePath)) {
                throw new TableAlreadyExistException(getName(), newTablePath);
            }
            glueTableOperator.renameGlueTable(tablePath, newTablePath);
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
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        List<String> tableAndViewList =
                glueTableOperator.getGlueTableList(
                        databaseName, CatalogBaseTable.TableKind.TABLE.name());
        tableAndViewList.addAll(listViews(databaseName));
        return tableAndViewList;
    }

    /**
     * Get names of all views under this database. An empty list is returned if none exists.
     *
     * @param databaseName the name of the given database
     * @return a list of the names of all views in the given database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return glueTableOperator.getGlueTableList(
                databaseName, CatalogBaseTable.TableKind.VIEW.name());
    }

    /**
     * Returns a {@link CatalogTable} or {@link CatalogView} identified by the given {@link
     * ObjectPath}. The framework will resolve the metadata objects when necessary.
     *
     * @param tablePath Path of the table or view
     * @return The requested table or view
     * @throws TableNotExistException if the target does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null");
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        return glueTableOperator.getCatalogBaseTableFromGlueTable(
                glueTableOperator.getGlueTable(tablePath));
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
        checkNotNull(tablePath, "TablePath cannot be null.");
        return databaseExists(tablePath.getDatabaseName())
                && glueTableOperator.glueTableExists(tablePath);
    }

    // ------ functions ------
    /**
     * Create a function. Function name should be handled in a case-insensitive way.
     *
     * @param path path of the function
     * @param function the function to be created
     * @param ignoreIfExists flag to specify behavior if a function with the given name already
     *     exists: if set to false, it throws a FunctionAlreadyExistException, if set to true,
     *     nothing happens.
     * @throws FunctionAlreadyExistException if the function already exist
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void createFunction(ObjectPath path, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(path, "Function path cannot be null.");
        checkNotNull(function, "Catalog Function cannot be null.");
        ObjectPath functionPath = normalize(path);
        if (!databaseExists(functionPath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), functionPath.getDatabaseName());
        }
        if (!functionExists(functionPath)) {
            glueFunctionOperator.createGlueFunction(functionPath, function);
        } else {
            if (!ignoreIfExists) {
                throw new FunctionAlreadyExistException(getName(), functionPath);
            }
        }
    }

    public ObjectPath normalize(ObjectPath path) {
        return new ObjectPath(
                path.getDatabaseName(), FunctionIdentifier.normalizeName(path.getObjectName()));
    }

    /**
     * Modify an existing function. Function name should be handled in a case-insensitive way.
     *
     * @param path path of the function
     * @param newFunction the function to be modified
     * @param ignoreIfNotExists flag to specify behavior if the function does not exist: if set to
     *     false, throw an exception if set to true, nothing happens
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterFunction(
            ObjectPath path, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path, "Function path cannot be null.");
        checkNotNull(newFunction, "Catalog Function cannot be null.");
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
            glueFunctionOperator.alterGlueFunction(functionPath, newFunction);
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    /**
     * Drop a function. Function name should be handled in a case-insensitive way.
     *
     * @param path path of the function to be dropped
     * @param ignoreIfNotExists flag to specify behavior if the function does not exist: if set to
     *     false, throw an exception if set to true, nothing happens
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void dropFunction(ObjectPath path, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path, "Function path cannot be null.");
        ObjectPath functionPath = normalize(path);
        if (functionExists(functionPath)) {
            glueFunctionOperator.dropGlueFunction(functionPath);
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
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<String> listFunctions(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        databaseName = GlueUtils.getGlueConventionalName(databaseName);
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return glueFunctionOperator.listGlueFunctions(databaseName);
    }

    /**
     * Get the function. Function name should be handled in a case-insensitive way.
     *
     * @param path path of the function
     * @return the requested function
     * @throws FunctionNotExistException if the function does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogFunction getFunction(ObjectPath path)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path, "Function path cannot be null.");
        ObjectPath functionPath = normalize(path);
        if (!functionExists(functionPath)) {
            throw new FunctionNotExistException(getName(), functionPath);
        } else {
            return glueFunctionOperator.getGlueFunction(functionPath);
        }
    }

    /**
     * Check whether a function exists or not. Function name should be handled in a case-insensitive
     * way.
     *
     * @param path path of the function
     * @return true if the function exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public boolean functionExists(ObjectPath path) throws CatalogException {
        checkNotNull(path, "Function path cannot be null.");
        ObjectPath functionPath = normalize(path);
        return databaseExists(functionPath.getDatabaseName())
                && glueFunctionOperator.glueFunctionExists(functionPath);
    }

    // ------ partitions ------
    /**
     * Create a partition.
     *
     * @param tablePath path of the table.
     * @param partitionSpec partition spec of the partition
     * @param partition the partition to add.
     * @param ignoreIfExists flag to specify behavior if a table with the given name already exists:
     *     if set to false, it throws a TableAlreadyExistException, if set to true, nothing happens.
     * @throws TableNotExistException thrown if the target table does not exist
     * @throws TableNotPartitionedException thrown if the target table is not partitioned
     * @throws PartitionSpecInvalidException thrown if the given partition spec is invalid
     * @throws PartitionAlreadyExistsException thrown if the target partition already exists
     * @throws CatalogException in case of any runtime exception
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
        checkNotNull(tablePath, "TablePath cannot be null.");
        checkNotNull(partitionSpec, "PartitionSpec cannot be null.");
        checkNotNull(partition, "Partition cannot be null.");
        Table glueTable = glueTableOperator.getGlueTable(tablePath);
        gluePartitionOperator.ensurePartitionedTable(tablePath, glueTable);
        if (!partitionExists(tablePath, partitionSpec)) {
            gluePartitionOperator.createGluePartition(glueTable, partitionSpec, partition);
        } else {
            if (!ignoreIfExists) {
                throw new PartitionAlreadyExistsException(getName(), tablePath, partitionSpec);
            }
        }
    }

    /**
     * Get CatalogPartitionSpec of all partitions of the table.
     *
     * @param tablePath path of the table
     * @return a list of CatalogPartitionSpec of the table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null");
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        if (isPartitionedTable(tablePath)) {
            return gluePartitionOperator.listPartitions(tablePath);
        }
        throw new TableNotPartitionedException(getName(), tablePath);
    }

    public boolean isPartitionedTable(ObjectPath tablePath) {
        CatalogBaseTable table;
        try {
            table = getTable(tablePath);
            if (table instanceof CatalogTable) {
                CatalogTable catalogTable = (CatalogTable) table;
                return catalogTable.isPartitioned();
            }
            return false;
        } catch (TableNotExistException e) {
            throw new CatalogException(tablePath.getFullName() + " Table doesn't Exists.", e);
        }
    }

    /**
     * Get CatalogPartitionSpec of all partitions that is under the given CatalogPartitionSpec in
     * the table.
     *
     * @param tablePath path of the table
     * @param partitionSpec the partition spec to list
     * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the
     *     table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null.");
        checkNotNull(partitionSpec, "Partition spec cannot be null.");
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        if (!isPartitionedTable(tablePath)) {
            throw new TableNotPartitionedException(getName(), tablePath);
        }
        return gluePartitionOperator.listPartitions(tablePath, partitionSpec);
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
     * the {@link UnsupportedOperationException} directly. Planner will fall back to get all
     * partitions and filter by itself.
     *
     * @param tablePath path of the table
     * @param filters filters to push down filter to catalog
     * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the
     *     table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null");
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        if (!isPartitionedTable(tablePath)) {
            throw new TableNotPartitionedException(getName(), tablePath);
        }
        return gluePartitionOperator.listGluePartitionsByFilter(tablePath, filters);
    }

    /**
     * Get a partition of the given table. The given partition spec keys and values need to be
     * matched exactly for a result.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of partition to get
     * @return the requested partition
     * @throws PartitionNotExistException thrown if the partition doesn't exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null.");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null.");
        Table glueTable;
        try {
            glueTable = glueTableOperator.getGlueTable(tablePath);
        } catch (TableNotExistException e) {
            throw new CatalogException("Table doesn't exist in Glue Data Catalog", e);
        }
        Partition gluePartition = gluePartitionOperator.getGluePartition(glueTable, partitionSpec);
        if (gluePartition == null) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
        Map<String, String> catalogPartitionProperties =
                new HashMap<>(gluePartition.storageDescriptor().parameters());
        String comment = catalogPartitionProperties.remove(GlueCatalogConstants.COMMENT);
        return new CatalogPartitionImpl(catalogPartitionProperties, comment);
    }

    /**
     * Check whether a partition exists or not.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition to check
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null");
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new CatalogException(tablePath.getDatabaseName() + " Database doesn't exists.");
        }
        try {
            Table glueTable = glueTableOperator.getGlueTable(tablePath);
            return gluePartitionOperator.gluePartitionExists(tablePath, glueTable, partitionSpec);
        } catch (TableNotExistException e) {
            throw new CatalogException(
                    tablePath.getFullName() + " Table doesn't Exists in Glue Data Catalog.", e);
        }
    }

    /**
     * Drop a partition.
     *
     * @param tablePath path of the table.
     * @param partitionSpec partition spec of the partition to drop
     * @param ignoreIfNotExists flag to specify behavior if the database does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException thrown if the target partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null.");
        checkNotNull(partitionSpec, "PartitionSpec cannot be null.");
        if (partitionExists(tablePath, partitionSpec)) {
            Table glueTable;
            try {
                glueTable = glueTableOperator.getGlueTable(tablePath);
            } catch (TableNotExistException e) {
                throw new CatalogException(tablePath.getFullName() + " Table doesn't exists.", e);
            }
            gluePartitionOperator.dropGluePartition(tablePath, partitionSpec, glueTable);
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }

    /**
     * Alter a partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @param newPartition new partition to replace the old one
     * @param ignoreIfNotExists flag to specify behavior if the database does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException thrown if the target partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath, "TablePath cannot be null.");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null.");
        checkNotNull(newPartition, "New partition cannot be null.");
        CatalogPartition existingPartition = getPartition(tablePath, partitionSpec);
        if (existingPartition != null) {
            try {
                Table glueTable = glueTableOperator.getGlueTable(tablePath);
                gluePartitionOperator.alterGluePartition(
                        tablePath, glueTable, partitionSpec, newPartition);
            } catch (TableNotExistException e) {
                throw new CatalogException("Table Not Found in Glue data catalog", e);
            } catch (PartitionSpecInvalidException e) {
                throw new CatalogException("Invalid Partition Spec", e);
            }
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }

    /**
     * Get the statistics of a table.
     *
     * @param tablePath path of the table
     * @return statistics of the given table
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    /**
     * Get the column statistics of a table.
     *
     * @param tablePath path of the table
     * @return column statistics of the given table
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    /**
     * Get the statistics of a partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @return statistics of the given partition
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    /**
     * Get the column statistics of a partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @return column statistics of the given partition
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    /**
     * Update the statistics of a table.
     *
     * @param tablePath path of the table
     * @param tableStatistics new statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the table does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Operation with Statistics not supported.");
    }

    /**
     * Update the column statistics of a table.
     *
     * @param tablePath path of the table
     * @param columnStatistics new column statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the table does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException("Operation with Statistics not supported.");
    }

    /**
     * Update the statistics of a table partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @param partitionStatistics new statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the partition does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Operation with Statistics not supported.");
    }

    /**
     * Update the column statistics of a table partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition @@param columnStatistics new column
     *     statistics to update
     * @param columnStatistics column related statistics
     * @param ignoreIfNotExists flag to specify behavior if the partition does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Operation with Statistics not supported.");
    }
}
