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
import org.apache.flink.table.catalog.FunctionLanguage;
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
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.util.StringUtils;

import com.amazonaws.services.glue.model.PartitionInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.GlueResponse;
import software.amazon.awssdk.services.glue.model.ResourceUri;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.glue.GlueCatalogConfig.TABLE_LOCATION_URI;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * A Glue catalog implementation that uses glue catalog. *
 */
public class GlueCatalog extends AbstractCatalog {

    private GlueClient glueClient;

    private String locationUri;

    private AwsProperties awsProperties;

    public static final String DEFAULT_DB = "default";
    public static final String LOCATION_URI = "location_uri";
    private static final Logger LOG = LoggerFactory.getLogger(GlueCatalog.class);

    public GlueCatalog(String name, String defaultDatabase, Map<String, String> glueProperties) {
        super(name, defaultDatabase);
        LOG.info("glue properties:-" + String.valueOf(glueProperties == null));
        checkNotNull(glueProperties);
        initialize(glueProperties);
    }

    public void initialize(Map<String, String> catalogProperties) {
        // setLocationUri
        locationUri = catalogProperties.getOrDefault(LOCATION_URI, "");
        // initialize aws client factories
        awsProperties = new AwsProperties(catalogProperties);

        // create glue client
        this.glueClient = AwsClientFactories.factory(awsProperties).glue();
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
            glueClient.close();
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
                            .locationUri(getLocationUri() + "/" + name)
                            .build();
            CreateDatabaseRequest.Builder createDatabaseRequestBuilder =
                    CreateDatabaseRequest.builder()
                            .databaseInput(databaseInput)
                            .catalogId(awsProperties.getGlueCatalogId());

            try {
                CreateDatabaseResponse response =
                        glueClient.createDatabase(createDatabaseRequestBuilder.build());
                validateGlueResponse(response);
                LOG.info("New Database created. ");
            } catch (GlueException e) {
                throw new DatabaseAlreadyExistException(getName(), name, e);
            }
        }
    }

    void validateGlueResponse(GlueResponse response) {
        if (!response.sdkHttpResponse().isSuccessful()) {
            throw new CatalogException(
                    String.format(
                            "Exception in glue call. RequestId: %s",
                            response.responseMetadata().requestId()));
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

            // Make sure database is empty
            if (isDatabaseEmpty(name)) {
                deleteGlueDatabase(name);
            } else if (cascade) {
                // delete all tables and functions in this database and then delete the database.

                // create a batch table list.
                // apply deleting all tables in batch using client.
                BatchDeleteTableRequest batchTableRequest =
                        BatchDeleteTableRequest.builder()
                                .databaseName(name)
                                .catalogId(awsProperties.getGlueCatalogId())
                                .build();
                BatchDeleteTableResponse response = glueClient.batchDeleteTable(batchTableRequest);
                if (!response.sdkHttpResponse().isSuccessful()) {
                    throw new CatalogException(
                            String.format(
                                    "Glue Table errors:- ",
                                    response.errors().stream()
                                            .map(
                                                    e ->
                                                            "Table: "
                                                                    + e.tableName()
                                                                    + "\nErrorDetail:  "
                                                                    + e.errorDetail()
                                                                    .errorMessage())
                                            .collect(Collectors.joining("\n"))));
                }

                // todo implement function delete
                // get list of all functions
                // build list of function delete request
                // apply deleting all function using client

            } else {
                throw new DatabaseNotEmptyException(getName(), name);
            }
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), name);
        }
    }

    private boolean isDatabaseEmpty(String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        GetTablesRequest tablesRequest =
                GetTablesRequest.builder()
                        .catalogId(awsProperties.getGlueCatalogId())
                        .databaseName(databaseName)
                        .build();
        GetTablesResponse response = glueClient.getTables(tablesRequest);
        if (response.sdkHttpResponse().isSuccessful()) {
            return response.tableList().size() == 0;
        }
        return false;
    }

    /**
     * Delete a database from Glue data catalog when it is empty. *
     */
    private void deleteGlueDatabase(String name) throws CatalogException {
        DeleteDatabaseRequest deleteDatabaseRequest =
                DeleteDatabaseRequest.builder()
                        .name(name)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        DeleteDatabaseResponse response = glueClient.deleteDatabase(deleteDatabaseRequest);
        if (!response.sdkHttpResponse().isSuccessful()) {
            throw new CatalogException("Exception in glue while deleting database.");
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

        CatalogDatabase existingDatabase = getExistingDatabase(name);

        if (existingDatabase != null) {
            if (existingDatabase.getClass() != newDatabase.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Database types don't match. Existing database is '%s' and new database is '%s'.",
                                existingDatabase.getClass().getName(),
                                newDatabase.getClass().getName()));
            }
            // update information in Glue Data catalog
            updateGlueDatabase(name, newDatabase);
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), name);
        }
    }

    private CatalogDatabase getExistingDatabase(String name) {
        GetDatabaseRequest getDatabaseRequest =
                GetDatabaseRequest.builder()
                        .name(name)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        GetDatabaseResponse response = glueClient.getDatabase(getDatabaseRequest);
        if (!response.sdkHttpResponse().isSuccessful()) {
            LOG.warn(
                    "Glue Exception existing database. Client call response :- "
                            + response.sdkHttpResponse().statusText());
            return null;
        }

        return getCatalogDatabase(response);
    }

    private CatalogDatabase getCatalogDatabase(GetDatabaseResponse response) {
        Map<String, String> properties = response.database().parameters();
        String comment = response.database().description();
        return new CatalogDatabaseImpl(properties, comment);
    }

    private void updateGlueDatabase(String databaseName, CatalogDatabase newDatabase)
            throws CatalogException {
        DatabaseInput input = buildGlueDatabaseinput(databaseName, newDatabase);
        UpdateDatabaseRequest updateRequest =
                UpdateDatabaseRequest.builder()
                        .databaseInput(input)
                        .name(databaseName)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        UpdateDatabaseResponse response = glueClient.updateDatabase(updateRequest);
        validateGlueResponse(response);
    }

    DatabaseInput buildGlueDatabaseinput(String name, CatalogDatabase database) {
        return DatabaseInput.builder()
                .parameters(database.getProperties())
                .description(database.getDescription().orElse(null))
                .name(name)
                .build();
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
                            .catalogId(awsProperties.getGlueCatalogId())
                            .build();
            GetDatabasesResponse response = glueClient.getDatabases(databasesRequest);
            validateGlueResponse(response);
            List<String> glueDatabases =
                    response.databaseList().stream()
                            .map(Database::name)
                            .collect(Collectors.toList());
            String dbResultNextToken = response.nextToken();
            if (Optional.ofNullable(dbResultNextToken).isPresent()) {
                do {
                    databasesRequest =
                            GetDatabasesRequest.builder()
                                    .catalogId(awsProperties.getGlueCatalogId())
                                    .nextToken(dbResultNextToken)
                                    .build();
                    response = glueClient.getDatabases(databasesRequest);
                    validateGlueResponse(response);
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
                            .catalogId(awsProperties.getGlueCatalogId())
                            .build();

            GetDatabaseResponse response = glueClient.getDatabase(databasesRequest);
            validateGlueResponse(response);
            return new CatalogDatabaseImpl(
                    getDatabaseProperties(response), response.database().description());

        } catch (GlueException e) {
            throw new CatalogException(e.awsErrorDetails().errorMessage(), e.getCause());
        }
    }

    Map<String, String> getDatabaseProperties(GetDatabaseResponse awsDatabaseResponse) {
        Map<String, String> properties = new HashMap<>(awsDatabaseResponse.database().parameters());
        properties.put("name", awsDatabaseResponse.database().name());
        properties.put("catalogId", awsDatabaseResponse.database().catalogId());
        properties.put(LOCATION_URI, awsDatabaseResponse.database().locationUri());
        properties.put(
                "createTime",
                String.valueOf(awsDatabaseResponse.database().createTime().getEpochSecond()));
        properties.put(
                "sdkFields",
                awsDatabaseResponse.database().sdkFields().stream()
                        .map(field -> field.location().name())
                        .collect(Collectors.joining(",")));
        return properties;
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
                            .catalogId(awsProperties.getGlueCatalogId())
                            .build();
            GetDatabaseResponse response = glueClient.getDatabase(dbRequest);
            return response.sdkHttpResponse().isSuccessful() && response.database().name().equals(databaseName);
        } catch (GlueException e) {
            LOG.warn("Exception occurred in glue connection :\n" + e.getMessage());
            return false;
        }
    }

    // todo 1: fix the uri path
    private String getLocationUri() {
        return locationUri;
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
            createGlueTable(tablePath, table);
            if (isPartitionedTable(tablePath)) {
                // todo :
                // update partition
                // update partition stats
                // update column stats
                updateGlueTablePartition(tablePath, table);
                updateGluePartitionStats(tablePath, table);
                updateGluePartitionColumnStats(tablePath, table);
            }
        }
    }

    private List<Column> getGlueColumnsFromCatalogTable(CatalogBaseTable table) {
        List<Column> glueColumns = new LinkedList<>();
        for (Schema.UnresolvedColumn column : table.getUnresolvedSchema().getColumns()) {
            // Todo : define datatypes from the function
            glueColumns.add(
                    Column.builder()
                            .name(column.getName())
                            .type(getDataTypeMaping(column))
                            .build());
        }
        return glueColumns;
    }

    private StorageDescriptor getTableStorageDescriptor(
            ObjectPath tablePath, Collection<Column> glueColumns, Map<String, String> options) {
        return StorageDescriptor.builder()
                .columns(glueColumns)
                .location(getLocationUri() + "/" + tablePath.getFullName())
                .parameters(options)
                .build();
    }

    private CreateTableRequest createTableRequest(ObjectPath tablePath, TableInput tableInput) {
        return CreateTableRequest.builder()
                .tableInput(tableInput)
                .databaseName(tablePath.getDatabaseName())
                .catalogId(awsProperties.getGlueCatalogId())
                .build();
    }

    private TableInput createTableInput(
            ObjectPath tablePath, CatalogBaseTable table, StorageDescriptor storageDescriptor) {
        return TableInput.builder()
                .tableType(table.getTableKind().name())
                .name(tablePath.getObjectName())
                .description(table.getDescription().orElse(null))
                .storageDescriptor(storageDescriptor)
                .parameters(table.getOptions())
                .build();
    }

    private void createGlueTable(ObjectPath tablePath, CatalogBaseTable table)
            throws TableAlreadyExistException, CatalogException {

        Collection<Column> glueColumns = getGlueColumnsFromCatalogTable(table);
        StorageDescriptor storageDescriptor =
                getTableStorageDescriptor(tablePath, glueColumns, table.getOptions());
        TableInput tableInput = createTableInput(tablePath, table, storageDescriptor);
        CreateTableRequest tableRequest = createTableRequest(tablePath, tableInput);
        CreateTableResponse response = glueClient.createTable(tableRequest);
        validateGlueResponse(response);
    }

    private boolean isPartitionedTable(ObjectPath tablePath) {
        CatalogBaseTable table = null;
        try {
            table = getTable(tablePath);
        } catch (TableNotExistException e) {
            return false;
        }

        return (table instanceof CatalogTable) && ((CatalogTable) table).isPartitioned();
    }

    private void updateGlueTablePartition(ObjectPath tablePath, CatalogBaseTable table) {
        // todo
    }

    private void updateGluePartitionStats(ObjectPath tablePath, CatalogBaseTable table) {
        // todo

    }

    private void updateGluePartitionColumnStats(ObjectPath tablePath, CatalogBaseTable table) {
        // todo
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
            Collection<Column> columns = getGlueColumnsFromCatalogTable(newTable);
            StorageDescriptor storageDesc =
                    getTableStorageDescriptor(tablePath, columns, newTable.getOptions());
            TableInput tableInput = createTableInput(tablePath, newTable, storageDesc);
            UpdateTableRequest tableRequest =
                    UpdateTableRequest.builder().tableInput(tableInput).build();
            UpdateTableResponse response = glueClient.updateTable(tableRequest);
            validateGlueResponse(response);
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
            deleteGlueTable(tablePath);
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    private void deleteGlueTable(ObjectPath tablePath) {
        DeleteTableRequest.Builder tableRequestBuilder =
                DeleteTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(awsProperties.getGlueCatalogId());

        DeleteTableResponse response = glueClient.deleteTable(tableRequestBuilder.build());
        validateGlueResponse(response);
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
                updateGlueTable(tablePath, newTablePath);
                // todo table statistics

                // todo table column statistics

                // todo partitions

                // todo partition statistics

            }
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    private void updateGlueTable(ObjectPath existingTablePath, ObjectPath newTablePath)
            throws TableNotExistException {
        CatalogBaseTable exisitingTable = getTable(existingTablePath);
        StorageDescriptor storageDescriptor =
                getTableStorageDescriptor(
                        newTablePath,
                        getGlueColumnsFromCatalogTable(exisitingTable),
                        exisitingTable.getOptions());
        TableInput tableInput = createTableInput(newTablePath, exisitingTable, storageDescriptor);
        UpdateTableRequest tableRequest =
                UpdateTableRequest.builder()
                        .databaseName(newTablePath.getDatabaseName())
                        .tableInput(tableInput)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        UpdateTableResponse response = glueClient.updateTable(tableRequest);
        validateGlueResponse(response);
    }

    /**
     * Get names of all tables and views under this database. An empty list is returned if none
     * exists.
     *
     * @param databaseName
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
        return getGlueTableList(databaseName, "CatalogTable");
    }

    private List<String> getGlueTableList(String databaseName, String type) {
        GetTablesRequest tablesRequest =
                GetTablesRequest.builder()
                        .databaseName(databaseName)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();

        GetTablesResponse response = glueClient.getTables(tablesRequest);
        validateGlueResponse(response);
        List<Table> finalTableList = new ArrayList<>(response.tableList());
        String tableResultNextToken = response.nextToken();

        if (Optional.ofNullable(tableResultNextToken).isPresent()) {
            do {
                // creating a new GetTablesResult using next token.
                tablesRequest =
                        GetTablesRequest.builder()
                                .databaseName(databaseName)
                                .nextToken(tableResultNextToken)
                                .catalogId(awsProperties.getGlueCatalogId())
                                .build();
                response = glueClient.getTables(tablesRequest);
                finalTableList.addAll(response.tableList());
                tableResultNextToken = response.nextToken();
            } while (Optional.ofNullable(tableResultNextToken).isPresent());
        }
        if (type.toLowerCase().equals("catalogtable")) {
            return finalTableList.stream()
                    .filter(
                            table ->
                                    Objects.equals(
                                            table.tableType().toLowerCase(Locale.ROOT), "table"))
                    .map(Table::name)
                    .collect(Collectors.toList());
        } else {
            return finalTableList.stream()
                    .filter(
                            table ->
                                    !Objects.equals(
                                            table.tableType().toLowerCase(Locale.ROOT), "table"))
                    .map(Table::name)
                    .collect(Collectors.toList());
        }
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
        return getGlueTableList(databaseName, "CatalogView");
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
            Table glueTable = getGlueTable(tablePath);
            Tuple2<Schema, String> schemaInfo = getSchemaFromGlueTable(glueTable);
            List<String> patitionKeys = getPartitionKeysFromGlueTable(glueTable);
            return CatalogTable.of(
                    schemaInfo.f0, schemaInfo.f1, patitionKeys, glueTable.parameters());
        }
    }

    private Table getGlueTable(ObjectPath tablePath) {
        GetTableRequest tablesRequest =
                GetTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        GetTableResponse response = glueClient.getTable(tablesRequest);
        validateGlueResponse(response);
        return response.table();
    }

    private Tuple2<Schema, String> getSchemaFromGlueTable(Table glueTable) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        StringBuilder comment = new StringBuilder();
        for (Column col : glueTable.storageDescriptor().columns()) {
            schemaBuilder.column(col.name(), col.type());
            comment.append(col.name()).append(":").append(col.comment()).append("\n");
        }
        return new Tuple2<>(schemaBuilder.build(), comment.toString());
    }

    private List<String> getPartitionKeysFromGlueTable(Table glueTable) {
        return glueTable.partitionKeys().stream().map(Column::name).collect(Collectors.toList());
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

        return databaseExists(tablePath.getDatabaseName()) && glueTableExists(tablePath);
    }

    private boolean glueTableExists(ObjectPath tablePath) {
        GetTableRequest tableRequest =
                GetTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        GetTableResponse response = glueClient.getTable(tableRequest);
        validateGlueResponse(response);
        return response.table().name().equals(tablePath.getObjectName());
    }

    private String getDataTypeMaping(Schema.UnresolvedColumn column) {
        // todo provide correct implementation
        return "string";
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
            createGlueFunction(functionPath, function);
        }
    }

    private static final String FLINK_SCALA_FUNCTION_PREFIX = "flink:scala:";
    private static final String FLINK_PYTHON_FUNCTION_PREFIX = "flink:python:";

    private void createGlueFunction(ObjectPath functionPath, CatalogFunction function) {
        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, function);
        CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest = CreateUserDefinedFunctionRequest.builder()
                .databaseName(functionPath.getDatabaseName())
                .catalogId(awsProperties.getGlueCatalogId()).functionInput(functionInput).build();
        CreateUserDefinedFunctionResponse response = glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest);
        validateGlueResponse(response);
    }

    private UserDefinedFunctionInput createFunctionInput(ObjectPath functionPath, CatalogFunction function) {
        // Glue Function does not have properties map
        // thus, use a prefix in class name to distinguish Java/Scala and Python functions
        String functionClassName;
        if (function.getFunctionLanguage() == FunctionLanguage.JAVA) {
            functionClassName = function.getClassName();
        } else if (function.getFunctionLanguage() == FunctionLanguage.SCALA) {
            functionClassName = FLINK_SCALA_FUNCTION_PREFIX + function.getClassName();
        } else if (function.getFunctionLanguage() == FunctionLanguage.PYTHON) {
            functionClassName = FLINK_PYTHON_FUNCTION_PREFIX + function.getClassName();
        } else {
            throw new UnsupportedOperationException(
                    "HiveCatalog supports only creating"
                            + " JAVA/SCALA or PYTHON based function for now");
        }
        Collection<software.amazon.awssdk.services.glue.model.ResourceUri> resourceUris = new LinkedList<>();
        for (org.apache.flink.table.resource.ResourceUri resourceUri : function.getFunctionResources()) {
            switch (resourceUri.getResourceType()) {
                case JAR:

                case FILE:
                case ARCHIVE:
                default:
            }
            resourceUris.add(ResourceUri.builder().resourceType(resourceUri.getResourceType().name()).uri(resourceUri.getUri()).build());
        }
        return UserDefinedFunctionInput.builder().functionName(functionPath.getObjectName())
                .className(function.getClassName())
                .resourceUris(resourceUris)
                .build();
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

            alterFunctionInGlue(functionPath, newFunction.copy());
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    private void alterFunctionInGlue(ObjectPath functionPath, CatalogFunction newFunction) {
        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, newFunction);
        UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest = UpdateUserDefinedFunctionRequest.builder()
                .functionName(functionPath.getObjectName()).databaseName(functionPath.getDatabaseName()).catalogId(awsProperties.getGlueCatalogId()).functionInput(functionInput).build();
        UpdateUserDefinedFunctionResponse response = glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
        validateGlueResponse(response);
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
            removeGlueFunction(functionPath);
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    private void removeGlueFunction(ObjectPath functionPath) {
        DeleteUserDefinedFunctionRequest request = DeleteUserDefinedFunctionRequest.builder()
                .catalogId(awsProperties.getGlueCatalogId())
                .functionName(functionPath.getObjectName())
                .databaseName(functionPath.getDatabaseName())
                .build();
        DeleteUserDefinedFunctionResponse response = glueClient.deleteUserDefinedFunction(request);
        validateGlueResponse(response);
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

        return listGlueFunctions(databaseName);
    }

    private List<String> listGlueFunctions(String databaseName) {
        GetUserDefinedFunctionsRequest request = GetUserDefinedFunctionsRequest.builder().databaseName(databaseName).catalogId(awsProperties.getGlueCatalogId()).build();
        GetUserDefinedFunctionsResponse response = glueClient.getUserDefinedFunctions(request);
        validateGlueResponse(response);
        String token = response.nextToken();
        List<String> glueFunctions = response
                .userDefinedFunctions()
                .stream()
                .map(
                        UserDefinedFunction::functionName)
                .collect(Collectors.toCollection(LinkedList::new));
        if (Optional.ofNullable(token).isPresent()) {
            do {
                request = GetUserDefinedFunctionsRequest.builder()
                        .catalogId(awsProperties.getGlueCatalogId())
                        .nextToken(token)
                        .build();
                response = glueClient.getUserDefinedFunctions(request);
                validateGlueResponse(response);
                glueFunctions.addAll(response
                        .userDefinedFunctions()
                        .stream()
                        .map(
                                UserDefinedFunction::functionName)
                        .collect(Collectors.toCollection(LinkedList::new)));
                token = response.nextToken();
            } while (Optional.ofNullable(token).isPresent());
        }
        return glueFunctions;
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
        GetUserDefinedFunctionRequest request = GetUserDefinedFunctionRequest.builder().catalogId(
                awsProperties.getGlueCatalogId()).databaseName(functionPath.getDatabaseName()).functionName(functionPath.getObjectName()).build();
        GetUserDefinedFunctionResponse response = glueClient.getUserDefinedFunction(request);
        validateGlueResponse(response);
        List<org.apache.flink.table.resource.ResourceUri> resourceUris = new LinkedList<>();
        for (ResourceUri resourceUri : response.userDefinedFunction().resourceUris()) {
            resourceUris.add(new org.apache.flink.table.resource.ResourceUri(ResourceType.valueOf(resourceUri.resourceType().name()), resourceUri.uri()));
        }

        return new CatalogFunctionImpl(response.userDefinedFunction().className(), getFunctionalLanguage(response.userDefinedFunction()), resourceUris);
    }

    private FunctionLanguage getFunctionalLanguage(UserDefinedFunction glueFunction) {
        // todo implement getfuntionalLanguage
        switch (glueFunction.functionName().toLowerCase()) {
//            case FunctionLanguage.JAVA:
//
//            case FunctionLanguage.PYTHON:
//
//            case FunctionLanguage.SCALA:

            default:
                throw new CatalogException("Invalid Functional Language");
        }
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

        return databaseExists(functionPath.getDatabaseName()) && isGlueFunctionExists(functionPath);
    }

    private boolean isGlueFunctionExists(ObjectPath functionPath) {
        GetUserDefinedFunctionRequest request = GetUserDefinedFunctionRequest.builder()
                .functionName(functionPath.getObjectName())
                .catalogId(awsProperties.getGlueCatalogId())
                .databaseName(functionPath.getDatabaseName()).build();
        GetUserDefinedFunctionResponse response = glueClient.getUserDefinedFunction(request);
        validateGlueResponse(response);
        return response.userDefinedFunction().functionName().equals(functionPath.getObjectName());
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

        Table glueTable = getGlueTable(tablePath);
        ensurePartitionedTable(tablePath, glueTable);

        try {
            glueClient.createPartition(instantiateGluePartition(glueTable, partitionSpec, partition));
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

    private void ensurePartitionedTable(ObjectPath tablePath, Table glueTable) {
        // todo provide implemetation
    }

    private software.amazon.awssdk.services.glue.model.CreatePartitionRequest instantiateGluePartition(
            Table glueTable, CatalogPartitionSpec partitionSpec, CatalogPartition catalogPartition)
            throws PartitionSpecInvalidException {
        List<String> partCols = getFieldNames(glueTable);
        List<String> partValues =
                getOrderedFullPartitionValues(
                        partitionSpec,
                        partCols,
                        new ObjectPath(glueTable.databaseName(), glueTable.name()));
        // validate partition values
        for (int i = 0; i < partCols.size(); i++) {
            if (isNullOrWhitespaceOnly(partValues.get(i))) {
                throw new PartitionSpecInvalidException(
                        getName(),
                        partCols,
                        new ObjectPath(glueTable.databaseName(), glueTable.name()),
                        partitionSpec);
            }
        }
        // TODO: handle GenericCatalogPartition
        StorageDescriptor.Builder sd = glueTable.storageDescriptor().toBuilder();
        sd.location(catalogPartition.getProperties().remove(TABLE_LOCATION_URI));

        Map<String, String> properties = new HashMap<>(catalogPartition.getProperties());
        String comment = catalogPartition.getComment();
        if (comment != null) {
            properties.put(GlueCatalogConfig.COMMENT, comment);
        }
        return createGluePartition(glueTable, partValues, sd.build(), properties);
    }

    private List<String> getFieldNames(Table gluetable) {
        List<String> fields = new ArrayList<>();
        // todo provide implementations
        return fields;
    }

    private List<String> getOrderedFullPartitionValues(CatalogPartitionSpec partitionSpec, List<String> partCols, ObjectPath tablePath) {
        // todo provide implementations
        return new ArrayList<>();
    }

    private software.amazon.awssdk.services.glue.model.CreatePartitionRequest createGluePartition(Table glueTable,
                                                                                                  List<String> partValues,
                                                                                                  StorageDescriptor sd, Map<String, String> properties) {
        // todo implement properly
        return software.amazon.awssdk.services.glue.model.CreatePartitionRequest.builder().build();
    }

    public static PartitionInput convertToPartitionInput(com.amazonaws.services.glue.model.Partition src) {
        PartitionInput partitionInput = new PartitionInput();

        partitionInput.setLastAccessTime(src.getLastAccessTime());
        partitionInput.setParameters(src.getParameters());
        partitionInput.setStorageDescriptor(src.getStorageDescriptor());
        partitionInput.setValues(src.getValues());

        return partitionInput;
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
