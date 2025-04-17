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

package org.apache.flink.table.catalog.glue.operations;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles all table-related operations for the Glue catalog.
 * Provides functionality for checking existence, listing, creating, getting, and dropping tables in AWS Glue.
 */
public class GlueTableOperations extends AbstractGlueOperations {

    /**
     * Logger for logging table operations.
     */
    private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperations.class);

    /**
     * Constructor for GlueTableOperations.
     * Initializes the Glue client and catalog name.
     *
     * @param glueClient  The Glue client to interact with AWS Glue.
     * @param catalogName The name of the catalog.
     */
    public GlueTableOperations(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Checks whether a table exists in the Glue catalog.
     *
     * @param databaseName The name of the database where the table should exist.
     * @param tableName    The name of the table to check.
     * @return true if the table exists, false otherwise.
     */
    public boolean glueTableExists(String databaseName, String tableName) {
        try {
            glueClient.getTable(builder -> builder.databaseName(databaseName).name(tableName));
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        } catch (GlueException e) {
            throw new CatalogException("Error checking table existence: " + databaseName + "." + tableName, e);
        }
    }

    /**
     * Lists all tables in a given database.
     *
     * @param databaseName The name of the database from which to list tables.
     * @return A list of table names.
     * @throws CatalogException if there is an error fetching the table list.
     */
    public List<String> listTables(String databaseName) {
        try {
            GetTablesRequest request = GetTablesRequest.builder()
                    .databaseName(databaseName)
                    .build();
            GetTablesResponse response = glueClient.getTables(request);
            return response.tableList().stream()
                    .map(Table::name)
                    .collect(Collectors.toList());
        } catch (GlueException e) {
            throw new CatalogException("Error listing tables: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a new table in Glue.
     *
     * @param databaseName The name of the database where the table should be created.
     * @param tableInput   The input data for creating the table.
     * @throws CatalogException if there is an error creating the table.
     */
    public void createTable(String databaseName, TableInput tableInput) {
        try {
            CreateTableRequest request = CreateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(tableInput)
                    .build();
            CreateTableResponse response = glueClient.createTable(request);
            if (response == null || (response.sdkHttpResponse() != null && !response.sdkHttpResponse().isSuccessful())) {
                throw new CatalogException("Error creating table: " + databaseName + "." + tableInput.name());
            }
        } catch (AlreadyExistsException e) {
            throw new CatalogException("Table already exists: " + e.getMessage(), e);
        } catch (GlueException e) {
            throw new CatalogException("Error creating table: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves the details of a specific table from Glue.
     *
     * @param databaseName The name of the database where the table resides.
     * @param tableName    The name of the table to retrieve.
     * @return The Table object containing the table details.
     * @throws TableNotExistException if the table does not exist in the Glue catalog.
     * @throws CatalogException       if there is an error fetching the table details.
     */
    public Table getGlueTable(String databaseName, String tableName) throws TableNotExistException {
        try {
            GetTableRequest request = GetTableRequest.builder()
                    .databaseName(databaseName)
                    .name(tableName)
                    .build();
            return glueClient.getTable(request).table();
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, new ObjectPath(databaseName, tableName));
        } catch (GlueException e) {
            throw new CatalogException("Error getting table: " + e.getMessage(), e);
        }
    }

    /**
     * Converts a Flink catalog table to Glue's TableInput object.
     *
     * @param tableName         The name of the table.
     * @param glueColumns       The list of columns for the table.
     * @param catalogTable      The Flink CatalogTable containing the table schema.
     * @param storageDescriptor The Glue storage descriptor for the table.
     * @param properties        The properties of the table.
     * @return The Glue TableInput object representing the table.
     */
    public TableInput buildTableInput(
            String tableName, List<Column> glueColumns,
            CatalogTable catalogTable,
            StorageDescriptor storageDescriptor, Map<String, String> properties) {

        return TableInput.builder()
                .name(tableName)
                .storageDescriptor(storageDescriptor)
                .parameters(properties)
                .tableType(catalogTable.getTableKind().name())
                .build();
    }

    /**
     * Drops a table from Glue.
     *
     * @param databaseName The name of the database containing the table.
     * @param tableName    The name of the table to delete.
     * @throws TableNotExistException if the table does not exist in the Glue catalog.
     * @throws CatalogException       if there is an error dropping the table.
     */
    public void dropTable(String databaseName, String tableName) throws TableNotExistException {
        try {
            DeleteTableRequest request = DeleteTableRequest.builder()
                    .databaseName(databaseName)
                    .name(tableName)
                    .build();
            glueClient.deleteTable(request);
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, new ObjectPath(databaseName, tableName));
        } catch (GlueException e) {
            throw new CatalogException("Error dropping table: " + e.getMessage(), e);
        }
    }
}
