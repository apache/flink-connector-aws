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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities for Glue catalog Database related operations. */
@Internal
public class GlueDatabaseOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlueDatabaseOperator.class);

    public GlueDatabaseOperator(String catalogName, GlueClient glueClient, String glueCatalogId) {
        super(catalogName, glueClient, glueCatalogId);
    }

    /**
     * List all databases present in glue data catalog service.
     *
     * @return fully qualified list of database name.
     */
    public List<String> listGlueDatabases() throws CatalogException {
        try {
            GetDatabasesRequest.Builder databasesRequestBuilder =
                    GetDatabasesRequest.builder().catalogId(getGlueCatalogId());
            GetDatabasesResponse response =
                    glueClient.getDatabases(databasesRequestBuilder.build());
            GlueUtils.validateGlueResponse(response);
            List<String> databaseList =
                    response.databaseList().stream()
                            .map(Database::name)
                            .collect(Collectors.toList());
            String dbResultNextToken = response.nextToken();
            while (Optional.ofNullable(dbResultNextToken).isPresent()) {
                databasesRequestBuilder.nextToken(dbResultNextToken);
                response = glueClient.getDatabases(databasesRequestBuilder.build());
                GlueUtils.validateGlueResponse(response);
                databaseList.addAll(
                        response.databaseList().stream()
                                .map(Database::name)
                                .collect(Collectors.toList()));
                dbResultNextToken = response.nextToken();
            }
            return databaseList;
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Create database in glue data catalog service.
     *
     * @param databaseName fully qualified name of database.
     * @param database Instance of {@link CatalogDatabase}.
     * @throws CatalogException on unexpected error happens.
     * @throws DatabaseAlreadyExistException when database exists already in glue data catalog.
     */
    public void createGlueDatabase(String databaseName, CatalogDatabase database)
            throws CatalogException, DatabaseAlreadyExistException {
        GlueUtils.validate(databaseName);
        Map<String, String> properties = new HashMap<>(database.getProperties());
        DatabaseInput databaseInput =
                DatabaseInput.builder()
                        .name(databaseName)
                        .description(database.getComment())
                        .parameters(properties)
                        .build();
        CreateDatabaseRequest.Builder requestBuilder =
                CreateDatabaseRequest.builder()
                        .databaseInput(databaseInput)
                        .catalogId(getGlueCatalogId());
        try {
            CreateDatabaseResponse response = glueClient.createDatabase(requestBuilder.build());
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            GlueUtils.validateGlueResponse(response);
        } catch (EntityNotFoundException e) {
            throw new CatalogException(catalogName, e);
        } catch (AlreadyExistsException e) {
            throw new DatabaseAlreadyExistException(catalogName, databaseName, e);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Delete a database from Glue data catalog service only when database is empty.
     *
     * @param databaseName fully qualified name of database.
     * @throws CatalogException on unexpected error happens.
     * @throws DatabaseNotExistException when database doesn't exists in glue catalog.
     */
    public void dropGlueDatabase(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        GlueUtils.validate(databaseName);
        DeleteDatabaseRequest deleteDatabaseRequest =
                DeleteDatabaseRequest.builder()
                        .name(databaseName)
                        .catalogId(getGlueCatalogId())
                        .build();
        try {
            DeleteDatabaseResponse response = glueClient.deleteDatabase(deleteDatabaseRequest);
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            GlueUtils.validateGlueResponse(response);
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        } catch (GlueException e) {
            throw new CatalogException(catalogName, e);
        }
    }

    /**
     * Delete list of table in database from glue data catalog service.
     *
     * @param databaseName fully qualified name of database.
     * @param tables List of table to remove from database.
     * @throws CatalogException on unexpected Exception thrown.
     */
    public void deleteTablesFromDatabase(String databaseName, Collection<String> tables)
            throws CatalogException {
        GlueUtils.validate(databaseName);
        BatchDeleteTableRequest batchTableRequest =
                BatchDeleteTableRequest.builder()
                        .databaseName(databaseName)
                        .catalogId(getGlueCatalogId())
                        .tablesToDelete(tables)
                        .build();
        try {
            BatchDeleteTableResponse response = glueClient.batchDeleteTable(batchTableRequest);
            if (response.hasErrors()) {
                String errorMsg =
                        String.format(
                                "Glue Table errors:- %s",
                                response.errors().stream()
                                        .map(
                                                e ->
                                                        "Table: "
                                                                + e.tableName()
                                                                + "\tErrorDetail: "
                                                                + e.errorDetail().errorMessage())
                                        .collect(Collectors.joining("\n")));
                LOG.error(errorMsg);
                throw new CatalogException(errorMsg);
            }
            GlueUtils.validateGlueResponse(response);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Delete list of user defined function associated with Database from glue data catalog service.
     *
     * @param databaseName fully qualified name of database.
     * @param functions List of functions to remove from database.
     * @throws CatalogException on unexpected Exception thrown.
     */
    public void deleteFunctionsFromDatabase(String databaseName, Collection<String> functions)
            throws CatalogException {
        GlueUtils.validate(databaseName);
        DeleteUserDefinedFunctionRequest.Builder requestBuilder =
                DeleteUserDefinedFunctionRequest.builder()
                        .databaseName(databaseName)
                        .catalogId(getGlueCatalogId());
        DeleteUserDefinedFunctionResponse response;
        for (String functionName : functions) {
            requestBuilder.functionName(functionName);
            try {
                response = glueClient.deleteUserDefinedFunction(requestBuilder.build());
            } catch (GlueException e) {
                LOG.error(
                        "Error deleting function {} in database: {}\n{}",
                        functionName,
                        databaseName,
                        e);
                throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            GlueUtils.validateGlueResponse(response);
        }
    }

    /**
     * Get {@link CatalogDatabase} instance using the information from glue data-catalog service.
     *
     * @param databaseName fully qualified name of database.
     * @return Instance of {@link CatalogDatabase } .
     * @throws DatabaseNotExistException when database doesn't exists in Glue data catalog Service.
     * @throws CatalogException when any unknown error occurs in Execution.
     */
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        GlueUtils.validate(databaseName);
        GetDatabaseRequest getDatabaseRequest =
                GetDatabaseRequest.builder()
                        .name(databaseName)
                        .catalogId(getGlueCatalogId())
                        .build();
        try {
            GetDatabaseResponse response = glueClient.getDatabase(getDatabaseRequest);
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            GlueUtils.validateGlueResponse(response);
            return GlueUtils.getCatalogDatabase(response.database());
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Update Database in Glue Metastore.
     *
     * @param databaseName Database name.
     * @param newDatabase instance of {@link CatalogDatabase}.
     * @throws CatalogException in case of Errors.
     */
    public void updateGlueDatabase(String databaseName, CatalogDatabase newDatabase)
            throws CatalogException {
        GlueUtils.validate(databaseName);
        Map<String, String> newProperties = new HashMap<>(newDatabase.getProperties());
        DatabaseInput databaseInput =
                DatabaseInput.builder()
                        .parameters(newProperties)
                        .description(newDatabase.getComment())
                        .name(databaseName)
                        .build();

        UpdateDatabaseRequest updateRequest =
                UpdateDatabaseRequest.builder()
                        .databaseInput(databaseInput)
                        .name(databaseName)
                        .catalogId(getGlueCatalogId())
                        .build();
        UpdateDatabaseResponse response = glueClient.updateDatabase(updateRequest);
        if (LOG.isDebugEnabled()) {
            LOG.debug(GlueUtils.getDebugLog(response));
        }
        GlueUtils.validateGlueResponse(response);
        LOG.info("Updated Database: {}", databaseName);
    }
}
