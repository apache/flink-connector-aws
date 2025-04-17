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

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles all database-related operations for the Glue catalog.
 * Provides functionality for listing, retrieving, creating, and deleting databases in AWS Glue.
 */
public class GlueDatabaseOperations extends AbstractGlueOperations {

    /** Logger for logging database operations. */
    private static final Logger LOG = LoggerFactory.getLogger(GlueDatabaseOperations.class);

    /**
     * Constructor for GlueDatabaseOperations.
     * Initializes the Glue client and catalog name.
     *
     * @param glueClient The Glue client to interact with AWS Glue.
     * @param catalogName The name of the catalog.
     */
    public GlueDatabaseOperations(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Lists all the databases in the Glue catalog.
     *
     * @return A list of database names.
     * @throws CatalogException if there is an error fetching the list of databases.
     */
    public List<String> listDatabases() throws CatalogException {
        try {
            LOG.debug("Listing databases from Glue catalog");
            List<String> databaseNames = new ArrayList<>();
            String nextToken = null;
            do {
                GetDatabasesRequest.Builder requestBuilder = GetDatabasesRequest.builder();
                // Add the next token if we have one
                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }
                GetDatabasesResponse response = glueClient.getDatabases(requestBuilder.build());
                // Add all found databases to our list
                databaseNames.addAll(response.databaseList().stream()
                        .map(Database::name)
                        .collect(Collectors.toList()));
                // Update the next token
                nextToken = response.nextToken();
                // Continue until no more pages (nextToken is null)
            } while (nextToken != null);
            LOG.debug("Found {} databases in Glue catalog", databaseNames.size());
            return databaseNames;
        } catch (GlueException e) {
            LOG.error("Failed to list databases in Glue", e);
            throw new CatalogException("Failed to list databases: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves the specified database from the Glue catalog.
     *
     * @param databaseName The name of the database to fetch.
     * @return The CatalogDatabase object representing the Glue database.
     * @throws DatabaseNotExistException If the database does not exist in the Glue catalog.
     * @throws CatalogException If there is any error retrieving the database.
     */
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        try {
            GetDatabaseResponse response = glueClient.getDatabase(
                    GetDatabaseRequest.builder()
                            .name(databaseName)
                            .build()
            );

            Database glueDatabase = response.database();
            return convertGlueDatabase(glueDatabase);
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        } catch (InvalidInputException e) {
            LOG.error("Invalid input while getting database: {}", databaseName, e);
            throw new CatalogException("Invalid database name: " + databaseName, e);
        } catch (OperationTimeoutException e) {
            LOG.error("Timeout while getting database: {}", databaseName, e);
            throw new CatalogException("Timeout while getting database: " + databaseName, e);
        } catch (GlueException e) {
            LOG.error("Error getting database: {}", databaseName, e);
            throw new CatalogException("Error getting database: " + databaseName, e);
        }
    }

    /**
     * Converts the Glue database model to a Flink CatalogDatabase.
     *
     * @param glueDatabase The Glue database model.
     * @return A CatalogDatabase representing the Glue database.
     */
    private CatalogDatabase convertGlueDatabase(Database glueDatabase) {
        Map<String, String> properties = new HashMap<>(glueDatabase.parameters());
        return new CatalogDatabaseImpl(
                properties,
                glueDatabase.description()
        );
    }

    /**
     * Checks whether a database exists in Glue.
     *
     * @param databaseName The name of the database to check.
     * @return true if the database exists, false otherwise.
     */
    public boolean glueDatabaseExists(String databaseName) {
        try {
            glueClient.getDatabase(builder -> builder.name(databaseName));
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        } catch (GlueException e) {
            throw new CatalogException("Error checking database existence: " + databaseName, e);
        }
    }

    /**
     * Creates a new database in Glue.
     *
     * @param databaseName The name of the database to create.
     * @param catalogDatabase The CatalogDatabase containing properties and description.
     * @throws DatabaseAlreadyExistException If the database already exists.
     * @throws CatalogException If there is any error creating the database.
     */
    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            glueClient.createDatabase(builder -> builder.databaseInput(db ->
                    db.name(databaseName)
                            .description(catalogDatabase.getDescription().orElse(null))
                            .parameters(catalogDatabase.getProperties())));
        } catch (AlreadyExistsException e) {
            throw new DatabaseAlreadyExistException(catalogName, databaseName);
        } catch (GlueException e) {
            throw new CatalogException("Error creating database: " + databaseName, e);
        }
    }

    /**
     * Deletes the specified database from Glue.
     *
     * @param databaseName The name of the database to delete.
     * @throws DatabaseNotExistException If the database does not exist in the Glue catalog.
     * @throws CatalogException If there is any error deleting the database.
     */
    public void dropGlueDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        try {
            DeleteDatabaseRequest deleteDatabaseRequest = DeleteDatabaseRequest.builder()
                    .name(databaseName)
                    .build();

            glueClient.deleteDatabase(deleteDatabaseRequest);
            LOG.info("Successfully dropped database: {}", databaseName);
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        } catch (GlueException e) {
            throw new CatalogException("Error dropping database: " + databaseName, e);
        }
    }
}
