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
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.glue.util.GlueCatalogConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Handles all database-related operations for the Glue catalog: listing, resolving, retrieving,
 * creating and deleting databases in AWS Glue.
 *
 * <p>AWS Glue stores database names in lowercase. This catalog therefore stores every database
 * under {@code lowercase(name)} and keeps the declared name in the {@link
 * GlueCatalogConstants#ORIGINAL_DATABASE_NAME} parameter. Because Glue guarantees storage names are
 * unique, a single {@code GetDatabase(lowercase(name))} call is sufficient to resolve any Flink
 * database name; no scan is ever required.
 */
@Internal
public class GlueDatabaseOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlueDatabaseOperator.class);

    /**
     * Pattern for validating database names. AWS Glue supports alphanumeric characters and
     * underscores. We preserve original case in metadata while storing lowercase in Glue.
     */
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    /**
     * Constructor for GlueDatabaseOperator.
     *
     * @param glueClient The Glue client to interact with AWS Glue.
     * @param catalogName The name of the catalog.
     */
    public GlueDatabaseOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    private void validateDatabaseName(String databaseName) {
        if (databaseName == null || databaseName.isEmpty()) {
            throw new CatalogException("Database name cannot be null or empty");
        }
        if (!VALID_NAME_PATTERN.matcher(databaseName).matches()) {
            throw new CatalogException(
                    "Database name can only contain letters, numbers, and underscores. "
                            + "Original case is preserved in metadata while AWS Glue stores lowercase internally.");
        }
    }

    /**
     * Converts a Flink database name to the name used for storage in Glue (lowercase).
     *
     * @param databaseName The database name as specified by the user
     * @return The Glue storage name
     */
    public static String toGlueDatabaseName(String databaseName) {
        return databaseName.toLowerCase();
    }

    /**
     * Returns the declared (case-preserved) name of a Glue database, falling back to the stored
     * name for databases created by other engines.
     *
     * @param database The Glue database
     * @return The original database name with case preserved
     */
    public String getOriginalDatabaseName(Database database) {
        if (database.parameters() != null
                && database.parameters().containsKey(GlueCatalogConstants.ORIGINAL_DATABASE_NAME)) {
            return database.parameters().get(GlueCatalogConstants.ORIGINAL_DATABASE_NAME);
        }
        return database.name();
    }

    /**
     * Lists the declared (case-preserved) names of all databases, following pagination via the SDK
     * paginator.
     *
     * @return A list of database names with original case preserved.
     * @throws CatalogException if there is an error fetching the list of databases.
     */
    public List<String> listDatabases() throws CatalogException {
        try {
            List<String> databaseNames = new ArrayList<>();
            for (GetDatabasesResponse page :
                    glueClient.getDatabasesPaginator(GetDatabasesRequest.builder().build())) {
                for (Database database : page.databaseList()) {
                    databaseNames.add(getOriginalDatabaseName(database));
                }
            }
            return databaseNames;
        } catch (GlueException e) {
            throw new CatalogException("Error listing databases: " + e.getMessage(), e);
        }
    }

    /**
     * Resolves a Flink database name to its Glue database with a single {@code GetDatabase} call.
     *
     * @param databaseName The database name as specified by the user (any case).
     * @return The Glue database, or {@code null} if no such database exists.
     * @throws CatalogException if Glue returns an error other than "not found".
     */
    public Database getGlueDatabaseOrNull(String databaseName) throws CatalogException {
        String glueName = toGlueDatabaseName(databaseName);
        try {
            Database database =
                    glueClient
                            .getDatabase(GetDatabaseRequest.builder().name(glueName).build())
                            .database();
            LOG.debug("Resolved database '{}' to Glue storage name '{}'", databaseName, glueName);
            return database;
        } catch (EntityNotFoundException e) {
            LOG.debug("Database '{}' not found in Glue", glueName);
            return null;
        } catch (InvalidInputException e) {
            throw new CatalogException(
                    "Invalid database name '" + databaseName + "': " + e.getMessage(), e);
        } catch (OperationTimeoutException e) {
            throw new CatalogException("Timed out looking up database '" + databaseName + "'", e);
        } catch (GlueException e) {
            throw new CatalogException(
                    "Error looking up database '" + databaseName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Resolves a Flink database name to its Glue storage name.
     *
     * @param databaseName The database name as specified by the user (any case).
     * @return The Glue storage name if the database exists, null otherwise.
     * @throws CatalogException if Glue returns an error other than "not found".
     */
    public String findGlueDatabaseName(String databaseName) throws CatalogException {
        Database database = getGlueDatabaseOrNull(databaseName);
        return database == null ? null : database.name();
    }

    /**
     * Checks whether a database exists.
     *
     * @param databaseName The database name as specified by the user (any case).
     * @return true if the database exists, false otherwise.
     * @throws CatalogException if Glue returns an error other than "not found".
     */
    public boolean glueDatabaseExists(String databaseName) {
        return getGlueDatabaseOrNull(databaseName) != null;
    }

    /**
     * Retrieves a database from Glue as a Flink {@link CatalogDatabase}.
     *
     * @param databaseName The database name as specified by the user (any case).
     * @return The CatalogDatabase object representing the Glue database.
     * @throws DatabaseNotExistException If the database does not exist.
     * @throws CatalogException If there is any error retrieving the database.
     */
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Database glueDatabase = getGlueDatabaseOrNull(databaseName);
        if (glueDatabase == null) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return convertGlueDatabase(glueDatabase);
    }

    /**
     * Converts a Glue database to a Flink CatalogDatabase, hiding the internal name parameter.
     *
     * @param glueDatabase The Glue database model.
     * @return A CatalogDatabase representing the Glue database.
     */
    public CatalogDatabase convertGlueDatabase(Database glueDatabase) {
        Map<String, String> properties = new HashMap<>();
        if (glueDatabase.parameters() != null) {
            properties.putAll(glueDatabase.parameters());
            properties.remove(GlueCatalogConstants.ORIGINAL_DATABASE_NAME);
        }
        return new CatalogDatabaseImpl(properties, glueDatabase.description());
    }

    /**
     * Creates a new database in Glue, storing the declared name in metadata for case preservation.
     *
     * @param databaseName The database name as specified by the user.
     * @param catalogDatabase The CatalogDatabase containing properties and description.
     * @throws DatabaseAlreadyExistException If a database with this storage name already exists.
     * @throws CatalogException If there is any other error creating the database.
     */
    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase)
            throws DatabaseAlreadyExistException, CatalogException {
        validateDatabaseName(databaseName);
        String glueDatabaseName = toGlueDatabaseName(databaseName);

        Map<String, String> parameters = new HashMap<>();
        if (catalogDatabase.getProperties() != null) {
            parameters.putAll(catalogDatabase.getProperties());
        }
        parameters.put(GlueCatalogConstants.ORIGINAL_DATABASE_NAME, databaseName);

        try {
            glueClient.createDatabase(
                    builder ->
                            builder.databaseInput(
                                    db ->
                                            db.name(glueDatabaseName)
                                                    .description(
                                                            catalogDatabase
                                                                    .getDescription()
                                                                    .orElse(null))
                                                    .parameters(parameters)));
            LOG.info(
                    "Created database '{}' in Glue (declared name '{}')",
                    glueDatabaseName,
                    databaseName);
        } catch (AlreadyExistsException e) {
            throw new DatabaseAlreadyExistException(catalogName, databaseName, e);
        } catch (InvalidInputException e) {
            throw new CatalogException(
                    "Invalid database definition for '" + databaseName + "': " + e.getMessage(), e);
        } catch (GlueException e) {
            throw new CatalogException(
                    "Error creating database '" + databaseName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Deletes a database from Glue.
     *
     * @param databaseName The database name as specified by the user (any case).
     * @throws DatabaseNotExistException If the database does not exist.
     * @throws CatalogException If there is any other error deleting the database.
     */
    public void dropGlueDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        String glueDatabaseName = toGlueDatabaseName(databaseName);
        try {
            glueClient.deleteDatabase(
                    DeleteDatabaseRequest.builder().name(glueDatabaseName).build());
            LOG.info("Dropped database '{}' from Glue", glueDatabaseName);
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, databaseName, e);
        } catch (GlueException e) {
            throw new CatalogException(
                    "Error dropping database '" + databaseName + "': " + e.getMessage(), e);
        }
    }
}
