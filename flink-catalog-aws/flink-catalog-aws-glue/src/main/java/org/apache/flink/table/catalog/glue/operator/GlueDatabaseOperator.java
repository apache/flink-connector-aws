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
import java.util.regex.Pattern;

/**
 * Handles all database-related operations for the Glue catalog.
 * Provides functionality for listing, retrieving, creating, and deleting databases in AWS Glue.
 */
public class GlueDatabaseOperator extends GlueOperator {

    /** Logger for logging database operations. */
    private static final Logger LOG = LoggerFactory.getLogger(GlueDatabaseOperator.class);

    /**
     * Pattern for validating database names.
     * AWS Glue supports alphanumeric characters and underscores.
     * We preserve original case in metadata while storing lowercase in Glue.
     */
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    /**
     * Constructor for GlueDatabaseOperations.
     * Initializes the Glue client and catalog name.
     *
     * @param glueClient The Glue client to interact with AWS Glue.
     * @param catalogName The name of the catalog.
     */
    public GlueDatabaseOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Validates that a database name contains only allowed characters.
     * AWS Glue supports alphanumeric characters and underscores.
     * Case is preserved in metadata while Glue stores lowercase internally.
     *
     * @param databaseName The database name to validate
     * @throws CatalogException if the database name contains invalid characters
     */
    private void validateDatabaseName(String databaseName) {
        if (databaseName == null || databaseName.isEmpty()) {
            throw new CatalogException("Database name cannot be null or empty");
        }

        if (!VALID_NAME_PATTERN.matcher(databaseName).matches()) {
            throw new CatalogException(
                    "Database name can only contain letters, numbers, and underscores. " +
                    "Original case is preserved in metadata while AWS Glue stores lowercase internally.");
        }
    }

    /**
     * Lists all the databases in the Glue catalog.
     * Returns the original database names as specified by users, not the lowercase names stored in Glue.
     *
     * @return A list of database names with original case preserved.
     * @throws CatalogException if there is an error fetching the list of databases.
     */
    public List<String> listDatabases() throws CatalogException {
        try {
            List<String> databaseNames = new ArrayList<>();
            String nextToken = null;
            while (true) {
                GetDatabasesRequest.Builder requestBuilder = GetDatabasesRequest.builder();
                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }
                GetDatabasesResponse response = glueClient.getDatabases(requestBuilder.build());

                // Extract original names from database metadata
                for (Database database : response.databaseList()) {
                    String originalName = getOriginalDatabaseName(database);
                    databaseNames.add(originalName);
                }

                nextToken = response.nextToken();
                if (nextToken == null) {
                    break;
                }
            }
            return databaseNames;
        } catch (GlueException e) {
            LOG.error("Failed to list databases in Glue", e);
            throw new CatalogException("Failed to list databases: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts the original database name from a Glue database object.
     * Falls back to the stored name if no original name is found.
     *
     * @param database The Glue database object
     * @return The original database name with case preserved
     */
    private String getOriginalDatabaseName(Database database) {
        if (database.parameters() != null &&
            database.parameters().containsKey(GlueCatalogConstants.ORIGINAL_DATABASE_NAME)) {
            return database.parameters().get(GlueCatalogConstants.ORIGINAL_DATABASE_NAME);
        }
        // Fallback to stored name for backward compatibility
        return database.name();
    }

    /**
     * Converts a user-provided database name to the name used for storage in Glue.
     * Glue requires lowercase names, so we store in lowercase but preserve original in metadata.
     *
     * @param originalDatabaseName The original database name as specified by the user
     * @return The database name to use for Glue storage (lowercase)
     */
    private String toGlueDatabaseName(String originalDatabaseName) {
        return originalDatabaseName.toLowerCase();
    }

    /**
     * Finds the Glue storage name for a given original database name.
     * This is needed because users may specify names with different casing than stored in Glue.
     *
     * @param originalDatabaseName The original database name to find
     * @return The Glue storage name if found, null if not found
     * @throws CatalogException if there's an error searching
     */
    private String findGlueDatabaseName(String originalDatabaseName) throws CatalogException {
        try {
            // First try the direct lowercase match (most common case)
            String glueName = toGlueDatabaseName(originalDatabaseName);
            if (glueDatabaseExistsByGlueName(glueName)) {
                // Verify this is actually the right database by checking stored original name
                Database database = glueClient.getDatabase(GetDatabaseRequest.builder().name(glueName).build()).database();
                if (database != null) {
                    String storedOriginalName = getOriginalDatabaseName(database);
                    if (storedOriginalName.equals(originalDatabaseName)) {
                        return glueName;
                    }
                }
            }

            // If direct match failed, search all databases (for backward compatibility or edge cases)
            String nextToken = null;
            while (true) {
                GetDatabasesRequest.Builder requestBuilder = GetDatabasesRequest.builder();
                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }
                GetDatabasesResponse response = glueClient.getDatabases(requestBuilder.build());

                for (Database database : response.databaseList()) {
                    String storedOriginalName = getOriginalDatabaseName(database);
                    if (storedOriginalName.equals(originalDatabaseName)) {
                        return database.name(); // Return the Glue storage name
                    }
                }

                nextToken = response.nextToken();
                if (nextToken == null) {
                    break;
                }
            }

            return null; // Database not found
        } catch (GlueException e) {
            throw new CatalogException("Error searching for database: " + originalDatabaseName, e);
        }
    }

    /**
     * Retrieves the specified database from the Glue catalog.
     *
     * @param originalDatabaseName The original name of the database to fetch.
     * @return The CatalogDatabase object representing the Glue database.
     * @throws DatabaseNotExistException If the database does not exist in the Glue catalog.
     * @throws CatalogException If there is any error retrieving the database.
     */
    public CatalogDatabase getDatabase(String originalDatabaseName) throws DatabaseNotExistException, CatalogException {
        try {
            String glueDatabaseName = findGlueDatabaseName(originalDatabaseName);
            if (glueDatabaseName == null) {
                throw new DatabaseNotExistException(catalogName, originalDatabaseName);
            }

            GetDatabaseResponse response = glueClient.getDatabase(
                    GetDatabaseRequest.builder()
                            .name(glueDatabaseName)
                            .build()
            );

            Database glueDatabase = response.database();
            if (glueDatabase == null) {
                throw new DatabaseNotExistException(catalogName, originalDatabaseName);
            }
            return convertGlueDatabase(glueDatabase);
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, originalDatabaseName);
        } catch (InvalidInputException e) {
            LOG.error("Invalid input while getting database: {}", originalDatabaseName, e);
            throw new CatalogException("Invalid database name: " + originalDatabaseName, e);
        } catch (OperationTimeoutException e) {
            LOG.error("Timeout while getting database: {}", originalDatabaseName, e);
            throw new CatalogException("Timeout while getting database: " + originalDatabaseName, e);
        } catch (GlueException e) {
            LOG.error("Error getting database: {}", originalDatabaseName, e);
            throw new CatalogException("Error getting database: " + originalDatabaseName, e);
        }
    }

    /**
     * Converts the Glue database model to a Flink CatalogDatabase.
     * Preserves original database name metadata in the properties.
     *
     * @param glueDatabase The Glue database model.
     * @return A CatalogDatabase representing the Glue database.
     */
    private CatalogDatabase convertGlueDatabase(Database glueDatabase) {
        Map<String, String> properties = new HashMap<>();

        // Copy all existing parameters
        if (glueDatabase.parameters() != null) {
            properties.putAll(glueDatabase.parameters());
        }

        return new CatalogDatabaseImpl(
                properties,
                glueDatabase.description()
        );
    }

    /**
     * Checks whether a database exists in Glue by original name.
     *
     * @param originalDatabaseName The original name of the database to check.
     * @return true if the database exists, false otherwise.
     */
    public boolean glueDatabaseExists(String originalDatabaseName) {
        try {
            String glueDatabaseName = findGlueDatabaseName(originalDatabaseName);
            return glueDatabaseName != null;
        } catch (CatalogException e) {
            LOG.warn("Error checking database existence for: {}", originalDatabaseName, e);
            return false;
        }
    }

    /**
     * Direct check if a database exists in Glue by Glue storage name.
     * This method directly calls Glue API without going through higher-level search functions.
     *
     * @param glueDatabaseName The Glue storage name of the database to check.
     * @return true if the database exists, false otherwise.
     */
    private boolean glueDatabaseExistsByGlueName(String glueDatabaseName) {
        try {
            glueClient.getDatabase(builder -> builder.name(glueDatabaseName));
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        } catch (GlueException e) {
            throw new CatalogException("Error checking database existence: " + glueDatabaseName, e);
        }
    }

    /**
     * Creates a new database in Glue.
     * Stores the original database name in metadata for case preservation.
     *
     * @param originalDatabaseName The original database name as specified by the user.
     * @param catalogDatabase The CatalogDatabase containing properties and description.
     * @throws DatabaseAlreadyExistException If the database already exists.
     * @throws CatalogException If there is any error creating the database.
     */
    public void createDatabase(String originalDatabaseName, CatalogDatabase catalogDatabase)
            throws DatabaseAlreadyExistException, CatalogException {
        // Validate database name
        validateDatabaseName(originalDatabaseName);

        try {
            // Store lowercase name in Glue (Glue requirement)
            String glueDatabaseName = originalDatabaseName.toLowerCase();

            // Prepare database parameters with original name preservation
            Map<String, String> parameters = new HashMap<>();
            if (catalogDatabase.getProperties() != null) {
                parameters.putAll(catalogDatabase.getProperties());
            }

            // Store original name in metadata
            parameters.put(GlueCatalogConstants.ORIGINAL_DATABASE_NAME, originalDatabaseName);

            glueClient.createDatabase(builder -> builder.databaseInput(db ->
                    db.name(glueDatabaseName)
                            .description(catalogDatabase.getDescription().orElse(null))
                            .parameters(parameters)));

            LOG.info("Created database '{}' in Glue with original name '{}' preserved",
                     glueDatabaseName, originalDatabaseName);
        } catch (AlreadyExistsException e) {
            throw new DatabaseAlreadyExistException(catalogName, originalDatabaseName);
        } catch (GlueException e) {
            throw new CatalogException("Error creating database: " + originalDatabaseName, e);
        }
    }

    /**
     * Deletes the specified database from Glue.
     *
     * @param originalDatabaseName The original name of the database to delete.
     * @throws DatabaseNotExistException If the database does not exist in the Glue catalog.
     * @throws CatalogException If there is any error deleting the database.
     */
    public void dropGlueDatabase(String originalDatabaseName) throws DatabaseNotExistException, CatalogException {
        try {
            String glueDatabaseName = findGlueDatabaseName(originalDatabaseName);
            if (glueDatabaseName == null) {
                throw new DatabaseNotExistException(catalogName, originalDatabaseName);
            }

            DeleteDatabaseRequest deleteDatabaseRequest = DeleteDatabaseRequest.builder()
                    .name(glueDatabaseName)
                    .build();

            glueClient.deleteDatabase(deleteDatabaseRequest);
            LOG.info("Successfully dropped database with original name '{}' (Glue name: '{}')",
                     originalDatabaseName, glueDatabaseName);
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, originalDatabaseName);
        } catch (GlueException e) {
            throw new CatalogException("Error dropping database: " + originalDatabaseName, e);
        }
    }
}
