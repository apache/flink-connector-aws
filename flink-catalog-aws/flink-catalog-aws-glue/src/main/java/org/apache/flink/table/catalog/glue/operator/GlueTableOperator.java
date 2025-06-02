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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.glue.util.GlueCatalogConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Handles all table-related operations for the Glue catalog.
 * Provides functionality for checking existence, listing, creating, getting, and dropping tables in AWS Glue.
 */
public class GlueTableOperator extends GlueOperator {

    /**
     * Logger for logging table operations.
     */
    private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperator.class);

    /**
     * Pattern for validating table names.
     * AWS Glue supports alphanumeric characters and underscores.
     * We preserve original case in metadata while storing lowercase in Glue.
     */
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    /**
     * Constructor for GlueTableOperations.
     * Initializes the Glue client and catalog name.
     *
     * @param glueClient  The Glue client to interact with AWS Glue.
     * @param catalogName The name of the catalog.
     */
    public GlueTableOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Validates that a table name contains only allowed characters.
     * AWS Glue supports alphanumeric characters and underscores.
     * Case is preserved in metadata while Glue stores lowercase internally.
     *
     * @param tableName The table name to validate
     * @throws CatalogException if the table name contains invalid characters
     */
    private void validateTableName(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new CatalogException("Table name cannot be null or empty");
        }

        if (!VALID_NAME_PATTERN.matcher(tableName).matches()) {
            throw new CatalogException(
                    "Table name can only contain letters, numbers, and underscores. " +
                    "Original case is preserved in metadata while AWS Glue stores lowercase internally.");
        }
    }

    /**
     * Checks whether a table exists in the Glue catalog by original names.
     *
     * @param databaseName The original name of the database where the table should exist.
     * @param tableName The original name of the table to check.
     * @return true if the table exists, false otherwise.
     */
    public boolean glueTableExists(String databaseName, String tableName) {
        try {
            // We need the Glue storage names to check existence
            // For now, assume database and table names are provided in their storage format
            // This will be updated when the main catalog is modified to handle conversions
            String glueTableName = findGlueTableName(databaseName, tableName);
            return glueTableName != null;
        } catch (CatalogException e) {
            LOG.warn("Error checking table existence for: {}.{}", databaseName, tableName, e);
            return false;
        }
    }

    /**
     * Lists all tables in a given database.
     * Returns the original table names as specified by users, not the lowercase names stored in Glue.
     *
     * @param databaseName The name of the database from which to list tables.
     * @return A list of table names with original case preserved.
     * @throws CatalogException if there is an error fetching the table list.
     */
    public List<String> listTables(String databaseName) {
        try {
            List<String> tableNames = new ArrayList<>();
            String nextToken = null;

            while (true) {
                GetTablesRequest.Builder requestBuilder = GetTablesRequest.builder()
                        .databaseName(databaseName);

                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }

                GetTablesResponse response = glueClient.getTables(requestBuilder.build());

                // Extract original names from table metadata
                for (Table table : response.tableList()) {
                    String originalName = getOriginalTableName(table);
                    tableNames.add(originalName);
                }

                nextToken = response.nextToken();

                if (nextToken == null) {
                    break;
                }
            }

            return tableNames;
        } catch (GlueException e) {
            throw new CatalogException("Error listing tables: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a new table in Glue.
     * Stores the original table name in metadata for case preservation.
     *
     * @param databaseName The Glue storage name of the database where the table should be created.
     * @param tableInput   The input data for creating the table (should include original name in parameters).
     * @throws CatalogException if there is an error creating the table.
     */
    public void createTable(String databaseName, TableInput tableInput) {
        try {
            // Validate table name from the TableInput
            if (tableInput.name() != null) {
                validateTableName(tableInput.name());
            }

            // The table name in tableInput should already be the Glue storage name (lowercase)
            // The original name should be stored in parameters by the caller

            CreateTableRequest request = CreateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(tableInput)
                    .build();
            CreateTableResponse response = glueClient.createTable(request);
            if (response == null || (response.sdkHttpResponse() != null && !response.sdkHttpResponse().isSuccessful())) {
                throw new CatalogException("Error creating table: " + databaseName + "." + tableInput.name());
            }

            // Log both original and storage names for clarity
            String originalTableName = tableInput.parameters() != null ?
                tableInput.parameters().get(GlueCatalogConstants.ORIGINAL_TABLE_NAME) :
                tableInput.name();
            LOG.info("Created table '{}' in Glue with original name '{}' preserved",
                     tableInput.name(), originalTableName);
        } catch (AlreadyExistsException e) {
            throw new CatalogException("Table already exists: " + e.getMessage(), e);
        } catch (GlueException e) {
            throw new CatalogException("Error creating table: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves the details of a specific table from Glue by original names.
     *
     * @param originalDatabaseName The original name of the database where the table resides.
     * @param originalTableName    The original name of the table to retrieve.
     * @return The Table object containing the table details.
     * @throws TableNotExistException if the table does not exist in the Glue catalog.
     * @throws CatalogException       if there is an error fetching the table details.
     */
    public Table getGlueTableByOriginalName(String originalDatabaseName, String originalTableName) throws TableNotExistException {
        try {
            // First find the Glue storage names
            String glueDatabaseName = originalDatabaseName.toLowerCase(); // For now, assume database name conversion is handled upstream
            String glueTableName = findGlueTableName(glueDatabaseName, originalTableName);

            if (glueTableName == null) {
                throw new TableNotExistException(catalogName, new ObjectPath(originalDatabaseName, originalTableName));
            }

            GetTableRequest request = GetTableRequest.builder()
                    .databaseName(glueDatabaseName)
                    .name(glueTableName)
                    .build();
            Table table = glueClient.getTable(request).table();
            if (table == null) {
                throw new TableNotExistException(catalogName, new ObjectPath(originalDatabaseName, originalTableName));
            }
            return table;
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, new ObjectPath(originalDatabaseName, originalTableName));
        } catch (GlueException e) {
            throw new CatalogException("Error getting table: " + e.getMessage(), e);
        }
    }

    /**
     * Drops a table from Glue by original names.
     *
     * @param originalDatabaseName The original name of the database where the table resides.
     * @param originalTableName    The original name of the table to drop.
     * @throws TableNotExistException if the table does not exist in the Glue catalog.
     * @throws CatalogException       if there is an error dropping the table.
     */
    public void dropTableByOriginalName(String originalDatabaseName, String originalTableName) throws TableNotExistException {
        try {
            // First find the Glue storage names
            String glueDatabaseName = originalDatabaseName.toLowerCase(); // For now, assume database name conversion is handled upstream
            String glueTableName = findGlueTableName(glueDatabaseName, originalTableName);

            if (glueTableName == null) {
                throw new TableNotExistException(catalogName, new ObjectPath(originalDatabaseName, originalTableName));
            }

            DeleteTableRequest request = DeleteTableRequest.builder()
                    .databaseName(glueDatabaseName)
                    .name(glueTableName)
                    .build();
            DeleteTableResponse response = glueClient.deleteTable(request);
            if (response == null || (response.sdkHttpResponse() != null && !response.sdkHttpResponse().isSuccessful())) {
                throw new CatalogException("Error dropping table: " + originalDatabaseName + "." + originalTableName);
            }

            LOG.info("Successfully dropped table with original name '{}.{}' (Glue name: '{}.{}')",
                     originalDatabaseName, originalTableName, glueDatabaseName, glueTableName);
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, new ObjectPath(originalDatabaseName, originalTableName));
        } catch (GlueException e) {
            throw new CatalogException("Error dropping table: " + e.getMessage(), e);
        }
    }

    /**
     * Converts a Flink catalog table to Glue's TableInput object.
     * Stores the original table name in metadata for case preservation.
     *
     * @param originalTableName The original table name as specified by the user.
     * @param glueColumns       The list of columns for the table.
     * @param catalogTable      The Flink CatalogTable containing the table schema.
     * @param storageDescriptor The Glue storage descriptor for the table.
     * @param properties        The properties of the table.
     * @return The Glue TableInput object representing the table.
     */
    public TableInput buildTableInput(
            String originalTableName, List<Column> glueColumns,
            CatalogTable catalogTable,
            StorageDescriptor storageDescriptor, Map<String, String> properties) {

        // Validate table name
        validateTableName(originalTableName);

        // Store lowercase name in Glue (Glue requirement)
        String glueTableName = toGlueTableName(originalTableName);

        // Prepare table parameters with original name preservation
        Map<String, String> enhancedProperties = new HashMap<>();
        if (properties != null) {
            enhancedProperties.putAll(properties);
        }

        // Store original name in metadata
        enhancedProperties.put(GlueCatalogConstants.ORIGINAL_TABLE_NAME, originalTableName);

        return TableInput.builder()
                .name(glueTableName)
                .storageDescriptor(storageDescriptor)
                .parameters(enhancedProperties)
                .tableType(catalogTable.getTableKind().name())
                .build();
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
            Table table = glueClient.getTable(request).table();
            if (table == null) {
                throw new TableNotExistException(catalogName, new ObjectPath(databaseName, tableName));
            }
            return table;
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, new ObjectPath(databaseName, tableName));
        } catch (GlueException e) {
            throw new CatalogException("Error getting table: " + e.getMessage(), e);
        }
    }

    /**
     * Drops a table from Glue.
     *
     * @param databaseName The name of the database where the table resides.
     * @param tableName    The name of the table to drop.
     * @throws TableNotExistException if the table does not exist in the Glue catalog.
     * @throws CatalogException       if there is an error dropping the table.
     */
    public void dropTable(String databaseName, String tableName) throws TableNotExistException {
        try {
            DeleteTableRequest request = DeleteTableRequest.builder()
                    .databaseName(databaseName)
                    .name(tableName)
                    .build();
            DeleteTableResponse response = glueClient.deleteTable(request);
            if (response == null || (response.sdkHttpResponse() != null && !response.sdkHttpResponse().isSuccessful())) {
                throw new CatalogException("Error dropping table: " + databaseName + "." + tableName);
            }
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, new ObjectPath(databaseName, tableName));
        } catch (GlueException e) {
            throw new CatalogException("Error dropping table: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts the original table name from a Glue table object.
     * Falls back to the stored name if no original name is found.
     *
     * @param table The Glue table object
     * @return The original table name with case preserved
     */
    private String getOriginalTableName(Table table) {
        if (table.parameters() != null &&
            table.parameters().containsKey(GlueCatalogConstants.ORIGINAL_TABLE_NAME)) {
            return table.parameters().get(GlueCatalogConstants.ORIGINAL_TABLE_NAME);
        }
        // Fallback to stored name for backward compatibility
        return table.name();
    }

    /**
     * Converts a user-provided table name to the name used for storage in Glue.
     * Glue requires lowercase names, so we store in lowercase but preserve original in metadata.
     *
     * @param originalTableName The original table name as specified by the user
     * @return The table name to use for Glue storage (lowercase)
     */
    private String toGlueTableName(String originalTableName) {
        return originalTableName.toLowerCase();
    }

    /**
     * Finds the Glue storage name for a given original table name in a database.
     * This is needed because users may specify names with different casing than stored in Glue.
     *
     * @param databaseName The database name (Glue storage name)
     * @param originalTableName The original table name to find
     * @return The Glue storage name if found, null if not found
     * @throws CatalogException if there's an error searching
     */
    private String findGlueTableName(String databaseName, String originalTableName) throws CatalogException {
        try {
            // First try the direct lowercase match (most common case)
            String glueTableName = toGlueTableName(originalTableName);
            if (glueTableExistsByGlueName(databaseName, glueTableName)) {
                // Verify this is actually the right table by checking stored original name
                Table table = glueClient.getTable(GetTableRequest.builder()
                        .databaseName(databaseName)
                        .name(glueTableName)
                        .build()).table();
                if (table != null) {
                    String storedOriginalName = getOriginalTableName(table);
                    if (storedOriginalName.equals(originalTableName)) {
                        return glueTableName;
                    }
                }
            }

            // If direct match failed, search all tables in the database
            String nextToken = null;

            while (true) {
                GetTablesRequest.Builder requestBuilder = GetTablesRequest.builder()
                        .databaseName(databaseName);

                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }

                GetTablesResponse response = glueClient.getTables(requestBuilder.build());

                for (Table table : response.tableList()) {
                    String storedOriginalName = getOriginalTableName(table);
                    if (storedOriginalName.equals(originalTableName)) {
                        return table.name(); // Return the Glue storage name
                    }
                }

                nextToken = response.nextToken();
                if (nextToken == null) {
                    break;
                }
            }

            return null; // Table not found
        } catch (GlueException e) {
            throw new CatalogException("Error searching for table: " + originalTableName + " in database: " + databaseName, e);
        }
    }

    /**
     * Direct check if a table exists in Glue by Glue storage names.
     * This method directly calls Glue API without going through higher-level search functions.
     *
     * @param databaseName The Glue storage name of the database
     * @param glueTableName The Glue storage name of the table to check
     * @return true if the table exists, false otherwise
     */
    private boolean glueTableExistsByGlueName(String databaseName, String glueTableName) {
        try {
            glueClient.getTable(builder -> builder.databaseName(databaseName).name(glueTableName));
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        } catch (GlueException e) {
            throw new CatalogException("Error checking table existence: " + databaseName + "." + glueTableName, e);
        }
    }
}
