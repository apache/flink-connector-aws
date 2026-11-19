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
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.glue.util.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueTableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Handles all table-related operations for the Glue catalog: existence checks, listing, creating,
 * getting, updating and dropping tables in AWS Glue.
 *
 * <p>AWS Glue stores table names in lowercase. This catalog therefore stores every table under
 * {@code lowercase(name)} and keeps the declared name in the {@link
 * GlueCatalogConstants#ORIGINAL_TABLE_NAME} parameter. Because Glue guarantees storage names are
 * unique, a single {@code GetTable(lowercase(name))} call is sufficient to resolve any Flink table
 * name; no scan is ever required.
 */
@Internal
public class GlueTableOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperator.class);

    /**
     * Pattern for validating table names. AWS Glue supports alphanumeric characters and
     * underscores. We preserve original case in metadata while storing lowercase in Glue.
     */
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    /**
     * Constructor for GlueTableOperator.
     *
     * @param glueClient The Glue client to interact with AWS Glue.
     * @param catalogName The name of the catalog.
     */
    public GlueTableOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Validates that a table name contains only allowed characters.
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
                    "Table name can only contain letters, numbers, and underscores. "
                            + "Original case is preserved in metadata while AWS Glue stores lowercase internally.");
        }
    }

    /**
     * Converts a Flink table name to the name used for storage in Glue (lowercase).
     *
     * @param tableName The table name as specified by the user
     * @return The Glue storage name
     */
    public static String toGlueTableName(String tableName) {
        return tableName.toLowerCase();
    }

    /**
     * Returns the declared (case-preserved) name of a Glue table, falling back to the stored name
     * for tables created by other engines.
     *
     * @param table The Glue table
     * @return The original table name with case preserved
     */
    public String getOriginalTableName(Table table) {
        if (table.parameters() != null
                && table.parameters().containsKey(GlueCatalogConstants.ORIGINAL_TABLE_NAME)) {
            return table.parameters().get(GlueCatalogConstants.ORIGINAL_TABLE_NAME);
        }
        return table.name();
    }

    /**
     * Resolves a Flink table name to its Glue table with a single {@code GetTable} call.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @param tableName The table name as specified by the user (any case).
     * @return The Glue table, or {@code null} if no such table exists.
     * @throws CatalogException if Glue returns an error other than "not found".
     */
    public Table getGlueTableOrNull(String glueDatabaseName, String tableName) {
        String glueTableName = toGlueTableName(tableName);
        try {
            GetTableRequest request =
                    GetTableRequest.builder()
                            .databaseName(glueDatabaseName)
                            .name(glueTableName)
                            .build();
            return glueClient.getTable(request).table();
        } catch (EntityNotFoundException e) {
            LOG.debug("Table {}.{} not found in Glue", glueDatabaseName, glueTableName);
            return null;
        } catch (InvalidInputException e) {
            throw new CatalogException(
                    String.format(
                            "Invalid table reference %s.%s: %s",
                            glueDatabaseName, glueTableName, e.getMessage()),
                    e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error getting table %s.%s from Glue: %s",
                            glueDatabaseName, glueTableName, e.getMessage()),
                    e);
        }
    }

    /**
     * Retrieves a table from Glue.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @param tableName The table name as specified by the user (any case).
     * @return The Glue table.
     * @throws TableNotExistException if the table does not exist.
     * @throws CatalogException if there is an error fetching the table.
     */
    public Table getGlueTable(String glueDatabaseName, String tableName)
            throws TableNotExistException {
        Table table = getGlueTableOrNull(glueDatabaseName, tableName);
        if (table == null) {
            throw new TableNotExistException(
                    catalogName, new ObjectPath(glueDatabaseName, tableName));
        }
        return table;
    }

    /**
     * Checks whether a table exists.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @param tableName The table name as specified by the user (any case).
     * @return true if the table exists, false otherwise.
     */
    public boolean glueTableExists(String glueDatabaseName, String tableName) {
        return getGlueTableOrNull(glueDatabaseName, tableName) != null;
    }

    /**
     * Lists all tables (including views) in a database with their full metadata, following
     * pagination via the SDK paginator.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @return All Glue tables in the database.
     * @throws CatalogException if there is an error fetching the tables.
     */
    public List<Table> getAllGlueTables(String glueDatabaseName) {
        try {
            List<Table> tables = new ArrayList<>();
            for (GetTablesResponse page :
                    glueClient.getTablesPaginator(
                            GetTablesRequest.builder().databaseName(glueDatabaseName).build())) {
                tables.addAll(page.tableList());
            }
            return tables;
        } catch (EntityNotFoundException e) {
            throw new CatalogException("Database does not exist in Glue: " + glueDatabaseName, e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error listing tables in %s: %s", glueDatabaseName, e.getMessage()),
                    e);
        }
    }

    /**
     * Lists the declared (case-preserved) names of all tables and views in a database.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @return The table names as declared by their creators.
     * @throws CatalogException if there is an error fetching the table list.
     */
    public List<String> listTables(String glueDatabaseName) {
        List<String> names = new ArrayList<>();
        for (Table table : getAllGlueTables(glueDatabaseName)) {
            names.add(getOriginalTableName(table));
        }
        return names;
    }

    /**
     * Creates a table in Glue.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @param tableInput The table definition; its name must already be the Glue storage name.
     * @throws TableAlreadyExistException if a table with this storage name already exists.
     * @throws CatalogException if there is any other error creating the table.
     */
    public void createTable(String glueDatabaseName, TableInput tableInput)
            throws TableAlreadyExistException {
        if (tableInput.name() != null) {
            validateTableName(tableInput.name());
        }
        String originalTableName =
                tableInput.parameters() != null
                                && tableInput
                                        .parameters()
                                        .containsKey(GlueCatalogConstants.ORIGINAL_TABLE_NAME)
                        ? tableInput.parameters().get(GlueCatalogConstants.ORIGINAL_TABLE_NAME)
                        : tableInput.name();
        try {
            CreateTableRequest request =
                    CreateTableRequest.builder()
                            .databaseName(glueDatabaseName)
                            .tableInput(tableInput)
                            .build();
            // The SDK throws a typed exception for any service error, so no response
            // inspection is needed: reaching the next statement means the call succeeded.
            glueClient.createTable(request);
            LOG.info(
                    "Created table '{}' in Glue database '{}' (declared name '{}')",
                    tableInput.name(),
                    glueDatabaseName,
                    originalTableName);
        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistException(
                    catalogName, new ObjectPath(glueDatabaseName, originalTableName), e);
        } catch (EntityNotFoundException e) {
            throw new CatalogException("Database does not exist in Glue: " + glueDatabaseName, e);
        } catch (InvalidInputException e) {
            throw new CatalogException(
                    String.format(
                            "Invalid table definition for %s.%s: %s",
                            glueDatabaseName, originalTableName, e.getMessage()),
                    e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error creating table %s.%s: %s",
                            glueDatabaseName, originalTableName, e.getMessage()),
                    e);
        }
    }

    /**
     * Replaces the definition of an existing table in Glue via {@code UpdateTable}.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @param tableInput The full replacement definition; its name must be the Glue storage name.
     * @throws TableNotExistException if the table does not exist.
     * @throws CatalogException if there is any other error updating the table.
     */
    public void updateTable(String glueDatabaseName, TableInput tableInput)
            throws TableNotExistException {
        try {
            UpdateTableRequest request =
                    UpdateTableRequest.builder()
                            .databaseName(glueDatabaseName)
                            .tableInput(tableInput)
                            .build();
            glueClient.updateTable(request);
            LOG.info("Updated table '{}.{}' in Glue", glueDatabaseName, tableInput.name());
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(
                    catalogName, new ObjectPath(glueDatabaseName, tableInput.name()), e);
        } catch (InvalidInputException e) {
            throw new CatalogException(
                    String.format(
                            "Invalid table definition for %s.%s: %s",
                            glueDatabaseName, tableInput.name(), e.getMessage()),
                    e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error updating table %s.%s: %s",
                            glueDatabaseName, tableInput.name(), e.getMessage()),
                    e);
        }
    }

    /**
     * Drops a table from Glue.
     *
     * @param glueDatabaseName The Glue storage name of the database.
     * @param tableName The table name as specified by the user (any case).
     * @throws TableNotExistException if the table does not exist.
     * @throws CatalogException if there is any other error dropping the table.
     */
    public void dropTable(String glueDatabaseName, String tableName) throws TableNotExistException {
        String glueTableName = toGlueTableName(tableName);
        try {
            DeleteTableRequest request =
                    DeleteTableRequest.builder()
                            .databaseName(glueDatabaseName)
                            .name(glueTableName)
                            .build();
            glueClient.deleteTable(request);
            LOG.info("Dropped table '{}.{}' from Glue", glueDatabaseName, glueTableName);
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(
                    catalogName, new ObjectPath(glueDatabaseName, tableName), e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error dropping table %s.%s: %s",
                            glueDatabaseName, glueTableName, e.getMessage()),
                    e);
        }
    }

    /**
     * Builds the Glue {@link TableInput} for a Flink table or view.
     *
     * <p>Partition columns are persisted at the {@code TableInput} level (Glue models partition
     * keys separately from the storage descriptor columns), and the comment is persisted as the
     * Glue table description.
     *
     * @param tableName The declared table name (case preserved in metadata).
     * @param tableKind Whether this is a table or a view.
     * @param comment The table comment, may be null.
     * @param partitionColumns The Glue columns for the partition keys, in declared order (may be
     *     empty).
     * @param storageDescriptor The Glue storage descriptor holding the data (non-partition)
     *     columns.
     * @param properties The table options plus internal parameters.
     * @return The Glue TableInput object representing the table.
     */
    public TableInput buildTableInput(
            String tableName,
            CatalogBaseTable.TableKind tableKind,
            String comment,
            List<Column> partitionColumns,
            StorageDescriptor storageDescriptor,
            Map<String, String> properties) {

        validateTableName(tableName);

        Map<String, String> tableParameters = new HashMap<>();
        if (properties != null) {
            tableParameters.putAll(properties);
        }
        tableParameters.put(GlueCatalogConstants.ORIGINAL_TABLE_NAME, tableName);

        // Glue rejects column-level parameters on partition columns, so the original-name
        // column parameter cannot be used for them. Strip any parameters and preserve the
        // declared partition-key case in an order-preserving table-level parameter instead.
        List<Column> sanitizedPartitionColumns = new ArrayList<>();
        if (partitionColumns != null && !partitionColumns.isEmpty()) {
            List<String> originalPartitionKeys = new ArrayList<>();
            boolean anyMixedCase = false;
            for (Column partitionColumn : partitionColumns) {
                String originalName = GlueTableUtils.getColumnName(partitionColumn);
                originalPartitionKeys.add(originalName);
                if (!originalName.equals(partitionColumn.name())) {
                    anyMixedCase = true;
                }
                sanitizedPartitionColumns.add(
                        Column.builder()
                                .name(partitionColumn.name())
                                .type(partitionColumn.type())
                                .comment(partitionColumn.comment())
                                .build());
            }
            if (anyMixedCase) {
                tableParameters.put(
                        GlueCatalogConstants.ORIGINAL_PARTITION_KEYS,
                        String.join(",", originalPartitionKeys));
            }
        }

        TableInput.Builder builder =
                TableInput.builder()
                        .name(toGlueTableName(tableName))
                        .storageDescriptor(storageDescriptor)
                        .parameters(tableParameters)
                        .tableType(tableKind.name());

        if (!sanitizedPartitionColumns.isEmpty()) {
            builder.partitionKeys(sanitizedPartitionColumns);
        }
        if (comment != null) {
            builder.description(comment);
        }
        return builder.build();
    }
}
