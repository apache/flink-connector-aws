package com.amazonaws.services.msf.operations;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.util.*;
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
    public boolean tableExists(String databaseName, String tableName) {
        try {
            glueClient.getTable(builder -> builder.databaseName(databaseName).name(tableName));
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        } catch (Exception e) {
            throw new CatalogException("Error checking if table exists: " + e.getMessage(), e);
        }
    }

    /**
     * Lists all tables in a given database.
     *
     * @param databaseName The name of the database from which to list tables.
     * @return A list of table names.
     * @throws CatalogException if there is an error fetching the table list.
     */
    public List<String> listTables(String databaseName) throws CatalogException {
        try {
            return glueClient.getTables(builder -> builder.databaseName(databaseName))
                    .tableList()
                    .stream()
                    .map(Table::name)
                    .collect(Collectors.toList());
        } catch (Exception e) {
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
    public void createTable(String databaseName, TableInput tableInput) throws CatalogException {
        try {
            glueClient.createTable(builder -> builder.databaseName(databaseName).tableInput(tableInput));
        } catch (Exception e) {
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
    public Table getGlueTable(String databaseName, String tableName) throws TableNotExistException, CatalogException {
        GetTableRequest tableRequest = GetTableRequest.builder()
                .databaseName(databaseName)
                .name(tableName)
                .build();

        try {
            GetTableResponse response = glueClient.getTable(tableRequest);
            return response.table();
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, new ObjectPath(databaseName, tableName));
        } catch (Exception e) {
            throw new CatalogException("Error fetching table: " + e.getMessage(), e);
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
            String tableName, List<software.amazon.awssdk.services.glue.model.Column> glueColumns,
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
     * @throws CatalogException if there is an error dropping the table.
     */
    public void dropTable(String databaseName, String tableName) throws CatalogException {
        try {
            // First, check if the table exists
            if (!tableExists(databaseName, tableName)) {
                throw new CatalogException("Table does not exist: " + tableName);
            }

            // Proceed to delete the table if it exists
            glueClient.deleteTable(builder -> builder.databaseName(databaseName).name(tableName));
        } catch (Exception e) {
            throw new CatalogException("Error dropping table: " + e.getMessage(), e);
        }
    }
}
