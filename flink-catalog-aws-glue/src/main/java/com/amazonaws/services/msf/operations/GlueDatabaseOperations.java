package com.amazonaws.services.msf.operations;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

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
            return glueClient.getDatabases(GetDatabasesRequest.builder().build())
                    .databaseList()
                    .stream()
                    .map(Database::name)
                    .collect(Collectors.toList());
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
        } catch (Exception e) {
            throw new CatalogException("Error fetching database: " + databaseName, e);
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
        } catch (Exception e) {
            throw new CatalogException("Error checking database existence: " + databaseName, e);
        }
    }

    /**
     * Creates a new database in Glue.
     *
     * @param databaseName The name of the database to create.
     * @param catalogDatabase The CatalogDatabase containing properties and description.
     */
    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase) {
        glueClient.createDatabase(builder -> builder.databaseInput(db ->
                db.name(databaseName)
                        .description(String.valueOf(catalogDatabase.getDescription()))
                        .parameters(catalogDatabase.getProperties())));
    }

    /**
     * Deletes the specified database from Glue.
     *
     * @param databaseName The name of the database to delete.
     * @throws DatabaseNotExistException If the database does not exist in the Glue catalog.
     */
    public void dropGlueDatabase(String databaseName) throws DatabaseNotExistException {
        DeleteDatabaseRequest deleteDatabaseRequest = DeleteDatabaseRequest.builder()
                .name(databaseName)
                .build();

        try {
            // Attempt to delete the database
            DeleteDatabaseResponse response = glueClient.deleteDatabase(deleteDatabaseRequest);
            LOG.info("Successfully dropped database: {}", databaseName);
        } catch (EntityNotFoundException e) {
            // If the database doesn't exist, throw an appropriate exception
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
    }
}
