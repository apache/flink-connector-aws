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
        } catch (OperationTimeoutException e) {
            LOG.error("Timeout while listing databases in Glue", e);
            throw new CatalogException("Timeout while listing databases: " + e.getMessage(), e);
        } catch (ResourceNumberLimitExceededException e) {
            LOG.error("Resource limit exceeded while listing databases in Glue", e);
            throw new CatalogException("Resource limit exceeded while listing databases: " + e.getMessage(), e);
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
        } catch (InvalidInputException e) {
            LOG.error("Invalid input while checking database existence: {}", databaseName, e);
            throw new CatalogException("Invalid database name: " + databaseName, e);
        } catch (OperationTimeoutException e) {
            LOG.error("Timeout while checking database existence: {}", databaseName, e);
            throw new CatalogException("Timeout while checking database existence: " + databaseName, e);
        } catch (GlueException e) {
            LOG.error("Error checking database existence: {}", databaseName, e);
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
                            .description(String.valueOf(catalogDatabase.getDescription()))
                            .parameters(catalogDatabase.getProperties())));
        } catch (AlreadyExistsException e) {
            throw new DatabaseAlreadyExistException(catalogName, databaseName);
        } catch (InvalidInputException e) {
            LOG.error("Invalid input while creating database: {}", databaseName, e);
            throw new CatalogException("Invalid database name or properties: " + databaseName, e);
        } catch (ResourceNumberLimitExceededException e) {
            LOG.error("Resource limit exceeded while creating database: {}", databaseName, e);
            throw new CatalogException("Resource limit exceeded while creating database: " + databaseName, e);
        } catch (OperationTimeoutException e) {
            LOG.error("Timeout while creating database: {}", databaseName, e);
            throw new CatalogException("Timeout while creating database: " + databaseName, e);
        } catch (GlueException e) {
            LOG.error("Error creating database: {}", databaseName, e);
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
        } catch (InvalidInputException e) {
            LOG.error("Invalid input while dropping database: {}", databaseName, e);
            throw new CatalogException("Invalid database name: " + databaseName, e);
        } catch (OperationTimeoutException e) {
            LOG.error("Timeout while dropping database: {}", databaseName, e);
            throw new CatalogException("Timeout while dropping database: " + databaseName, e);
        } catch (GlueException e) {
            LOG.error("Error dropping database: {}", databaseName, e);
            throw new CatalogException("Error dropping database: " + databaseName, e);
        }
    }
}
