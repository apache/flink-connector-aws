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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.glue.exception.UnsupportedDataTypeMappingException;
import org.apache.flink.table.catalog.glue.operator.GlueDatabaseOperator;
import org.apache.flink.table.catalog.glue.operator.GlueFunctionOperator;
import org.apache.flink.table.catalog.glue.operator.GluePartitionOperator;
import org.apache.flink.table.catalog.glue.operator.GlueTableOperator;
import org.apache.flink.table.catalog.glue.util.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueFlinkSchemaProperties;
import org.apache.flink.table.catalog.glue.util.GlueFunctionsUtil;
import org.apache.flink.table.catalog.glue.util.GlueTableUtils;
import org.apache.flink.table.catalog.glue.util.GlueTypeConverter;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * A Flink {@link org.apache.flink.table.catalog.Catalog} backed by the AWS Glue Data Catalog.
 *
 * <p>Databases, tables, views, partitions and functions are stored as their Glue counterparts.
 * Since Glue stores object names in lowercase, this catalog stores every object under its lowercase
 * name and records the declared name in a Glue parameter, so identifiers are presented to Flink
 * exactly as declared. Because Glue storage names are unique, every lookup resolves with a single
 * {@code Get*} call on the lowercase name; the catalog never scans Glue to resolve a name.
 *
 * <p>Parts of a Flink schema that Glue columns cannot represent (computed and metadata columns,
 * watermarks, primary keys, exact types such as {@code TIMESTAMP(3)}, NOT NULL constraints) are
 * persisted as {@code flink.schema.*} table parameters and restored on read, so tables created by
 * this catalog round-trip exactly. Tables created by other engines (Athena, Glue crawlers, Spark,
 * Hive) are exposed with the schema Glue holds for them.
 */
@PublicEvolving
public class GlueCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GlueCatalog.class);

    /** Glue table types that Flink exposes as views. */
    private static final List<String> GLUE_VIEW_TABLE_TYPES =
            Collections.unmodifiableList(
                    java.util.Arrays.asList(
                            CatalogBaseTable.TableKind.VIEW.name(),
                            "VIRTUAL_VIEW",
                            "MATERIALIZED_VIEW"));

    /** Characters Hive escapes in partition path segments, plus control characters. */
    private static final String PATH_UNSAFE_CHARS = "\"#%'*/:=?\\\u007F{[]^";

    private GlueClient glueClient;
    private GlueTypeConverter glueTypeConverter;
    private GlueDatabaseOperator glueDatabaseOperations;
    private GlueTableOperator glueTableOperations;
    private GlueFunctionOperator glueFunctionsOperations;
    private GluePartitionOperator gluePartitionOperations;
    private GlueTableUtils glueTableUtils;

    /**
     * Constructs a GlueCatalog with a provided Glue client.
     *
     * @param name the name of the catalog
     * @param defaultDatabase the default database for the catalog
     * @param region the AWS region to be used for Glue operations
     * @param glueClient the Glue client to use; when null a default client for the region is built
     */
    @VisibleForTesting
    GlueCatalog(String name, String defaultDatabase, String region, GlueClient glueClient) {
        super(name, defaultDatabase);
        Preconditions.checkNotNull(region, "region cannot be null");
        Preconditions.checkArgument(!region.trim().isEmpty(), "region cannot be empty");

        if (glueClient != null) {
            setup(glueClient);
        } else {
            setup(GlueClient.builder().region(Region.of(region)).build());
        }
    }

    /**
     * Constructs a GlueCatalog with default client configuration.
     *
     * @param name the name of the catalog
     * @param defaultDatabase the default database for the catalog
     * @param region the AWS region to be used for Glue operations
     */
    public GlueCatalog(String name, String defaultDatabase, String region) {
        this(name, defaultDatabase, region, new Properties());
    }

    /**
     * Constructs a GlueCatalog whose Glue client is built from the given AWS client properties,
     * using the same client-creation path as the other AWS connectors ({@link AWSClientUtil}). This
     * makes the standard {@code aws.*} settings available to the catalog - for example {@code
     * aws.credentials.provider} to select a credential mode, {@code aws.endpoint} to point at a
     * Glue-compatible endpoint, and the {@code aws.http-client.*} options.
     *
     * @param name the name of the catalog
     * @param defaultDatabase the default database for the catalog
     * @param region the AWS region to be used for Glue operations
     * @param glueClientProperties AWS client properties, keyed by {@link AWSConfigConstants}
     */
    public GlueCatalog(
            String name, String defaultDatabase, String region, Properties glueClientProperties) {
        super(name, defaultDatabase);
        Preconditions.checkNotNull(region, "region cannot be null");
        Preconditions.checkArgument(!region.trim().isEmpty(), "region cannot be empty");
        Preconditions.checkNotNull(glueClientProperties, "glueClientProperties cannot be null");

        Properties clientProperties = new Properties();
        clientProperties.putAll(glueClientProperties);
        // The explicit region argument wins over any aws.region property.
        clientProperties.setProperty(AWSConfigConstants.AWS_REGION, region);
        AWSGeneralUtil.validateAwsConfiguration(clientProperties);

        GlueClient client =
                AWSClientUtil.createAwsSyncClient(
                        clientProperties,
                        AWSGeneralUtil.createSyncHttpClient(
                                clientProperties, ApacheHttpClient.builder()),
                        GlueClient.builder(),
                        GlueCatalogConstants.BASE_GLUE_USER_AGENT_PREFIX_FORMAT,
                        GlueCatalogConstants.GLUE_CLIENT_USER_AGENT_PREFIX);
        setup(client);
    }

    private void setup(GlueClient glueClient) {
        this.glueClient = glueClient;
        this.glueTypeConverter = new GlueTypeConverter();
        this.glueTableUtils = new GlueTableUtils(glueTypeConverter);
        this.glueDatabaseOperations = new GlueDatabaseOperator(glueClient, getName());
        this.glueTableOperations = new GlueTableOperator(glueClient, getName());
        this.glueFunctionsOperations = new GlueFunctionOperator(glueClient, getName());
        this.gluePartitionOperations = new GluePartitionOperator(glueClient, getName());
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("Opening GlueCatalog '{}'", getName());
    }

    @Override
    public void close() throws CatalogException {
        if (glueClient != null) {
            LOG.info("Closing GlueCatalog '{}'", getName());
            glueClient.close();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Databases
    // ------------------------------------------------------------------------------------------

    @Override
    public List<String> listDatabases() throws CatalogException {
        return glueDatabaseOperations.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkDatabaseName(databaseName);
        return glueDatabaseOperations.getDatabase(databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkDatabaseName(databaseName);
        return glueDatabaseOperations.glueDatabaseExists(databaseName);
    }

    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase catalogDatabase, boolean ifNotExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkDatabaseName(databaseName);
        Preconditions.checkNotNull(catalogDatabase, "CatalogDatabase cannot be null");

        // One GetDatabase both answers "does it exist" and tells us how it was declared, so the
        // error can explain a case-only clash (Glue stores names in lowercase).
        Database existing = glueDatabaseOperations.getGlueDatabaseOrNull(databaseName);
        if (existing != null) {
            if (ifNotExists) {
                return;
            }
            throw databaseAlreadyExists(
                    databaseName, glueDatabaseOperations.getOriginalDatabaseName(existing));
        }
        glueDatabaseOperations.createDatabase(databaseName, catalogDatabase);
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkDatabaseName(databaseName);

        String glueDatabaseName = glueDatabaseOperations.findGlueDatabaseName(databaseName);
        if (glueDatabaseName == null) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // GetTables already returns views (they are Glue tables), so one listing covers both.
        List<Table> tables = glueTableOperations.getAllGlueTables(glueDatabaseName);
        List<String> functions = glueFunctionsOperations.listGlueFunctions(glueDatabaseName);
        if (!tables.isEmpty() || !functions.isEmpty()) {
            if (!cascade) {
                throw new DatabaseNotEmptyException(getName(), databaseName);
            }
            for (Table table : tables) {
                try {
                    glueTableOperations.dropTable(glueDatabaseName, table.name());
                } catch (TableNotExistException e) {
                    LOG.debug("Table {} vanished during cascading drop", table.name());
                }
            }
            for (String function : functions) {
                try {
                    glueFunctionsOperations.dropGlueFunction(
                            new ObjectPath(glueDatabaseName, function));
                } catch (FunctionNotExistException e) {
                    LOG.debug("Function {} vanished during cascading drop", function);
                }
            }
        }
        glueDatabaseOperations.dropGlueDatabase(databaseName);
    }

    @Override
    public void alterDatabase(
            String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException(
                "Altering databases is not supported by the Glue Catalog.");
    }

    // ------------------------------------------------------------------------------------------
    // Tables and views
    // ------------------------------------------------------------------------------------------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return glueTableOperations.listTables(requireGlueDatabaseName(databaseName));
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        String glueDatabaseName = requireGlueDatabaseName(databaseName);
        return glueTableOperations.getAllGlueTables(glueDatabaseName).stream()
                .filter(table -> resolveTableKind(table) == CatalogBaseTable.TableKind.VIEW)
                .map(glueTableOperations::getOriginalTableName)
                .collect(Collectors.toList());
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        Table glueTable = getGlueTableOrNull(objectPath);
        if (glueTable == null) {
            throw new TableNotExistException(getName(), objectPath);
        }
        return toCatalogBaseTable(objectPath, glueTable);
    }

    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        return getGlueTableOrNull(objectPath) != null;
    }

    @Override
    public void dropTable(ObjectPath objectPath, boolean ifExists)
            throws TableNotExistException, CatalogException {
        Preconditions.checkNotNull(objectPath, "ObjectPath cannot be null");
        String glueDatabaseName =
                glueDatabaseOperations.findGlueDatabaseName(objectPath.getDatabaseName());
        if (glueDatabaseName == null) {
            if (ifExists) {
                return;
            }
            throw new TableNotExistException(getName(), objectPath);
        }
        try {
            glueTableOperations.dropTable(glueDatabaseName, objectPath.getObjectName());
        } catch (TableNotExistException e) {
            if (!ifExists) {
                throw new TableNotExistException(getName(), objectPath, e);
            }
        }
    }

    @Override
    public void createTable(
            ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean ifNotExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        Preconditions.checkNotNull(objectPath, "ObjectPath cannot be null");
        Preconditions.checkNotNull(catalogBaseTable, "CatalogBaseTable cannot be null");

        String glueDatabaseName = requireGlueDatabaseName(objectPath.getDatabaseName());

        // One GetTable both answers "does it exist" and tells us how it was declared, so the
        // error can explain a case-only clash (Glue stores names in lowercase).
        Table existing =
                glueTableOperations.getGlueTableOrNull(
                        glueDatabaseName, objectPath.getObjectName());
        if (existing != null) {
            if (ifNotExists) {
                return;
            }
            throw tableAlreadyExists(
                    objectPath, glueTableOperations.getOriginalTableName(existing));
        }

        ResolvedSchema resolvedSchema = requireResolvedSchema(objectPath, catalogBaseTable);
        Map<String, String> options = new HashMap<>(catalogBaseTable.getOptions());
        rejectReservedOptions(objectPath, options);

        TableInput tableInput;
        switch (catalogBaseTable.getTableKind()) {
            case TABLE:
                CatalogTable catalogTable = (CatalogTable) catalogBaseTable;
                tableInput =
                        buildTableInput(
                                objectPath.getObjectName(),
                                CatalogBaseTable.TableKind.TABLE,
                                catalogTable.getComment(),
                                resolvedSchema,
                                catalogTable.getPartitionKeys(),
                                options,
                                glueTableUtils.resolveTableLocation(
                                        options,
                                        GlueDatabaseOperator.toGlueDatabaseName(
                                                objectPath.getDatabaseName()),
                                        GlueTableOperator.toGlueTableName(
                                                objectPath.getObjectName()),
                                        !catalogTable.getPartitionKeys().isEmpty()));
                break;
            case VIEW:
                CatalogView catalogView = (CatalogView) catalogBaseTable;
                tableInput =
                        buildTableInput(
                                        objectPath.getObjectName(),
                                        CatalogBaseTable.TableKind.VIEW,
                                        catalogView.getComment(),
                                        resolvedSchema,
                                        Collections.emptyList(),
                                        options,
                                        null)
                                .toBuilder()
                                .viewOriginalText(catalogView.getOriginalQuery())
                                .viewExpandedText(catalogView.getExpandedQuery())
                                .build();
                break;
            default:
                throw new CatalogException(
                        String.format(
                                "Cannot create %s: table kind %s is not supported by the Glue Catalog",
                                objectPath.getFullName(), catalogBaseTable.getTableKind()));
        }

        try {
            glueTableOperations.createTable(glueDatabaseName, tableInput);
        } catch (TableAlreadyExistException e) {
            // Created concurrently between our existence check and the create.
            if (!ifNotExists) {
                throw new TableAlreadyExistException(getName(), objectPath, e);
            }
        }
        LOG.info(
                "Created {} {} in Glue", catalogBaseTable.getTableKind(), objectPath.getFullName());
    }

    @Override
    public void renameTable(ObjectPath objectPath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException(
                "Renaming tables is not supported by the Glue Catalog.");
    }

    @Override
    public void alterTable(
            ObjectPath objectPath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        Preconditions.checkNotNull(objectPath, "ObjectPath cannot be null");
        Preconditions.checkNotNull(newTable, "CatalogBaseTable cannot be null");

        Table existing = getGlueTableOrNull(objectPath);
        if (existing == null) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(getName(), objectPath);
        }

        if (newTable.getTableKind() != CatalogBaseTable.TableKind.TABLE) {
            throw new UnsupportedOperationException(
                    "Altering views is not supported by the Glue Catalog.");
        }
        if (resolveTableKind(existing) != CatalogBaseTable.TableKind.TABLE) {
            throw new CatalogException(
                    String.format(
                            "Cannot alter %s as a table: the Glue object is a %s",
                            objectPath.getFullName(), existing.tableType()));
        }

        CatalogTable catalogTable = (CatalogTable) newTable;
        List<String> existingPartitionKeys = GlueTableUtils.getPartitionKeyNames(existing);
        if (!existingPartitionKeys.equals(catalogTable.getPartitionKeys())) {
            throw new CatalogException(
                    String.format(
                            "Cannot alter %s: changing the partition keys (%s -> %s) is not "
                                    + "supported because existing partitions would become "
                                    + "unreachable",
                            objectPath.getFullName(),
                            existingPartitionKeys,
                            catalogTable.getPartitionKeys()));
        }

        ResolvedSchema resolvedSchema = requireResolvedSchema(objectPath, newTable);
        Map<String, String> options = new HashMap<>(catalogTable.getOptions());
        rejectReservedOptions(objectPath, options);

        String glueDatabaseName =
                GlueDatabaseOperator.toGlueDatabaseName(objectPath.getDatabaseName());
        TableInput tableInput =
                buildTableInput(
                        // Preserve the originally declared table name (case) across the alter.
                        glueTableOperations.getOriginalTableName(existing),
                        CatalogBaseTable.TableKind.TABLE,
                        catalogTable.getComment(),
                        resolvedSchema,
                        catalogTable.getPartitionKeys(),
                        options,
                        glueTableUtils.resolveTableLocation(
                                options,
                                glueDatabaseName,
                                existing.name(),
                                !catalogTable.getPartitionKeys().isEmpty()));
        glueTableOperations.updateTable(glueDatabaseName, tableInput);
        LOG.info("Altered table {} in Glue", objectPath.getFullName());
    }

    // ------------------------------------------------------------------------------------------
    // Partitions
    // ------------------------------------------------------------------------------------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        GlueTableRef tableRef = resolvePartitionedTable(objectPath);
        List<String> partitionKeys = tableRef.partitionKeys();
        return gluePartitionOperations
                .listPartitions(tableRef.databaseName, tableRef.tableName)
                .stream()
                .map(partition -> toPartitionSpec(objectPath, partitionKeys, partition.values()))
                .collect(Collectors.toList());
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws TableNotExistException,
                    TableNotPartitionedException,
                    PartitionSpecInvalidException,
                    CatalogException {
        GlueTableRef tableRef = resolvePartitionedTable(objectPath);
        List<String> partitionKeys = tableRef.partitionKeys();

        Map<String, String> partialSpec =
                catalogPartitionSpec == null
                        ? Collections.emptyMap()
                        : catalogPartitionSpec.getPartitionSpec();
        // Flink's Catalog contract: a partial spec referencing unknown partition keys is invalid.
        if (!partitionKeys.containsAll(partialSpec.keySet())) {
            throw new PartitionSpecInvalidException(
                    getName(), partitionKeys, objectPath, catalogPartitionSpec);
        }

        return gluePartitionOperations
                .listPartitions(tableRef.databaseName, tableRef.tableName)
                .stream()
                .map(partition -> toPartitionSpec(objectPath, partitionKeys, partition.values()))
                .filter(
                        spec ->
                                spec.getPartitionSpec()
                                        .entrySet()
                                        .containsAll(partialSpec.entrySet()))
                .collect(Collectors.toList());
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath objectPath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        // Expression push-down to Glue partition filters is not implemented. Flink's planner
        // catches UnsupportedOperationException and falls back to listPartitions().
        throw new UnsupportedOperationException(
                "Listing partitions by filter expression is not supported by the Glue Catalog.");
    }

    @Override
    public CatalogPartition getPartition(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        Partition partition = getGluePartitionOrNull(objectPath, catalogPartitionSpec);
        if (partition == null) {
            throw new PartitionNotExistException(getName(), objectPath, catalogPartitionSpec);
        }

        Map<String, String> properties = new HashMap<>();
        if (partition.parameters() != null) {
            properties.putAll(partition.parameters());
        }
        if (partition.storageDescriptor() != null
                && partition.storageDescriptor().location() != null) {
            properties.put(
                    GlueCatalogConstants.PARTITION_LOCATION,
                    partition.storageDescriptor().location());
        }
        return new CatalogPartitionImpl(properties, null);
    }

    @Override
    public boolean partitionExists(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        try {
            return getGluePartitionOrNull(objectPath, catalogPartitionSpec) != null;
        } catch (PartitionNotExistException e) {
            return false;
        }
    }

    @Override
    public void createPartition(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogPartition catalogPartition,
            boolean ifNotExists)
            throws TableNotExistException,
                    TableNotPartitionedException,
                    PartitionSpecInvalidException,
                    PartitionAlreadyExistsException,
                    CatalogException {
        Preconditions.checkNotNull(catalogPartitionSpec, "CatalogPartitionSpec cannot be null");
        GlueTableRef tableRef = resolvePartitionedTable(objectPath);
        List<String> partitionKeys = tableRef.partitionKeys();

        Map<String, String> spec = catalogPartitionSpec.getPartitionSpec();
        if (!spec.keySet().equals(new HashSet<>(partitionKeys))) {
            throw new PartitionSpecInvalidException(
                    getName(), partitionKeys, objectPath, catalogPartitionSpec);
        }
        List<String> partitionValues =
                partitionKeys.stream().map(spec::get).collect(Collectors.toList());

        Map<String, String> partitionProperties =
                catalogPartition == null
                        ? new HashMap<>()
                        : new HashMap<>(catalogPartition.getProperties());
        String location = partitionProperties.remove(GlueCatalogConstants.PARTITION_LOCATION);
        String tableLocation =
                tableRef.glueTable.storageDescriptor() != null
                        ? tableRef.glueTable.storageDescriptor().location()
                        : null;
        StorageDescriptor.Builder sdBuilder =
                tableRef.glueTable.storageDescriptor() != null
                        ? tableRef.glueTable.storageDescriptor().toBuilder()
                        : StorageDescriptor.builder();
        if (location != null) {
            validateExplicitPartitionLocation(
                    location,
                    tableLocation,
                    partitionKeys,
                    partitionValues,
                    objectPath,
                    catalogPartitionSpec);
            sdBuilder.location(location);
        } else if (tableLocation != null) {
            sdBuilder.location(
                    buildDefaultPartitionLocation(tableLocation, partitionKeys, partitionValues));
        }

        PartitionInput partitionInput =
                PartitionInput.builder()
                        .values(partitionValues)
                        .storageDescriptor(sdBuilder.build())
                        .parameters(partitionProperties)
                        .build();

        try {
            gluePartitionOperations.createPartition(
                    tableRef.databaseName, tableRef.tableName, partitionInput);
        } catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException e) {
            if (!ifNotExists) {
                throw new PartitionAlreadyExistsException(
                        getName(), objectPath, catalogPartitionSpec, e);
            }
        } catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException e) {
            throw new TableNotExistException(getName(), objectPath, e);
        }
    }

    @Override
    public void dropPartition(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        try {
            GlueTableRef tableRef = resolvePartitionedTable(objectPath);
            List<String> partitionValues =
                    toOrderedPartitionValues(tableRef, objectPath, catalogPartitionSpec);
            gluePartitionOperations.dropPartition(
                    tableRef.databaseName, tableRef.tableName, partitionValues);
        } catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException
                | PartitionNotExistException
                | TableNotExistException
                | TableNotPartitionedException e) {
            if (!ignoreIfNotExists) {
                // Chain the original exception so the real cause (missing table, table not
                // partitioned, or missing partition) stays visible for debugging.
                throw new PartitionNotExistException(
                        getName(), objectPath, catalogPartitionSpec, e);
            }
        }
    }

    @Override
    public void alterPartition(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogPartition catalogPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        try {
            GlueTableRef tableRef = resolvePartitionedTable(objectPath);
            List<String> partitionValues =
                    toOrderedPartitionValues(tableRef, objectPath, catalogPartitionSpec);
            Partition existing =
                    gluePartitionOperations.getPartition(
                            tableRef.databaseName, tableRef.tableName, partitionValues);
            if (existing == null) {
                throw new PartitionNotExistException(getName(), objectPath, catalogPartitionSpec);
            }

            Map<String, String> partitionProperties =
                    catalogPartition == null
                            ? new HashMap<>()
                            : new HashMap<>(catalogPartition.getProperties());
            String location = partitionProperties.remove(GlueCatalogConstants.PARTITION_LOCATION);
            StorageDescriptor.Builder sdBuilder =
                    existing.storageDescriptor() != null
                            ? existing.storageDescriptor().toBuilder()
                            : StorageDescriptor.builder();
            if (location != null) {
                validateExplicitPartitionLocation(
                        location,
                        tableRef.glueTable.storageDescriptor() != null
                                ? tableRef.glueTable.storageDescriptor().location()
                                : null,
                        tableRef.partitionKeys(),
                        partitionValues,
                        objectPath,
                        catalogPartitionSpec);
                sdBuilder.location(location);
            }

            PartitionInput partitionInput =
                    PartitionInput.builder()
                            .values(partitionValues)
                            .storageDescriptor(sdBuilder.build())
                            .parameters(partitionProperties)
                            .build();

            gluePartitionOperations.updatePartition(
                    tableRef.databaseName, tableRef.tableName, partitionValues, partitionInput);
        } catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException
                | TableNotExistException
                | TableNotPartitionedException e) {
            if (!ignoreIfNotExists) {
                throw new PartitionNotExistException(
                        getName(), objectPath, catalogPartitionSpec, e);
            }
        } catch (PartitionNotExistException e) {
            if (!ignoreIfNotExists) {
                throw e;
            }
        }
    }

    /** Resolved Glue storage names plus the fetched Glue table for partition operations. */
    private static final class GlueTableRef {
        private final String databaseName;
        private final String tableName;
        private final Table glueTable;

        private GlueTableRef(String databaseName, String tableName, Table glueTable) {
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.glueTable = glueTable;
        }

        private List<String> partitionKeys() {
            return GlueTableUtils.getPartitionKeyNames(glueTable);
        }
    }

    /**
     * Resolves the given path to its Glue table (one GetDatabase + one GetTable) and validates that
     * the table is partitioned.
     */
    private GlueTableRef resolvePartitionedTable(ObjectPath objectPath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        Table glueTable = getGlueTableOrNull(objectPath);
        if (glueTable == null) {
            throw new TableNotExistException(getName(), objectPath);
        }
        if (glueTable.partitionKeys() == null || glueTable.partitionKeys().isEmpty()) {
            throw new TableNotPartitionedException(getName(), objectPath);
        }
        return new GlueTableRef(
                GlueDatabaseOperator.toGlueDatabaseName(objectPath.getDatabaseName()),
                glueTable.name(),
                glueTable);
    }

    /**
     * Resolves a full partition spec into partition values ordered by the table's partition keys.
     * An incomplete or mismatched spec identifies a partition that cannot exist, so {@link
     * PartitionNotExistException} is thrown (matching the Flink Catalog contract for
     * partition-addressing methods that do not declare PartitionSpecInvalidException).
     */
    private List<String> toOrderedPartitionValues(
            GlueTableRef tableRef, ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException {
        if (catalogPartitionSpec == null) {
            throw new PartitionNotExistException(getName(), objectPath, null);
        }
        List<String> partitionKeys = tableRef.partitionKeys();
        Map<String, String> spec = catalogPartitionSpec.getPartitionSpec();
        if (!spec.keySet().equals(new HashSet<>(partitionKeys))) {
            throw new PartitionNotExistException(getName(), objectPath, catalogPartitionSpec);
        }
        return partitionKeys.stream().map(spec::get).collect(Collectors.toList());
    }

    private Partition getGluePartitionOrNull(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        try {
            GlueTableRef tableRef = resolvePartitionedTable(objectPath);
            List<String> partitionValues =
                    toOrderedPartitionValues(tableRef, objectPath, catalogPartitionSpec);
            return gluePartitionOperations.getPartition(
                    tableRef.databaseName, tableRef.tableName, partitionValues);
        } catch (TableNotExistException | TableNotPartitionedException e) {
            throw new PartitionNotExistException(getName(), objectPath, catalogPartitionSpec, e);
        }
    }

    private static CatalogPartitionSpec toPartitionSpec(
            ObjectPath objectPath, List<String> partitionKeys, List<String> values) {
        if (values == null || values.size() != partitionKeys.size()) {
            // Truncating to the shorter list would return a spec that names a partition which
            // does not exist. This happens when another engine changed the table's partition
            // keys after partitions were created, or the partition is otherwise corrupt.
            throw new CatalogException(
                    String.format(
                            "Partition %s of table %s has %d value(s) but the table declares %d "
                                    + "partition key(s) %s; the Glue partition does not match the "
                                    + "table definition.",
                            values,
                            objectPath.getFullName(),
                            values == null ? 0 : values.size(),
                            partitionKeys.size(),
                            partitionKeys));
        }
        Map<String, String> spec = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            spec.put(partitionKeys.get(i), values.get(i));
        }
        return new CatalogPartitionSpec(spec);
    }

    /**
     * Builds the default Hive-style partition location {@code location/key1=value1/key2=value2},
     * escaping characters that are not valid in a path segment the same way Hive does.
     */
    @VisibleForTesting
    static String buildDefaultPartitionLocation(
            String tableLocation, List<String> partitionKeys, List<String> partitionValues) {
        StringBuilder location = new StringBuilder(tableLocation);
        for (int i = 0; i < partitionKeys.size(); i++) {
            location.append('/')
                    .append(escapePathName(partitionKeys.get(i)))
                    .append('=')
                    .append(escapePathName(partitionValues.get(i)));
        }
        return location.toString();
    }

    /**
     * Guards the explicit {@link GlueCatalogConstants#PARTITION_LOCATION} property against the most
     * common misuse: copying the properties returned by {@link #getPartition} for one partition
     * into {@link #createPartition}/{@link #alterPartition} of another, which would point the new
     * partition at the old partition's data.
     *
     * <p>Only locations the catalog itself would have generated are checked: a location under the
     * table location whose trailing path segments spell a Hive-style {@code key=value} chain for
     * this table's partition keys must spell <em>this</em> partition's values. Any other location
     * (a different bucket, a custom layout) is a deliberate choice and is accepted as-is. A table
     * without a location generates no default partition locations, so nothing is checked.
     */
    private static void validateExplicitPartitionLocation(
            String location,
            String tableLocation,
            List<String> partitionKeys,
            List<String> partitionValues,
            ObjectPath objectPath,
            CatalogPartitionSpec spec)
            throws CatalogException {
        if (tableLocation == null) {
            return;
        }
        String tableRoot = tableLocation.endsWith("/") ? tableLocation : tableLocation + "/";
        if (!location.startsWith(tableRoot)) {
            return;
        }
        String[] segments = location.substring(tableRoot.length()).split("/");
        if (segments.length != partitionKeys.size()) {
            return;
        }
        for (int i = 0; i < partitionKeys.size(); i++) {
            String expectedPrefix = escapePathName(partitionKeys.get(i)) + "=";
            if (!segments[i].startsWith(expectedPrefix)) {
                // Not a Hive-style path for this table's keys: a custom layout, accept it.
                return;
            }
            if (!segments[i].equals(expectedPrefix + escapePathName(partitionValues.get(i)))) {
                throw new CatalogException(
                        String.format(
                                "Partition %s of table %s cannot use location '%s': it is the "
                                        + "location of a different partition of the same table. "
                                        + "Remove the '%s' property to use the default location, "
                                        + "or provide a location that does not belong to another "
                                        + "partition.",
                                spec.getPartitionSpec(),
                                objectPath.getFullName(),
                                location,
                                GlueCatalogConstants.PARTITION_LOCATION));
            }
        }
    }

    private static String escapePathName(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < ' ' || PATH_UNSAFE_CHARS.indexOf(c) >= 0) {
                sb.append('%').append(String.format("%02X", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------------------------
    // Functions
    // ------------------------------------------------------------------------------------------

    @Override
    public List<String> listFunctions(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return glueFunctionsOperations.listGlueFunctions(requireGlueDatabaseName(databaseName));
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        ObjectPath normalizedPath = normalize(functionPath);
        ObjectPath gluePath = toGlueFunctionPathOrNull(normalizedPath);
        if (gluePath == null) {
            // A function in a non-existent database does not exist: report it per the
            // Catalog contract so the planner can fall back to built-in functions instead
            // of failing SQL validation (matches Hive/GenericInMemoryCatalog behaviour).
            throw new FunctionNotExistException(getName(), normalizedPath);
        }
        try {
            return glueFunctionsOperations.getGlueFunction(gluePath);
        } catch (FunctionNotExistException e) {
            throw new FunctionNotExistException(getName(), normalizedPath);
        }
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        ObjectPath gluePath = toGlueFunctionPathOrNull(normalize(functionPath));
        return gluePath != null && glueFunctionsOperations.glueFunctionExists(gluePath);
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        Preconditions.checkNotNull(function, "CatalogFunction cannot be null");
        ObjectPath normalizedPath = normalize(functionPath);
        String glueDatabaseName = requireGlueDatabaseName(normalizedPath.getDatabaseName());
        ObjectPath gluePath = new ObjectPath(glueDatabaseName, normalizedPath.getObjectName());
        try {
            // Glue's own AlreadyExists check replaces a separate existence round-trip.
            glueFunctionsOperations.createGlueFunction(gluePath, function);
        } catch (FunctionAlreadyExistException e) {
            if (!ignoreIfExists) {
                throw new FunctionAlreadyExistException(getName(), normalizedPath, e);
            }
        }
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        Preconditions.checkNotNull(newFunction, "CatalogFunction cannot be null");
        ObjectPath normalizedPath = normalize(functionPath);
        ObjectPath gluePath = toGlueFunctionPathOrNull(normalizedPath);
        UserDefinedFunction existing =
                gluePath == null ? null : glueFunctionsOperations.getGlueFunctionOrNull(gluePath);
        if (existing == null) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new FunctionNotExistException(getName(), normalizedPath);
        }

        FunctionLanguage existingLanguage = GlueFunctionsUtil.getFunctionalLanguage(existing);
        if (existingLanguage != newFunction.getFunctionLanguage()) {
            throw new CatalogException(
                    String.format(
                            "Cannot alter function %s: the existing function is a %s function "
                                    + "and the new definition is %s",
                            normalizedPath.getFullName(),
                            existingLanguage,
                            newFunction.getFunctionLanguage()));
        }
        try {
            glueFunctionsOperations.alterGlueFunction(gluePath, newFunction);
        } catch (FunctionNotExistException e) {
            if (!ignoreIfNotExists) {
                throw new FunctionNotExistException(getName(), normalizedPath, e);
            }
        }
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        ObjectPath normalizedPath = normalize(functionPath);
        ObjectPath gluePath = toGlueFunctionPathOrNull(normalizedPath);
        if (gluePath == null) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new FunctionNotExistException(getName(), normalizedPath);
        }
        try {
            glueFunctionsOperations.dropGlueFunction(gluePath);
        } catch (FunctionNotExistException e) {
            if (!ignoreIfNotExists) {
                throw new FunctionNotExistException(getName(), normalizedPath, e);
            }
        }
    }

    /**
     * Normalizes a function path the way Flink resolves function identifiers (case-insensitive
     * function names).
     */
    private ObjectPath normalize(ObjectPath path) {
        Preconditions.checkNotNull(path, "ObjectPath cannot be null");
        return new ObjectPath(
                path.getDatabaseName(), FunctionIdentifier.normalizeName(path.getObjectName()));
    }

    /**
     * Translates a normalized function path to Glue storage names with a single GetDatabase call,
     * or returns null when the database does not exist.
     */
    private ObjectPath toGlueFunctionPathOrNull(ObjectPath normalizedPath) throws CatalogException {
        String glueDatabaseName =
                glueDatabaseOperations.findGlueDatabaseName(normalizedPath.getDatabaseName());
        return glueDatabaseName == null
                ? null
                : new ObjectPath(glueDatabaseName, normalizedPath.getObjectName());
    }

    // ------------------------------------------------------------------------------------------
    // Statistics (not supported by Glue)
    // ------------------------------------------------------------------------------------------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath objectPath,
            CatalogTableStatistics catalogTableStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException(
                "Altering table statistics is not supported by the Glue Catalog.");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath objectPath,
            CatalogColumnStatistics catalogColumnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException(
                "Altering table column statistics is not supported by the Glue Catalog.");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogTableStatistics catalogTableStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException(
                "Altering partition statistics is not supported by the Glue Catalog.");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogColumnStatistics catalogColumnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException(
                "Altering partition column statistics is not supported by the Glue Catalog.");
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private static void checkDatabaseName(String databaseName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");
    }

    /** Resolves a Flink database name to its Glue storage name or throws (one GetDatabase). */
    private String requireGlueDatabaseName(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkDatabaseName(databaseName);
        String glueDatabaseName = glueDatabaseOperations.findGlueDatabaseName(databaseName);
        if (glueDatabaseName == null) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return glueDatabaseName;
    }

    /** Resolves a table path to its Glue table (one GetDatabase + one GetTable), or null. */
    private Table getGlueTableOrNull(ObjectPath objectPath) throws CatalogException {
        Preconditions.checkNotNull(objectPath, "ObjectPath cannot be null");
        String glueDatabaseName =
                glueDatabaseOperations.findGlueDatabaseName(objectPath.getDatabaseName());
        if (glueDatabaseName == null) {
            return null;
        }
        return glueTableOperations.getGlueTableOrNull(glueDatabaseName, objectPath.getObjectName());
    }

    private DatabaseAlreadyExistException databaseAlreadyExists(
            String databaseName, String existingDeclaredName) {
        if (existingDeclaredName.equals(databaseName)) {
            return new DatabaseAlreadyExistException(getName(), databaseName);
        }
        return new DatabaseAlreadyExistException(
                getName(),
                databaseName,
                new CatalogException(
                        String.format(
                                "Database '%s' conflicts with existing database '%s': AWS Glue "
                                        + "stores database names in lowercase, so both would be "
                                        + "stored as '%s'",
                                databaseName,
                                existingDeclaredName,
                                GlueDatabaseOperator.toGlueDatabaseName(databaseName))));
    }

    private TableAlreadyExistException tableAlreadyExists(
            ObjectPath objectPath, String existingDeclaredName) {
        if (existingDeclaredName.equals(objectPath.getObjectName())) {
            return new TableAlreadyExistException(getName(), objectPath);
        }
        return new TableAlreadyExistException(
                getName(),
                objectPath,
                new CatalogException(
                        String.format(
                                "Table '%s' conflicts with existing table '%s.%s': AWS Glue "
                                        + "stores table names in lowercase, so both would be "
                                        + "stored as '%s'",
                                objectPath.getFullName(),
                                objectPath.getDatabaseName(),
                                existingDeclaredName,
                                GlueTableOperator.toGlueTableName(objectPath.getObjectName()))));
    }

    /**
     * Returns the resolved schema of a table or view. Glue needs concrete column types, so an
     * unresolved {@link CatalogTable} (which only the Table API can produce, by bypassing the
     * planner) cannot be stored.
     */
    private ResolvedSchema requireResolvedSchema(ObjectPath objectPath, CatalogBaseTable table) {
        if (table instanceof ResolvedCatalogBaseTable) {
            return ((ResolvedCatalogBaseTable<?>) table).getResolvedSchema();
        }
        throw new CatalogException(
                String.format(
                        "Cannot store %s in AWS Glue: the %s must be resolved (a %s was given). "
                                + "Create it through SQL DDL or the Table API so its schema is "
                                + "resolved before it reaches the catalog.",
                        objectPath.getFullName(),
                        table.getTableKind().name().toLowerCase(),
                        table.getClass().getSimpleName()));
    }

    /**
     * Rejects table options that collide with the parameters this catalog uses internally; they
     * would otherwise be misread as schema metadata or declared names on the way back.
     */
    private static void rejectReservedOptions(ObjectPath objectPath, Map<String, String> options) {
        for (String key : options.keySet()) {
            if (GlueFlinkSchemaProperties.isSchemaParameter(key)
                    || GlueCatalogConstants.ORIGINAL_TABLE_NAME.equals(key)
                    || GlueCatalogConstants.ORIGINAL_DATABASE_NAME.equals(key)
                    || GlueCatalogConstants.ORIGINAL_PARTITION_KEYS.equals(key)) {
                throw new CatalogException(
                        String.format(
                                "Cannot create %s: table option '%s' is reserved by the Glue "
                                        + "Catalog (options starting with '%s' and the "
                                        + "'flink.original-*' keys are used internally)",
                                objectPath.getFullName(),
                                key,
                                GlueFlinkSchemaProperties.SCHEMA_PARAMETER_PREFIX));
            }
        }
    }

    /**
     * Builds the Glue {@link TableInput} for a table or view: physical columns go to the storage
     * descriptor (partition columns to the table-level partition keys), and everything Glue columns
     * cannot represent is serialized into {@code flink.schema.*} parameters.
     */
    private TableInput buildTableInput(
            String tableName,
            CatalogBaseTable.TableKind tableKind,
            String comment,
            ResolvedSchema resolvedSchema,
            List<String> partitionKeys,
            Map<String, String> options,
            String location) {

        List<software.amazon.awssdk.services.glue.model.Column> dataColumns = new ArrayList<>();
        Map<String, software.amazon.awssdk.services.glue.model.Column> partitionColumnsByName =
                new HashMap<>();
        for (Column flinkColumn : resolvedSchema.getColumns()) {
            if (!flinkColumn.isPhysical()) {
                // Computed and metadata columns cannot be represented as Glue columns;
                // they are persisted as flink.schema.* table parameters instead.
                continue;
            }
            software.amazon.awssdk.services.glue.model.Column glueColumn =
                    glueTableUtils.mapFlinkColumnToGlueColumn(flinkColumn);
            if (partitionKeys.contains(flinkColumn.getName())) {
                partitionColumnsByName.put(flinkColumn.getName(), glueColumn);
            } else {
                dataColumns.add(glueColumn);
            }
        }
        // Preserve the declared partition-key order.
        List<software.amazon.awssdk.services.glue.model.Column> partitionColumns =
                partitionKeys.stream()
                        .map(partitionColumnsByName::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        Map<String, String> parameters = new HashMap<>(options);
        GlueFlinkSchemaProperties.serializeNonPhysicalSchema(resolvedSchema, parameters);

        return glueTableOperations.buildTableInput(
                tableName,
                tableKind,
                comment,
                partitionColumns,
                glueTableUtils.buildStorageDescriptor(dataColumns, location),
                parameters);
    }

    /**
     * Maps a Glue table type to the Flink table kind. Glue itself writes {@code EXTERNAL_TABLE} /
     * {@code VIRTUAL_VIEW} (Athena, crawlers, Spark, Hive); this catalog writes {@code TABLE} /
     * {@code VIEW}. Anything that is not a view is a table.
     */
    private static CatalogBaseTable.TableKind resolveTableKind(Table glueTable) {
        String tableType = glueTable.tableType();
        if (tableType != null && GLUE_VIEW_TABLE_TYPES.contains(tableType.toUpperCase())) {
            return CatalogBaseTable.TableKind.VIEW;
        }
        return CatalogBaseTable.TableKind.TABLE;
    }

    /** Converts a Glue table to a Flink {@link CatalogTable} or {@link CatalogView}. */
    private CatalogBaseTable toCatalogBaseTable(ObjectPath objectPath, Table glueTable) {
        Schema schema;
        try {
            schema = glueTableUtils.getSchemaFromGlueTable(glueTable);
        } catch (UnsupportedDataTypeMappingException e) {
            throw new CatalogException(
                    String.format(
                            "Cannot read %s: %s. The table was created by another engine with a "
                                    + "column type that has no Flink equivalent.",
                            objectPath.getFullName(), e.getMessage()),
                    e);
        }

        // Options are exactly the Glue table parameters the write path produced, minus the
        // catalog's own bookkeeping. Glue storage metadata (owner, input/output format, storage
        // descriptor parameters) is deliberately NOT surfaced as options: nothing on the write
        // path consumes it, so exposing it would (a) turn a getTable -> createTable copy into a
        // table with stray parameters and (b) hand unknown keys to the connector factory, which
        // rejects them at query time.
        Map<String, String> options = new HashMap<>();
        if (glueTable.parameters() != null) {
            for (Map.Entry<String, String> entry : glueTable.parameters().entrySet()) {
                String key = entry.getKey();
                if (!GlueCatalogConstants.ORIGINAL_TABLE_NAME.equals(key)
                        && !GlueCatalogConstants.ORIGINAL_DATABASE_NAME.equals(key)
                        && !GlueCatalogConstants.ORIGINAL_PARTITION_KEYS.equals(key)
                        && !GlueFlinkSchemaProperties.isSchemaParameter(key)) {
                    options.put(key, entry.getValue());
                }
            }
        }

        if (resolveTableKind(glueTable) == CatalogBaseTable.TableKind.VIEW) {
            String originalQuery = glueTable.viewOriginalText();
            if (originalQuery == null) {
                throw new CatalogException(
                        String.format(
                                "Cannot read view %s: Glue holds no query text for it",
                                objectPath.getFullName()));
            }
            String expandedQuery =
                    glueTable.viewExpandedText() != null
                            ? glueTable.viewExpandedText()
                            : originalQuery;
            return CatalogView.of(
                    schema, glueTable.description(), originalQuery, expandedQuery, options);
        }

        return CatalogTable.newBuilder()
                .schema(schema)
                .comment(glueTable.description())
                .partitionKeys(GlueTableUtils.getPartitionKeyNames(glueTable))
                .options(options)
                .build();
    }
}
