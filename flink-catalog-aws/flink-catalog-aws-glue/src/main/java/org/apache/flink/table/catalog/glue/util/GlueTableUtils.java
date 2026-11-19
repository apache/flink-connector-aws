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

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for working with Glue tables, including transforming Glue-specific metadata into
 * Flink-compatible objects.
 */
@Internal
public class GlueTableUtils {

    /** Logger for logging Glue table operations. */
    private static final Logger LOG = LoggerFactory.getLogger(GlueTableUtils.class);

    /** The Flink table option naming the connector. */
    private static final String CONNECTOR_OPTION = "connector";

    /** Marker that a connector location value is a URI (and therefore a valid Glue location). */
    private static final String URI_SCHEME_SEPARATOR = "://";

    /** Scheme of the synthetic location written for partitioned tables without a real one. */
    private static final String SYNTHETIC_LOCATION_SCHEME = "flink";

    /** Glue type converter for type conversions between Flink and Glue types. */
    private final GlueTypeConverter glueTypeConverter;

    /**
     * Constructor to initialize GlueTableUtils with a GlueTypeConverter.
     *
     * @param glueTypeConverter The GlueTypeConverter instance for type mapping.
     */
    public GlueTableUtils(GlueTypeConverter glueTypeConverter) {
        this.glueTypeConverter = glueTypeConverter;
    }

    /**
     * Builds a Glue StorageDescriptor from the given columns and location.
     *
     * @param glueColumns Columns to be included in the StorageDescriptor.
     * @param tableLocation Location of the Glue table; {@code null} to omit (non-filesystem
     *     connectors have no storage location).
     * @return A newly built StorageDescriptor object.
     */
    public StorageDescriptor buildStorageDescriptor(
            List<Column> glueColumns, String tableLocation) {
        StorageDescriptor.Builder builder = StorageDescriptor.builder().columns(glueColumns);
        if (tableLocation != null) {
            builder.location(tableLocation);
        }
        return builder.build();
    }

    /**
     * Extracts the storage location to record on the Glue table, if any. Glue (and the engines that
     * read the Glue Data Catalog, such as Athena) expect {@code StorageDescriptor.location} to be a
     * URI, so a location is only reported when the connector declares a location option (see {@link
     * ConnectorRegistry}) whose value carries a URI scheme, for example {@code s3://bucket/path}
     * for the filesystem connector or a {@code jdbc:...://} URL. Connector endpoints that are not
     * URIs (Kafka bootstrap servers, Kinesis stream ARNs, DynamoDB table names) are kept as table
     * options only.
     *
     * @param tableProperties Table properties containing the connector and location.
     * @return The location of the Glue table, or {@code null} when there is none.
     */
    public String extractTableLocation(Map<String, String> tableProperties) {
        String connectorType = tableProperties.get(CONNECTOR_OPTION);
        if (connectorType == null) {
            return null;
        }
        String locationKey = ConnectorRegistry.getLocationKey(connectorType);
        if (locationKey == null) {
            return null;
        }
        String location = tableProperties.get(locationKey);
        return location != null && location.contains(URI_SCHEME_SEPARATOR) ? location : null;
    }

    /**
     * Resolves the storage location to record on a Glue table: the connector's URI location when it
     * has one (see {@link #extractTableLocation}); otherwise, for partitioned tables only, a
     * synthetic {@code flink://database/table} URI. AWS Glue rejects {@code CreatePartition} on a
     * table without a location ("Cannot add partition to a table without location"), so a
     * partitioned Kinesis or Kafka table needs one even though its data has no filesystem path.
     * Unpartitioned tables of such connectors keep no location, so other engines are not shown a
     * path that does not exist.
     *
     * @param tableProperties Table options containing the connector and its location option.
     * @param glueDatabaseName The Glue storage name of the database.
     * @param glueTableName The Glue storage name of the table.
     * @param partitioned Whether the table declares partition keys.
     * @return The location to store, or {@code null} for none.
     */
    public String resolveTableLocation(
            Map<String, String> tableProperties,
            String glueDatabaseName,
            String glueTableName,
            boolean partitioned) {
        String location = extractTableLocation(tableProperties);
        if (location == null && partitioned) {
            return SYNTHETIC_LOCATION_SCHEME
                    + URI_SCHEME_SEPARATOR
                    + glueDatabaseName
                    + "/"
                    + glueTableName;
        }
        return location;
    }

    /**
     * Converts a Flink column to a Glue column. The column's data type is converted using the
     * GlueTypeConverter and the column comment, if any, is stored as the Glue column comment.
     *
     * @param flinkColumn The Flink column to be converted.
     * @return The corresponding Glue column.
     */
    public Column mapFlinkColumnToGlueColumn(org.apache.flink.table.catalog.Column flinkColumn) {
        String glueType = glueTypeConverter.toGlueDataType(flinkColumn.getDataType());

        // AWS Glue lowercases column names on CreateTable/UpdateTable (verified empirically:
        // a column created as "userId" is stored and returned as "userid"). To preserve the
        // declared case, store the lowercased name explicitly and stash the original name in
        // the column parameter, which the read path restores.
        String originalName = flinkColumn.getName();
        String glueName = originalName.toLowerCase();

        Column.Builder builder = Column.builder().name(glueName).type(glueType);
        flinkColumn.getComment().ifPresent(builder::comment);
        if (!glueName.equals(originalName)) {
            builder.parameters(
                    Collections.singletonMap(
                            GlueCatalogConstants.ORIGINAL_COLUMN_NAME, originalName));
        }
        return builder.build();
    }

    /**
     * Converts a Glue table into a Flink schema. Each Glue column is mapped to a Flink column using
     * the GlueTypeConverter. Partition columns (stored at the Glue table level, not in the storage
     * descriptor) are appended after the data columns so that declared partition keys are part of
     * the Flink schema, as required by {@code CatalogTable}. Computed and metadata columns,
     * watermarks, and the primary key - which Glue columns cannot represent - are restored from the
     * {@code flink.schema.*} table parameters written by {@link GlueFlinkSchemaProperties}.
     *
     * @param glueTable The Glue table from which the schema will be derived.
     * @return A Flink schema constructed from the Glue table's columns.
     */
    public Schema getSchemaFromGlueTable(Table glueTable) {
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Physical columns read from Glue, keyed by Flink-facing name, in the order Glue
        // returns them: storage-descriptor columns first, then partition keys. The declared
        // order (and any type/nullability overrides) is restored by name in restoreSchema, so
        // this list does not need to be in declared order.
        LinkedHashMap<String, DataType> glueColumns = new LinkedHashMap<>();
        Map<String, String> glueColumnComments = new HashMap<>();

        List<Column> columns =
                glueTable.storageDescriptor() != null
                        ? glueTable.storageDescriptor().columns()
                        : Collections.emptyList();
        for (Column column : columns) {
            String name = getColumnName(column);
            glueColumns.put(name, glueTypeConverter.toFlinkDataType(column.type()));
            if (column.comment() != null) {
                glueColumnComments.put(name, column.comment());
            }
        }

        // Partition columns live in Table.partitionKeys(), not in the storage descriptor.
        // Their declared case is restored from the table-level parameter (Glue rejects
        // column-level parameters on partition columns).
        if (glueTable.partitionKeys() != null && !glueTable.partitionKeys().isEmpty()) {
            List<String> partitionKeyNames = getPartitionKeyNames(glueTable);
            List<Column> partitionColumns = glueTable.partitionKeys();
            for (int i = 0; i < partitionColumns.size(); i++) {
                String name = partitionKeyNames.get(i);
                glueColumns.put(
                        name, glueTypeConverter.toFlinkDataType(partitionColumns.get(i).type()));
                if (partitionColumns.get(i).comment() != null) {
                    glueColumnComments.put(name, partitionColumns.get(i).comment());
                }
            }
        }

        // Restore the declared column order (by name), computed/metadata columns, NOT NULL
        // constraints, comments, watermarks, and the primary key.
        GlueFlinkSchemaProperties.restoreSchema(
                glueTable.parameters(), glueColumns, glueColumnComments, schemaBuilder);

        return schemaBuilder.build();
    }

    /**
     * Returns the Flink-facing partition key names of a Glue table, in declared order. The original
     * (case-preserved) names come from the table-level {@link
     * GlueCatalogConstants#ORIGINAL_PARTITION_KEYS} parameter when present (written by this catalog
     * because Glue rejects column-level parameters on partition columns), falling back to the
     * per-column resolution for tables written by other writers or older versions.
     *
     * @param glueTable The Glue table.
     * @return Ordered partition key names to expose to Flink; empty when not partitioned.
     */
    public static List<String> getPartitionKeyNames(Table glueTable) {
        if (glueTable.partitionKeys() == null || glueTable.partitionKeys().isEmpty()) {
            return Collections.emptyList();
        }
        List<Column> partitionColumns = glueTable.partitionKeys();
        if (glueTable.parameters() != null
                && glueTable
                        .parameters()
                        .containsKey(GlueCatalogConstants.ORIGINAL_PARTITION_KEYS)) {
            String[] originalNames =
                    glueTable
                            .parameters()
                            .get(GlueCatalogConstants.ORIGINAL_PARTITION_KEYS)
                            .split(",", -1);
            if (originalNames.length == partitionColumns.size()) {
                return java.util.Arrays.asList(originalNames);
            }
            LOG.warn(
                    "Ignoring malformed {} parameter on table {}: {} entries for {} partition keys",
                    GlueCatalogConstants.ORIGINAL_PARTITION_KEYS,
                    glueTable.name(),
                    originalNames.length,
                    partitionColumns.size());
        }
        List<String> names = new java.util.ArrayList<>(partitionColumns.size());
        for (Column partitionColumn : partitionColumns) {
            names.add(getColumnName(partitionColumn));
        }
        return names;
    }

    /**
     * Returns the Flink-facing name of a Glue column: the original (case-preserved) name from the
     * "originalName" column parameter when present, otherwise the Glue-stored name. Glue lowercases
     * column names on write, so this parameter is how the declared case survives the round-trip.
     *
     * @param column The Glue column.
     * @return The column name to expose to Flink.
     */
    public static String getColumnName(Column column) {
        if (column.parameters() != null) {
            if (column.parameters().containsKey(GlueCatalogConstants.ORIGINAL_COLUMN_NAME)) {
                return column.parameters().get(GlueCatalogConstants.ORIGINAL_COLUMN_NAME);
            }
            if (column.parameters().containsKey(GlueCatalogConstants.LEGACY_ORIGINAL_COLUMN_NAME)) {
                return column.parameters().get(GlueCatalogConstants.LEGACY_ORIGINAL_COLUMN_NAME);
            }
        }
        return column.name();
    }
}
