package com.amazonaws.services.msf.util;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility class for working with Glue tables, including transforming Glue-specific metadata into Flink-compatible objects.
 */
public class GlueTableUtils {

    /** Logger for logging Glue table operations. */
    private static final Logger LOG = LoggerFactory.getLogger(GlueTableUtils.class);

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
     * Builds a Glue StorageDescriptor from the given table properties, columns, and location.
     *
     * @param tableProperties Table properties for the Glue table.
     * @param glueColumns Columns to be included in the StorageDescriptor.
     * @param tableLocation Location of the Glue table.
     * @return A newly built StorageDescriptor object.
     */
    public StorageDescriptor buildStorageDescriptor(Map<String, String> tableProperties, List<Column> glueColumns, String tableLocation) {
        // Log the action of building the storage descriptor.
        LOG.debug("Building StorageDescriptor for table location: {}", tableLocation);

        return StorageDescriptor.builder()
                .columns(glueColumns)
                .location(tableLocation)
                .build();
    }

    /**
     * Extracts the table location based on the table properties and the table path.
     * First, it checks for a location key from the connector registry. If no such key is found,
     * it uses a default path based on the table path.
     *
     * @param tableProperties Table properties containing the connector and location.
     * @param tablePath The Flink ObjectPath representing the table.
     * @return The location of the Glue table.
     */
    public String extractTableLocation(Map<String, String> tableProperties, ObjectPath tablePath) {
        String connectorType = tableProperties.get("connector");
        if (connectorType != null) {
            String locationKey = ConnectorRegistry.getLocationKey(connectorType);
            if (locationKey != null && tableProperties.containsKey(locationKey)) {
                String location = tableProperties.get(locationKey);
                LOG.debug("Using location key '{}' to extract table location: {}", locationKey, location);
                return location;
            }
        }

        String defaultLocation = tablePath.getDatabaseName() + "/tables/" + tablePath.getObjectName();
        LOG.debug("No location key found, using default location: {}", defaultLocation);
        return defaultLocation;
    }

    /**
     * Converts a Flink column to a Glue column.
     * The column's data type is converted using the GlueTypeConverter.
     *
     * @param flinkColumn The Flink column to be converted.
     * @return The corresponding Glue column.
     */
    public Column mapFlinkColumnToGlueColumn(org.apache.flink.table.catalog.Column flinkColumn) {
        String glueType = glueTypeConverter.toGlueType(flinkColumn.getDataType());
        // Log the conversion action.
        LOG.debug("Mapping Flink column '{}' to Glue column with type '{}'", flinkColumn.getName(), glueType);

        return Column.builder()
                .name(flinkColumn.getName().toLowerCase())
                .type(glueType)
                .parameters(Map.of("originalName", flinkColumn.getName()))
                .build();
    }

    /**
     * Converts a Glue table into a Flink schema.
     * Each Glue column is mapped to a Flink column using the GlueTypeConverter.
     *
     * @param glueTable The Glue table from which the schema will be derived.
     * @return A Flink schema constructed from the Glue table's columns.
     */
    public Schema getSchemaFromGlueTable(Table glueTable) {
        List<Column> columns = glueTable.storageDescriptor().columns();
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Log the conversion action.
        LOG.debug("Converting Glue table to Flink schema with {} columns", columns.size());

        for (Column column : columns) {
            String columnName = column.name();
            String originalName = columnName;

            if (column.parameters() != null && column.parameters().containsKey("originalName")) {
                originalName = column.parameters().get("originalName");
            }

            String columnType = column.type().toLowerCase();
            DataType flinkDataType = glueTypeConverter.toFlinkType(columnType);

            schemaBuilder.column(originalName, flinkDataType);
        }

        Schema schema = schemaBuilder.build();
        LOG.debug("Built Flink schema: {}", schema);
        return schema;
    }
}
