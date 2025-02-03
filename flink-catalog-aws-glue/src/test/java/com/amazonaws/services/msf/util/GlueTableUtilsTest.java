package com.amazonaws.services.msf.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;

class GlueTableUtilsTest {

    private GlueTypeConverter glueTypeConverter;
    private GlueTableUtils glueTableUtils;

    // Test data
    private static final String TEST_CONNECTOR_TYPE = "kinesis";
    private static final String TEST_TABLE_LOCATION = "arn://...";
    private static final String TEST_TABLE_NAME = "test_table";
    private static final String TEST_COLUMN_NAME = "test_column";

    @BeforeEach
    void setUp() {
        // Initialize GlueTypeConverter directly as it is already implemented
        glueTypeConverter = new GlueTypeConverter();
        glueTableUtils = new GlueTableUtils(glueTypeConverter);
    }

    @Test
    void testBuildStorageDescriptor() {
        // Prepare test data
        List<Column> glueColumns = Arrays.asList(
                Column.builder().name(TEST_COLUMN_NAME).type("string").build()
        );

        // Build the StorageDescriptor
        StorageDescriptor storageDescriptor = glueTableUtils.buildStorageDescriptor(
                new HashMap<>(), glueColumns, TEST_TABLE_LOCATION
        );

        // Assert that the StorageDescriptor is not null and contains the correct location
        assertNotNull(storageDescriptor, "StorageDescriptor should not be null");
        assertEquals(TEST_TABLE_LOCATION, storageDescriptor.location(), "Table location should match");
        assertEquals(1, storageDescriptor.columns().size(), "StorageDescriptor should have one column");
        assertEquals(TEST_COLUMN_NAME, storageDescriptor.columns().get(0).name(), "Column name should match");
    }

    @Test
    void testExtractTableLocationWithLocationKey() {
        // Prepare table properties with a connector type and location
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", TEST_CONNECTOR_TYPE);
        tableProperties.put("stream.arn", TEST_TABLE_LOCATION);  // Mimicking a location key for kinesis

        ObjectPath tablePath = new ObjectPath("test_database", TEST_TABLE_NAME);

        // Extract table location
        String location = glueTableUtils.extractTableLocation(tableProperties, tablePath);

        // Assert that the correct location is used
        assertEquals(TEST_TABLE_LOCATION, location, "Table location should match the location key");
    }

    @Test
    void testExtractTableLocationWithDefaultLocation() {
        // Prepare table properties without a location key
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", TEST_CONNECTOR_TYPE);  // No actual location key here

        ObjectPath tablePath = new ObjectPath("test_database", TEST_TABLE_NAME);

        // Extract table location
        String location = glueTableUtils.extractTableLocation(tableProperties, tablePath);

        // Assert that the default location is used
        String expectedLocation = tablePath.getDatabaseName() + "/tables/" + tablePath.getObjectName();
        assertEquals(expectedLocation, location, "Default location should be used");
    }

    @Test
    void testMapFlinkColumnToGlueColumn() {
        // Prepare a Flink column to convert
        org.apache.flink.table.catalog.Column flinkColumn = org.apache.flink.table.catalog.Column.physical(
                TEST_COLUMN_NAME, DataTypes.STRING()  // Fix: DataTypes.STRING() instead of DataType.STRING()
        );

        // Convert Flink column to Glue column
        Column glueColumn = glueTableUtils.mapFlinkColumnToGlueColumn(flinkColumn);

        // Assert that the Glue column is correctly mapped
        assertNotNull(glueColumn, "Converted Glue column should not be null");
        assertEquals(TEST_COLUMN_NAME.toLowerCase(), glueColumn.name(), "Column name should be lowercase");
        assertEquals("string", glueColumn.type(), "Column type should match the expected Glue type");
    }
    @Test
    void testGetSchemaFromGlueTable() {
        // Prepare a Glue table with columns
        List<Column> glueColumns = Arrays.asList(
                Column.builder().name(TEST_COLUMN_NAME).type("string").build(),
                Column.builder().name("another_column").type("int").build()
        );
        StorageDescriptor storageDescriptor = StorageDescriptor.builder().columns(glueColumns).build();
        Table glueTable = Table.builder().storageDescriptor(storageDescriptor).build();

        // Get the schema from the Glue table
        Schema schema = glueTableUtils.getSchemaFromGlueTable(glueTable);

        // Assert that the schema is correctly constructed
        assertNotNull(schema, "Schema should not be null");
        assertEquals(2, schema.getColumns().size(), "Schema should have two columns");
    }
}
