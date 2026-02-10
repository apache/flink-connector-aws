package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit tests for the GlueTableUtils class.
 * Tests the utility methods for working with AWS Glue tables.
 */
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
        List<Column> glueColumns =
                Arrays.asList(Column.builder().name(TEST_COLUMN_NAME).type("string").build());

        // Build the StorageDescriptor
        StorageDescriptor storageDescriptor =
                glueTableUtils.buildStorageDescriptor(
                        new HashMap<>(), glueColumns, TEST_TABLE_LOCATION);

        // Assert that the StorageDescriptor is not null and contains the correct location
        Assertions.assertNotNull(storageDescriptor, "StorageDescriptor should not be null");
        Assertions.assertEquals(
                TEST_TABLE_LOCATION, storageDescriptor.location(), "Table location should match");
        Assertions.assertEquals(
                1, storageDescriptor.columns().size(), "StorageDescriptor should have one column");
        Assertions.assertEquals(
                TEST_COLUMN_NAME,
                storageDescriptor.columns().get(0).name(),
                "Column name should match");
    }

    @Test
    void testExtractTableLocationWithLocationKey() {
        // Prepare table properties with a connector type and location
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", TEST_CONNECTOR_TYPE);
        tableProperties.put(
                "stream.arn", TEST_TABLE_LOCATION); // Mimicking a location key for kinesis

        ObjectPath tablePath = new ObjectPath("test_database", TEST_TABLE_NAME);

        // Extract table location
        String location = glueTableUtils.extractTableLocation(tableProperties, tablePath);

        // Assert that the correct location is used
        Assertions.assertEquals(TEST_TABLE_LOCATION, location, "Table location should match the location key");
    }

    @Test
    void testExtractTableLocationWithDefaultLocation() {
        // Prepare table properties without a location key
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", TEST_CONNECTOR_TYPE); // No actual location key here

        ObjectPath tablePath = new ObjectPath("test_database", TEST_TABLE_NAME);

        // Extract table location
        String location = glueTableUtils.extractTableLocation(tableProperties, tablePath);

        // Assert that the default location is used
        String expectedLocation =
                tablePath.getDatabaseName() + "/tables/" + tablePath.getObjectName();
        Assertions.assertEquals(expectedLocation, location, "Default location should be used");
    }

    @Test
    void testMapFlinkColumnToGlueColumn() {
        // Prepare a Flink column to convert
        org.apache.flink.table.catalog.Column flinkColumn =
                org.apache.flink.table.catalog.Column.physical(
                        TEST_COLUMN_NAME,
                        DataTypes.STRING() // Fix: DataTypes.STRING() instead of DataType.STRING()
                        );

        // Convert Flink column to Glue column
        Column glueColumn = glueTableUtils.mapFlinkColumnToGlueColumn(flinkColumn);

        // Assert that the Glue column is correctly mapped
        Assertions.assertNotNull(glueColumn, "Converted Glue column should not be null");
        Assertions.assertEquals(
                TEST_COLUMN_NAME.toLowerCase(),
                glueColumn.name(),
                "Column name should be lowercase");
        Assertions.assertEquals(
                "string", glueColumn.type(), "Column type should match the expected Glue type");
    }

    @Test
    void testGetSchemaFromGlueTable() {
        // Prepare a Glue table with columns
        List<Column> glueColumns =
                Arrays.asList(
                        Column.builder().name(TEST_COLUMN_NAME).type("string").build(),
                        Column.builder().name("another_column").type("int").build());
        StorageDescriptor storageDescriptor =
                StorageDescriptor.builder().columns(glueColumns).build();
        Table glueTable = Table.builder().storageDescriptor(storageDescriptor).build();

        // Get the schema from the Glue table
        Schema schema = glueTableUtils.getSchemaFromGlueTable(glueTable);

        // Assert that the schema is correctly constructed
        Assertions.assertNotNull(schema, "Schema should not be null");
        Assertions.assertEquals(2, schema.getColumns().size(), "Schema should have two columns");
    }

    @Test
    void testColumnNameCaseSensitivity() {
        // 1. Define Flink columns with mixed case names
        org.apache.flink.table.catalog.Column upperCaseColumn =
                org.apache.flink.table.catalog.Column.physical(
                        "UpperCaseColumn", DataTypes.STRING());

        org.apache.flink.table.catalog.Column mixedCaseColumn =
                org.apache.flink.table.catalog.Column.physical("mixedCaseColumn", DataTypes.INT());

        org.apache.flink.table.catalog.Column lowerCaseColumn =
                org.apache.flink.table.catalog.Column.physical(
                        "lowercase_column", DataTypes.BOOLEAN());

        // 2. Convert Flink columns to Glue columns
        Column glueUpperCase = glueTableUtils.mapFlinkColumnToGlueColumn(upperCaseColumn);
        Column glueMixedCase = glueTableUtils.mapFlinkColumnToGlueColumn(mixedCaseColumn);
        Column glueLowerCase = glueTableUtils.mapFlinkColumnToGlueColumn(lowerCaseColumn);

        // 3. Verify that Glue column names are lowercase
        Assertions.assertEquals(
                "uppercasecolumn", glueUpperCase.name(), "Glue column name should be lowercase");
        Assertions.assertEquals(
                "mixedcasecolumn", glueMixedCase.name(), "Glue column name should be lowercase");
        Assertions.assertEquals(
                "lowercase_column", glueLowerCase.name(), "Glue column name should be lowercase");

        // 4. Verify that originalName parameter preserves case
        Assertions.assertEquals(
                "UpperCaseColumn",
                glueUpperCase.parameters().get("originalName"),
                "originalName parameter should preserve original case");
        Assertions.assertEquals(
                "mixedCaseColumn",
                glueMixedCase.parameters().get("originalName"),
                "originalName parameter should preserve original case");
        Assertions.assertEquals(
                "lowercase_column",
                glueLowerCase.parameters().get("originalName"),
                "originalName parameter should preserve original case");

        // 5. Create a Glue table with these columns
        List<Column> glueColumns = Arrays.asList(glueUpperCase, glueMixedCase, glueLowerCase);
        StorageDescriptor storageDescriptor =
                StorageDescriptor.builder().columns(glueColumns).build();
        Table glueTable = Table.builder().storageDescriptor(storageDescriptor).build();

        // 6. Convert back to Flink schema
        Schema schema = glueTableUtils.getSchemaFromGlueTable(glueTable);

        // 7. Verify that original case is preserved in schema
        List<String> columnNames =
                schema.getColumns().stream().map(col -> col.getName()).collect(Collectors.toList());

        Assertions.assertEquals(3, columnNames.size(), "Schema should have three columns");
        Assertions.assertTrue(
                columnNames.contains("UpperCaseColumn"),
                "Schema should contain the uppercase column with original case");
        Assertions.assertTrue(
                columnNames.contains("mixedCaseColumn"),
                "Schema should contain the mixed case column with original case");
        Assertions.assertTrue(
                columnNames.contains("lowercase_column"),
                "Schema should contain the lowercase column with original case");
    }

    @Test
    void testEndToEndColumnNameCasePreservation() {
        // This test simulates a more complete lifecycle with table creation and JSON parsing

        // 1. Create Flink columns with mixed case (representing original source)
        List<org.apache.flink.table.catalog.Column> flinkColumns =
                Arrays.asList(
                        org.apache.flink.table.catalog.Column.physical("ID", DataTypes.INT()),
                        org.apache.flink.table.catalog.Column.physical(
                                "UserName", DataTypes.STRING()),
                        org.apache.flink.table.catalog.Column.physical(
                                "timestamp", DataTypes.TIMESTAMP()),
                        org.apache.flink.table.catalog.Column.physical(
                                "DATA_VALUE", DataTypes.STRING()));

        // 2. Convert to Glue columns (simulating what happens in table creation)
        List<Column> glueColumns =
                flinkColumns.stream()
                        .map(glueTableUtils::mapFlinkColumnToGlueColumn)
                        .collect(Collectors.toList());

        // 3. Verify Glue columns are lowercase but have original names in parameters
        for (int i = 0; i < flinkColumns.size(); i++) {
            String originalName = flinkColumns.get(i).getName();
            String glueName = glueColumns.get(i).name();

            Assertions.assertEquals(
                    originalName.toLowerCase(),
                    glueName,
                    "Glue column name should be lowercase of original");
            Assertions.assertEquals(
                    originalName,
                    glueColumns.get(i).parameters().get("originalName"),
                    "Original name should be preserved in column parameters");
        }

        // 4. Create a Glue table with these columns (simulating storage in Glue)
        StorageDescriptor storageDescriptor =
                StorageDescriptor.builder().columns(glueColumns).build();
        Table glueTable = Table.builder().storageDescriptor(storageDescriptor).build();

        // 5. Convert back to Flink schema (simulating table retrieval for queries)
        Schema schema = glueTableUtils.getSchemaFromGlueTable(glueTable);

        // 6. Verify original case is preserved in the resulting schema
        List<String> resultColumnNames =
                schema.getColumns().stream().map(col -> col.getName()).collect(Collectors.toList());

        for (org.apache.flink.table.catalog.Column originalColumn : flinkColumns) {
            String originalName = originalColumn.getName();
            Assertions.assertTrue(
                    resultColumnNames.contains(originalName),
                    "Result schema should contain original column name with case preserved: "
                            + originalName);
        }

        // 7. Verify that a JSON string matching the original schema can be parsed correctly
        // This is a simulation of the real-world scenario where properly cased column names
        // are needed for JSON parsing
        String jsonExample =
                "{\"ID\":1,\"UserName\":\"test\",\"timestamp\":\"2023-01-01 12:00:00\",\"DATA_VALUE\":\"sample\"}";

        // We don't actually parse the JSON here since that would require external dependencies,
        // but this illustrates the scenario where correct case is important

        Assertions.assertEquals("ID", resultColumnNames.get(0), "First column should maintain original case");
        Assertions.assertEquals(
                "UserName",
                resultColumnNames.get(1),
                "Second column should maintain original case");
        Assertions.assertEquals(
                "timestamp",
                resultColumnNames.get(2),
                "Third column should maintain original case");
        Assertions.assertEquals(
                "DATA_VALUE",
                resultColumnNames.get(3),
                "Fourth column should maintain original case");
    }
}
