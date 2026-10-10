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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

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
 * Unit tests for the GlueTableUtils class. Tests the utility methods for working with AWS Glue
 * tables.
 */
class GlueTableUtilsTest {

    private GlueTypeConverter glueTypeConverter;
    private GlueTableUtils glueTableUtils;

    // Test data
    private static final String TEST_CONNECTOR_TYPE = "kinesis";
    private static final String TEST_TABLE_LOCATION = "s3://bucket/warehouse/test_table";
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
                glueTableUtils.buildStorageDescriptor(glueColumns, TEST_TABLE_LOCATION);

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
    void testBuildStorageDescriptorWithoutLocation() {
        List<Column> glueColumns =
                Arrays.asList(Column.builder().name(TEST_COLUMN_NAME).type("string").build());

        StorageDescriptor storageDescriptor =
                glueTableUtils.buildStorageDescriptor(glueColumns, null);

        Assertions.assertNull(
                storageDescriptor.location(),
                "No location must be recorded for connectors without a storage location");
        Assertions.assertEquals(1, storageDescriptor.columns().size());
    }

    @Test
    void testExtractTableLocationWithUriLocationKey() {
        // The filesystem connector's path is a URI: it is a valid Glue storage location.
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", "filesystem");
        tableProperties.put("path", "s3://bucket/warehouse/orders");

        Assertions.assertEquals(
                "s3://bucket/warehouse/orders",
                glueTableUtils.extractTableLocation(tableProperties),
                "A URI location option must be used as the Glue table location");
    }

    @Test
    void testExtractTableLocationIgnoresNonUriLocationKey() {
        // A Kinesis stream ARN is not a URI; Glue (and Athena) expect StorageDescriptor.location
        // to be one, so it must not be recorded as the table location.
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", TEST_CONNECTOR_TYPE);
        tableProperties.put("stream.arn", "arn:aws:kinesis:us-east-1:123456789012:stream/orders");

        Assertions.assertNull(
                glueTableUtils.extractTableLocation(tableProperties),
                "Non-URI connector endpoints must not become the Glue table location");
    }

    @Test
    void testExtractTableLocationWithoutLocationKey() {
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", TEST_CONNECTOR_TYPE); // No location key present

        Assertions.assertNull(
                glueTableUtils.extractTableLocation(tableProperties),
                "No location must be fabricated when the connector declares none");
    }

    @Test
    void testResolveTableLocationSynthesizesOneOnlyForPartitionedTables() {
        // Glue rejects CreatePartition on a table without a location, so a partitioned table of
        // a connector without a URI location gets a synthetic one; an unpartitioned one does not.
        Map<String, String> kinesis = new HashMap<>();
        kinesis.put("connector", TEST_CONNECTOR_TYPE);
        kinesis.put("stream.arn", "arn:aws:kinesis:us-east-1:123456789012:stream/orders");

        Assertions.assertNull(glueTableUtils.resolveTableLocation(kinesis, "db", "orders", false));
        Assertions.assertEquals(
                "flink://db/orders",
                glueTableUtils.resolveTableLocation(kinesis, "db", "orders", true));

        // A real connector URI always wins.
        Map<String, String> filesystem = new HashMap<>();
        filesystem.put("connector", "filesystem");
        filesystem.put("path", "s3://bucket/warehouse/orders");
        Assertions.assertEquals(
                "s3://bucket/warehouse/orders",
                glueTableUtils.resolveTableLocation(filesystem, "db", "orders", true));
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
                TEST_COLUMN_NAME,
                glueColumn.name(),
                "Column name should be preserved as declared (no lowercasing)");
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

        // 3. Verify Glue column names are stored lowercase (Glue lowercases on write, so we
        // store lowercase deterministically) with the original case in the parameters.
        Assertions.assertEquals(
                "uppercasecolumn",
                glueUpperCase.name(),
                "Glue column name should be stored lowercase");
        Assertions.assertEquals(
                "mixedcasecolumn",
                glueMixedCase.name(),
                "Glue column name should be stored lowercase");
        Assertions.assertEquals(
                "lowercase_column",
                glueLowerCase.name(),
                "Glue column name should be stored lowercase");

        // 4. Verify the originalName parameter carries the declared case (only when needed)
        Assertions.assertEquals(
                "UpperCaseColumn",
                glueUpperCase.parameters().get(GlueCatalogConstants.ORIGINAL_COLUMN_NAME),
                "originalName parameter should preserve the declared case");
        Assertions.assertEquals(
                "mixedCaseColumn",
                glueMixedCase.parameters().get(GlueCatalogConstants.ORIGINAL_COLUMN_NAME),
                "originalName parameter should preserve the declared case");
        Assertions.assertFalse(
                glueLowerCase.parameters() != null
                        && glueLowerCase
                                .parameters()
                                .containsKey(GlueCatalogConstants.ORIGINAL_COLUMN_NAME),
                "already-lowercase columns need no originalName parameter");

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

        // 3. Verify Glue columns are stored lowercase (real Glue lowercases on write) with the
        // declared case preserved via the originalName parameter.
        for (int i = 0; i < flinkColumns.size(); i++) {
            String originalName = flinkColumns.get(i).getName();
            Column glueColumn = glueColumns.get(i);

            Assertions.assertEquals(
                    originalName.toLowerCase(),
                    glueColumn.name(),
                    "Glue column name should be stored lowercase");
            if (!originalName.equals(originalName.toLowerCase())) {
                Assertions.assertEquals(
                        originalName,
                        glueColumn.parameters().get(GlueCatalogConstants.ORIGINAL_COLUMN_NAME),
                        "originalName parameter should preserve the declared case");
            }
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

        Assertions.assertEquals(
                "ID", resultColumnNames.get(0), "First column should maintain original case");
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

    @Test
    void testGetSchemaFromGlueTableHonorsLegacyOriginalNameParameter() {
        // Tables written by older catalog versions carry lowercased names plus an
        // "originalName" column parameter; reads must still surface the declared name.
        Column legacyColumn =
                Column.builder()
                        .name("username")
                        .type("string")
                        .parameters(java.util.Collections.singletonMap("originalName", "UserName"))
                        .build();
        StorageDescriptor storageDescriptor =
                StorageDescriptor.builder().columns(Arrays.asList(legacyColumn)).build();
        Table glueTable = Table.builder().storageDescriptor(storageDescriptor).build();

        Schema schema = glueTableUtils.getSchemaFromGlueTable(glueTable);

        Assertions.assertEquals(1, schema.getColumns().size(), "Schema should have one column");
        Assertions.assertEquals(
                "UserName",
                schema.getColumns().get(0).getName(),
                "Legacy originalName parameter should still be honored on read");
    }

    @Test
    void testGetSchemaFromGlueTableIncludesPartitionColumns() {
        // Partition columns live in Table.partitionKeys(), not in the storage descriptor;
        // the derived Flink schema must include them for CatalogTable partition keys to resolve.
        Column dataColumn = Column.builder().name("id").type("int").build();
        Column partitionColumn = Column.builder().name("region").type("string").build();
        StorageDescriptor storageDescriptor =
                StorageDescriptor.builder().columns(Arrays.asList(dataColumn)).build();
        Table glueTable =
                Table.builder()
                        .storageDescriptor(storageDescriptor)
                        .partitionKeys(Arrays.asList(partitionColumn))
                        .build();

        Schema schema = glueTableUtils.getSchemaFromGlueTable(glueTable);

        List<String> columnNames =
                schema.getColumns().stream().map(col -> col.getName()).collect(Collectors.toList());
        Assertions.assertEquals(
                Arrays.asList("id", "region"),
                columnNames,
                "Schema should contain data columns followed by partition columns");
    }
}
