/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.operator;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Unit tests for {@link GluePartitionOperator}: the partition CRUD lifecycle against Glue, the
 * translation of Glue errors, and the pass-through of the exceptions the catalog layer maps itself.
 * Runs against the in-memory fake by default and against real AWS Glue when credentials are
 * supplied (see {@link GlueTestClientFactory}).
 */
@ExtendWith(RealGlueCleanupExtension.class)
class GluePartitionOperatorTest {

    private static final String CATALOG_NAME = "testcatalog";
    private static final String TABLE_NAME = "partitionedtable";

    private GlueClient glueClient;
    private GluePartitionOperator partitionOperator;
    private String databaseName;

    @BeforeEach
    void setUp() {
        glueClient = GlueTestClientFactory.createClient();
        partitionOperator = new GluePartitionOperator(glueClient, CATALOG_NAME);
        databaseName = GlueTestClientFactory.uniqueName("partdb");
        // Real Glue requires the database and a partitioned table to exist.
        glueClient.createDatabase(b -> b.databaseInput(db -> db.name(databaseName)));
        glueClient.createTable(
                b ->
                        b.databaseName(databaseName)
                                .tableInput(
                                        TableInput.builder()
                                                .name(TABLE_NAME)
                                                .tableType("EXTERNAL_TABLE")
                                                .partitionKeys(
                                                        Column.builder()
                                                                .name("region")
                                                                .type("string")
                                                                .build(),
                                                        Column.builder()
                                                                .name("dt")
                                                                .type("string")
                                                                .build())
                                                .storageDescriptor(
                                                        StorageDescriptor.builder()
                                                                .location(
                                                                        "s3://bucket/" + TABLE_NAME)
                                                                .columns(
                                                                        Column.builder()
                                                                                .name("id")
                                                                                .type("int")
                                                                                .build())
                                                                .build())
                                                .build()));
    }

    @AfterEach
    void tearDown() {
        glueClient.close();
    }

    @Test
    void testListPartitionsOfTableWithoutPartitionsIsEmpty() {
        assertThat(partitionOperator.listPartitions(databaseName, TABLE_NAME)).isEmpty();
    }

    @Test
    void testCreateGetListAndDropPartition() {
        List<String> euValues = Arrays.asList("eu", "2026-01-01");
        List<String> usValues = Arrays.asList("us", "2026-01-01");
        partitionOperator.createPartition(databaseName, TABLE_NAME, partitionInput(euValues));
        partitionOperator.createPartition(databaseName, TABLE_NAME, partitionInput(usValues));

        Partition eu = partitionOperator.getPartition(databaseName, TABLE_NAME, euValues);
        assertThat(eu).isNotNull();
        assertThat(eu.values()).containsExactlyElementsOf(euValues);
        assertThat(eu.storageDescriptor().location()).isEqualTo(location(euValues));
        assertThat(eu.parameters()).containsEntry("owner", "tests");

        assertThat(partitionOperator.listPartitions(databaseName, TABLE_NAME))
                .extracting(Partition::values)
                .containsExactlyInAnyOrder(euValues, usValues);

        partitionOperator.dropPartition(databaseName, TABLE_NAME, euValues);
        assertThat(partitionOperator.getPartition(databaseName, TABLE_NAME, euValues)).isNull();
        assertThat(partitionOperator.listPartitions(databaseName, TABLE_NAME))
                .extracting(Partition::values)
                .containsExactly(usValues);
    }

    @Test
    void testGetMissingPartitionReturnsNull() {
        assertThat(
                        partitionOperator.getPartition(
                                databaseName, TABLE_NAME, Arrays.asList("nowhere", "never")))
                .isNull();
    }

    @Test
    void testUpdatePartitionReplacesStorageDescriptorAndParameters() {
        List<String> values = Arrays.asList("eu", "2026-01-02");
        partitionOperator.createPartition(databaseName, TABLE_NAME, partitionInput(values));

        PartitionInput updated =
                PartitionInput.builder()
                        .values(values)
                        .storageDescriptor(
                                StorageDescriptor.builder()
                                        .location("s3://elsewhere/eu/2026-01-02")
                                        .build())
                        .parameters(Collections.singletonMap("owner", "updated"))
                        .build();
        partitionOperator.updatePartition(databaseName, TABLE_NAME, values, updated);

        Partition partition = partitionOperator.getPartition(databaseName, TABLE_NAME, values);
        assertThat(partition.storageDescriptor().location())
                .isEqualTo("s3://elsewhere/eu/2026-01-02");
        assertThat(partition.parameters()).containsEntry("owner", "updated");
    }

    /**
     * The catalog maps these two Glue exceptions to Flink's {@code PartitionAlreadyExistsException}
     * / {@code PartitionNotExistException}, so the operator must let them through untranslated.
     */
    @Test
    void testAlreadyExistsAndNotFoundPassThrough() {
        List<String> values = Arrays.asList("eu", "2026-01-03");
        partitionOperator.createPartition(databaseName, TABLE_NAME, partitionInput(values));

        assertThatThrownBy(
                        () ->
                                partitionOperator.createPartition(
                                        databaseName, TABLE_NAME, partitionInput(values)))
                .isInstanceOf(AlreadyExistsException.class);

        List<String> missing = Arrays.asList("eu", "1970-01-01");
        assertThatThrownBy(
                        () ->
                                partitionOperator.updatePartition(
                                        databaseName, TABLE_NAME, missing, partitionInput(missing)))
                .isInstanceOf(EntityNotFoundException.class);
        assertThatThrownBy(() -> partitionOperator.dropPartition(databaseName, TABLE_NAME, missing))
                .isInstanceOf(EntityNotFoundException.class);
    }

    @Test
    void testAccessDeniedIsTranslatedWithTarget() {
        fakeClient()
                .setNextException(AccessDeniedException.builder().message("not allowed").build());
        assertThatThrownBy(() -> partitionOperator.listPartitions(databaseName, TABLE_NAME))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("listing partitions of " + databaseName + "." + TABLE_NAME)
                .hasMessageContaining("access denied by AWS Glue")
                .hasCauseInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testInvalidInputIsTranslatedWithPartitionValues() {
        List<String> values = Arrays.asList("eu", "bad");
        fakeClient().setNextException(InvalidInputException.builder().message("bad value").build());
        assertThatThrownBy(
                        () ->
                                partitionOperator.createPartition(
                                        databaseName, TABLE_NAME, partitionInput(values)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("creating partition " + values)
                .hasMessageContaining("rejected the request as invalid")
                .hasMessageContaining("bad value")
                .hasCauseInstanceOf(InvalidInputException.class);
    }

    private static PartitionInput partitionInput(List<String> values) {
        return PartitionInput.builder()
                .values(values)
                .storageDescriptor(StorageDescriptor.builder().location(location(values)).build())
                .parameters(Collections.singletonMap("owner", "tests"))
                .build();
    }

    private static String location(List<String> values) {
        return "s3://bucket/" + TABLE_NAME + "/region=" + values.get(0) + "/dt=" + values.get(1);
    }

    private FakeGlueClient fakeClient() {
        assumeThat(glueClient)
                .as("Fault-injection tests require the in-memory FakeGlueClient")
                .isInstanceOf(FakeGlueClient.class);
        return (FakeGlueClient) glueClient;
    }
}
