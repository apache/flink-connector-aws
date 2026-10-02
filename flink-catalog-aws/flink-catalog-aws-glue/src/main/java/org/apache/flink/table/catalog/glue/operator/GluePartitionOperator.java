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

package org.apache.flink.table.catalog.glue.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles partition operations for the Glue catalog: listing, retrieving, creating, updating and
 * deleting partitions in AWS Glue, with pagination handled internally.
 *
 * <p>Glue's {@code AlreadyExistsException} (create) and {@code EntityNotFoundException} (get,
 * update, drop) are propagated unchanged so the catalog can apply ignore-if-exists /
 * ignore-if-not-exists semantics; every other Glue error is translated to a {@link
 * CatalogException} whose message names the partition and the underlying cause.
 */
@Internal
public class GluePartitionOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GluePartitionOperator.class);

    /**
     * Constructor for GluePartitionOperator.
     *
     * @param glueClient The Glue client to use for partition operations.
     * @param catalogName The name of the catalog.
     */
    public GluePartitionOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Lists all partitions of a table, following pagination via the SDK paginator.
     *
     * @param databaseName The Glue storage name of the database.
     * @param tableName The Glue storage name of the table.
     * @return All partitions of the table.
     * @throws EntityNotFoundException if the table does not exist.
     * @throws CatalogException if any other error occurs while listing partitions.
     */
    public List<Partition> listPartitions(String databaseName, String tableName) {
        try {
            List<Partition> partitions = new ArrayList<>();
            for (GetPartitionsResponse page :
                    glueClient.getPartitionsPaginator(
                            GetPartitionsRequest.builder()
                                    .databaseName(databaseName)
                                    .tableName(tableName)
                                    .build())) {
                if (page.partitions() != null) {
                    partitions.addAll(page.partitions());
                }
            }
            return partitions;
        } catch (EntityNotFoundException e) {
            throw e;
        } catch (GlueException e) {
            throw translate("listing partitions of", databaseName, tableName, null, e);
        }
    }

    /**
     * Gets a single partition by its ordered partition values.
     *
     * @param databaseName The Glue storage name of the database.
     * @param tableName The Glue storage name of the table.
     * @param partitionValues The partition values, ordered by the table's partition keys.
     * @return The partition, or {@code null} if it does not exist.
     * @throws CatalogException if any other error occurs while getting the partition.
     */
    public Partition getPartition(
            String databaseName, String tableName, List<String> partitionValues) {
        try {
            GetPartitionRequest request =
                    GetPartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValues(partitionValues)
                            .build();
            return glueClient.getPartition(request).partition();
        } catch (EntityNotFoundException e) {
            LOG.debug("Partition {} of {}.{} not found", partitionValues, databaseName, tableName);
            return null;
        } catch (GlueException e) {
            throw translate("getting partition", databaseName, tableName, partitionValues, e);
        }
    }

    /**
     * Creates a partition.
     *
     * @param databaseName The Glue storage name of the database.
     * @param tableName The Glue storage name of the table.
     * @param partitionInput The Glue partition input.
     * @throws software.amazon.awssdk.services.glue.model.AlreadyExistsException if the partition
     *     already exists.
     * @throws EntityNotFoundException if the table does not exist.
     * @throws CatalogException if any other error occurs while creating the partition.
     */
    public void createPartition(
            String databaseName, String tableName, PartitionInput partitionInput) {
        try {
            CreatePartitionRequest request =
                    CreatePartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionInput(partitionInput)
                            .build();
            glueClient.createPartition(request);
            LOG.info(
                    "Created partition {} in {}.{}",
                    partitionInput.values(),
                    databaseName,
                    tableName);
        } catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException
                | EntityNotFoundException e) {
            throw e;
        } catch (GlueException e) {
            throw translate(
                    "creating partition", databaseName, tableName, partitionInput.values(), e);
        }
    }

    /**
     * Updates an existing partition.
     *
     * @param databaseName The Glue storage name of the database.
     * @param tableName The Glue storage name of the table.
     * @param partitionValues The current partition values identifying the partition.
     * @param partitionInput The new Glue partition input.
     * @throws EntityNotFoundException if the partition (or table) does not exist.
     * @throws CatalogException if any other error occurs while updating the partition.
     */
    public void updatePartition(
            String databaseName,
            String tableName,
            List<String> partitionValues,
            PartitionInput partitionInput) {
        try {
            UpdatePartitionRequest request =
                    UpdatePartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValueList(partitionValues)
                            .partitionInput(partitionInput)
                            .build();
            glueClient.updatePartition(request);
            LOG.info("Updated partition {} in {}.{}", partitionValues, databaseName, tableName);
        } catch (EntityNotFoundException e) {
            throw e;
        } catch (GlueException e) {
            throw translate("updating partition", databaseName, tableName, partitionValues, e);
        }
    }

    /**
     * Deletes a partition.
     *
     * @param databaseName The Glue storage name of the database.
     * @param tableName The Glue storage name of the table.
     * @param partitionValues The partition values identifying the partition.
     * @throws EntityNotFoundException if the partition (or table) does not exist.
     * @throws CatalogException if any other error occurs while deleting the partition.
     */
    public void dropPartition(String databaseName, String tableName, List<String> partitionValues) {
        try {
            DeletePartitionRequest request =
                    DeletePartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValues(partitionValues)
                            .build();
            glueClient.deletePartition(request);
            LOG.info("Dropped partition {} from {}.{}", partitionValues, databaseName, tableName);
        } catch (EntityNotFoundException e) {
            throw e;
        } catch (GlueException e) {
            throw translate("dropping partition", databaseName, tableName, partitionValues, e);
        }
    }

    /**
     * Translates a Glue error into a {@link CatalogException} whose message tells the user what
     * failed and why, distinguishing the errors they can act on (permissions, invalid input) from
     * service failures.
     */
    private static CatalogException translate(
            String action,
            String databaseName,
            String tableName,
            List<String> partitionValues,
            GlueException e) {
        String target =
                partitionValues == null
                        ? databaseName + "." + tableName
                        : partitionValues + " of " + databaseName + "." + tableName;
        String reason;
        if (e instanceof AccessDeniedException) {
            reason = "access denied by AWS Glue (check the IAM permissions of the caller)";
        } else if (e instanceof InvalidInputException) {
            reason = "AWS Glue rejected the request as invalid";
        } else {
            reason = "AWS Glue returned an error";
        }
        LOG.error("Error {} {}: {} - {}", action, target, reason, e.getMessage());
        return new CatalogException(
                String.format("Error %s %s: %s: %s", action, target, reason, e.getMessage()), e);
    }
}
