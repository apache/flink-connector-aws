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
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueUtils;
import org.apache.flink.table.expressions.Expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionResponse;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Utilities for Glue catalog Partition related operations. */
@Internal
public class GluePartitionOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GluePartitionOperator.class);

    public GluePartitionOperator(String catalogName, GlueClient glueClient, String glueCatalogId) {
        super(catalogName, glueClient, glueCatalogId);
    }

    /**
     * Create partition in glue data catalog service.
     *
     * @param glueTable glue table
     * @param partitionSpec partition spec
     * @param catalogPartition partition to add.
     * @throws CatalogException when partition is unable to be created.
     * @throws PartitionSpecInvalidException when partition specification is invalid.
     */
    public void createGluePartition(
            final Table glueTable,
            final CatalogPartitionSpec partitionSpec,
            final CatalogPartition catalogPartition)
            throws CatalogException, PartitionSpecInvalidException {
        Map<String, String> catalogPartitionProperties =
                new HashMap<>(catalogPartition.getProperties());
        String comment = catalogPartition.getComment();
        Map<String, String> partitionSpecProperties =
                new HashMap<>(partitionSpec.getPartitionSpec());
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Partition Keys retrieved from glue table: {}",
                    glueTable.partitionKeys().stream()
                            .map(Column::name)
                            .collect(Collectors.toList()));
        }
        List<String> partitionColumns = GlueUtils.getColumnNames(glueTable.partitionKeys());
        List<String> partitionValues =
                getOrderedFullPartitionValues(
                        partitionSpec,
                        partitionColumns,
                        new ObjectPath(glueTable.databaseName(), glueTable.name()));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Partition Values are: {}", String.join(", ", partitionValues));
        }
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (isNullOrWhitespaceOnly(partitionValues.get(i))) {
                throw new PartitionSpecInvalidException(
                        catalogName,
                        partitionColumns,
                        new ObjectPath(glueTable.databaseName(), glueTable.name()),
                        partitionSpec);
            }
        }
        StorageDescriptor.Builder storageDescriptor = glueTable.storageDescriptor().toBuilder();
        storageDescriptor.parameters(partitionSpecProperties);
        catalogPartitionProperties.put(GlueCatalogConstants.COMMENT, comment);
        PartitionInput.Builder partitionInput =
                PartitionInput.builder()
                        .parameters(catalogPartitionProperties)
                        .lastAccessTime(Instant.now())
                        .storageDescriptor(storageDescriptor.build())
                        .values(partitionValues);
        CreatePartitionRequest createPartitionRequest =
                CreatePartitionRequest.builder()
                        .partitionInput(partitionInput.build())
                        .catalogId(getGlueCatalogId())
                        .databaseName(glueTable.databaseName())
                        .tableName(glueTable.name())
                        .build();
        try {
            CreatePartitionResponse response = glueClient.createPartition(createPartitionRequest);
            GlueUtils.validateGlueResponse(response);
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Alter Partition in glue data catalog service.
     *
     * @param tablePath contains database name and table name.
     * @param partitionSpec Existing partition information.
     * @param newPartition Partition information with new changes.
     * @throws CatalogException Exception in failure.
     */
    public void alterGluePartition(
            final ObjectPath tablePath,
            final Table glueTable,
            final CatalogPartitionSpec partitionSpec,
            final CatalogPartition newPartition)
            throws CatalogException, PartitionSpecInvalidException {
        Map<String, String> partitionSpecProperties =
                new HashMap<>(partitionSpec.getPartitionSpec());
        Map<String, String> newPartitionProperties = new HashMap<>(newPartition.getProperties());
        String comment = newPartition.getComment();
        List<String> partitionColumns = GlueUtils.getColumnNames(glueTable.partitionKeys());
        List<String> partitionValues =
                getOrderedFullPartitionValues(
                        partitionSpec,
                        partitionColumns,
                        new ObjectPath(glueTable.databaseName(), glueTable.name()));
        StorageDescriptor.Builder storageDescriptor = glueTable.storageDescriptor().toBuilder();
        storageDescriptor.parameters(partitionSpecProperties);
        newPartitionProperties.put(GlueCatalogConstants.COMMENT, comment);
        PartitionInput.Builder partitionInput =
                PartitionInput.builder()
                        .lastAccessTime(Instant.now())
                        .parameters(newPartitionProperties)
                        .storageDescriptor(storageDescriptor.build())
                        .values(partitionValues);
        UpdatePartitionRequest.Builder updatePartitionRequest =
                UpdatePartitionRequest.builder()
                        .partitionInput(partitionInput.build())
                        .databaseName(tablePath.getDatabaseName())
                        .catalogId(getGlueCatalogId())
                        .tableName(tablePath.getObjectName())
                        .partitionValueList(partitionValues);
        UpdatePartitionResponse response =
                glueClient.updatePartition(updatePartitionRequest.build());
        GlueUtils.validateGlueResponse(response);
    }

    /**
     * Drop partition from Glue data catalog service.
     *
     * @param tablePath fully qualified table path
     * @param partitionSpec partition spec details
     * @throws CatalogException in case of unknown errors
     */
    public void dropGluePartition(
            final ObjectPath tablePath,
            final CatalogPartitionSpec partitionSpec,
            final Table glueTable)
            throws CatalogException {
        try {
            List<String> partitionColumns = GlueUtils.getColumnNames(glueTable.partitionKeys());
            DeletePartitionRequest deletePartitionRequest =
                    DeletePartitionRequest.builder()
                            .catalogId(getGlueCatalogId())
                            .databaseName(tablePath.getDatabaseName())
                            .tableName(tablePath.getObjectName())
                            .partitionValues(
                                    getOrderedFullPartitionValues(
                                            partitionSpec, partitionColumns, tablePath))
                            .build();
            DeletePartitionResponse response = glueClient.deletePartition(deletePartitionRequest);
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            GlueUtils.validateGlueResponse(response);
        } catch (PartitionSpecInvalidException e) {
            throw new CatalogException("Invalid Partition Spec", e);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Retrieve partition from glue data catalog service.
     *
     * @param glueTable Instance of {@link Table} from glue data Catalog.
     * @param partitionSpec instance of {@link CatalogPartitionSpec} containing details of partition
     * @return Instance of {@link Partition} matching the given partitionSpec.
     * @throws PartitionNotExistException when partition doesn't exist in Glue data catalog.
     */
    public Partition getGluePartition(
            final Table glueTable, final CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        ObjectPath tablePath = new ObjectPath(glueTable.databaseName(), glueTable.name());
        try {
            List<String> partitionColumns = GlueUtils.getColumnNames(glueTable.partitionKeys());
            List<String> partitionValues =
                    getOrderedFullPartitionValues(partitionSpec, partitionColumns, tablePath);
            LOG.info("Partition values are: {}", String.join(", ", partitionValues));
            GetPartitionRequest request =
                    GetPartitionRequest.builder()
                            .catalogId(getGlueCatalogId())
                            .databaseName(glueTable.databaseName())
                            .tableName(glueTable.name())
                            .partitionValues(partitionValues)
                            .build();
            GetPartitionResponse response = glueClient.getPartition(request);
            GlueUtils.validateGlueResponse(response);
            Partition partition = response.partition();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "(catalogPartition properties) Partition Parameters: {}",
                        partition.parameters().entrySet().stream()
                                .map(e -> e.getKey() + " - " + e.getValue())
                                .collect(Collectors.joining(", ")));
                LOG.debug(
                        "(PartitionSpec properties) Partition Parameters: {}",
                        partition.storageDescriptor().parameters().entrySet().stream()
                                .map(e -> e.getKey() + " - " + e.getValue())
                                .collect(Collectors.joining(", ")));
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            if (partition.hasValues()) {
                return partition;
            }
        } catch (EntityNotFoundException e) {
            throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
        } catch (PartitionSpecInvalidException e) {
            throw new CatalogException("PartitionSpec Invalid ", e);
        }
        return null;
    }

    /**
     * check Partition exists in glue data catalog.
     *
     * @param tablePath Fully Qualified tablePath.
     * @param partitionSpec Instance of {@link CatalogPartitionSpec}.
     * @return weather partition exists ?
     * @throws CatalogException in case of unknown errors.
     */
    public boolean gluePartitionExists(
            final ObjectPath tablePath,
            final Table glueTable,
            final CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        try {
            List<String> partitionColumns = GlueUtils.getColumnNames(glueTable.partitionKeys());
            List<String> partitionValues =
                    getOrderedFullPartitionValues(partitionSpec, partitionColumns, tablePath);
            GetPartitionRequest request =
                    GetPartitionRequest.builder()
                            .catalogId(getGlueCatalogId())
                            .databaseName(tablePath.getDatabaseName())
                            .tableName(tablePath.getObjectName())
                            .partitionValues(partitionValues)
                            .build();
            GetPartitionResponse response = glueClient.getPartition(request);
            GlueUtils.validateGlueResponse(response);
            return response.partition()
                    .storageDescriptor()
                    .parameters()
                    .keySet()
                    .containsAll(partitionSpec.getPartitionSpec().keySet());
        } catch (EntityNotFoundException e) {
            LOG.warn(String.format("%s is not found", partitionSpec.getPartitionSpec()));
        } catch (GlueException e) {
            throw new CatalogException(catalogName, e);
        } catch (PartitionSpecInvalidException e) {
            throw new CatalogException("PartitionSpec Invalid ", e);
        }
        return false;
    }

    /**
     * Get List of CatalogPartitionSpec from glue data catalog associated with Table.
     *
     * @param tablePath fully qualified table path.
     * @return List of PartitionSpec
     */
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {

        GetPartitionsRequest.Builder getPartitionRequest =
                GetPartitionsRequest.builder()
                        .catalogId(getGlueCatalogId())
                        .databaseName(tablePath.getDatabaseName())
                        .tableName(tablePath.getObjectName());
        try {
            GetPartitionsResponse response = glueClient.getPartitions(getPartitionRequest.build());
            GlueUtils.validateGlueResponse(response);
            List<CatalogPartitionSpec> finalPartitionsList =
                    response.partitions().stream()
                            .map(this::getCatalogPartitionSpec)
                            .collect(Collectors.toList());
            String partitionsResultNextToken = response.nextToken();
            while (Optional.ofNullable(partitionsResultNextToken).isPresent()) {
                getPartitionRequest.nextToken(partitionsResultNextToken);
                response = glueClient.getPartitions(getPartitionRequest.build());
                finalPartitionsList.addAll(
                        response.partitions().stream()
                                .map(this::getCatalogPartitionSpec)
                                .collect(Collectors.toList()));
                partitionsResultNextToken = response.nextToken();
            }
            return finalPartitionsList;
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Get list of CatalogPartitionSpec from glue data catalog service associated with table and
     * matches given CatalogPartitionSpec.
     *
     * @param tablePath Fully qualified table Path.
     * @param partitionSpec Partition spec .
     * @return List of CatalogPartitionSpec.
     */
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        List<CatalogPartitionSpec> partitionSpecList = listPartitions(tablePath);
        return partitionSpecList.stream()
                .filter(
                        currPartSpec ->
                                currPartSpec
                                        .getPartitionSpec()
                                        .entrySet()
                                        .containsAll(partitionSpec.getPartitionSpec().entrySet()))
                .collect(Collectors.toList());
    }

    /**
     * Get List of CatalogPartitionSpec of partition by expression filters from glue data catalog
     * associated with the given Table.
     *
     * @param tablePath Fully Qualified Table Path.
     * @param filters List of Filters.
     * @return List of Partition Spec
     */
    public List<CatalogPartitionSpec> listGluePartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) {
        String expression =
                filters.stream()
                        .map(GlueUtils::getExpressionString)
                        .collect(
                                Collectors.joining(
                                        GlueCatalogConstants.SPACE
                                                + GlueCatalogConstants.AND
                                                + GlueCatalogConstants.SPACE));
        try {
            GetPartitionsRequest.Builder getPartitionsRequest =
                    GetPartitionsRequest.builder()
                            .databaseName(tablePath.getDatabaseName())
                            .tableName(tablePath.getObjectName())
                            .catalogId(getGlueCatalogId())
                            .expression(expression);
            GetPartitionsResponse response = glueClient.getPartitions(getPartitionsRequest.build());
            List<CatalogPartitionSpec> catalogPartitionSpecList =
                    response.partitions().stream()
                            .map(this::getCatalogPartitionSpec)
                            .collect(Collectors.toList());
            GlueUtils.validateGlueResponse(response);
            String nextToken = response.nextToken();
            while (Optional.ofNullable(nextToken).isPresent()) {
                getPartitionsRequest.nextToken(nextToken);
                response = glueClient.getPartitions(getPartitionsRequest.build());
                GlueUtils.validateGlueResponse(response);
                catalogPartitionSpecList.addAll(
                        response.partitions().stream()
                                .map(this::getCatalogPartitionSpec)
                                .collect(Collectors.toList()));
                nextToken = response.nextToken();
            }
            return catalogPartitionSpecList;
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Get a list of ordered partition values by re-arranging them based on the given list of
     * partition keys. If the partition value is null, it'll be converted into default partition
     * name.
     *
     * @param partitionSpec a partition spec.
     * @param partitionKeys a list of partition keys.
     * @param tablePath path of the table to which the partition belongs.
     * @return A list of partition values ordered according to partitionKeys.
     * @throws PartitionSpecInvalidException thrown if partitionSpec and partitionKeys have
     *     different sizes, or any key in partitionKeys doesn't exist in partitionSpec.
     */
    private List<String> getOrderedFullPartitionValues(
            CatalogPartitionSpec partitionSpec, List<String> partitionKeys, ObjectPath tablePath)
            throws PartitionSpecInvalidException {
        Map<String, String> spec = partitionSpec.getPartitionSpec();
        if (spec.size() != partitionKeys.size()) {
            throw new PartitionSpecInvalidException(
                    catalogName, partitionKeys, tablePath, partitionSpec);
        }

        List<String> values = new ArrayList<>(spec.size());
        for (String key : partitionKeys) {
            if (!spec.containsKey(key)) {
                throw new PartitionSpecInvalidException(
                        catalogName, partitionKeys, tablePath, partitionSpec);
            } else {
                String value = spec.get(key);
                if (value == null) {
                    value = GlueCatalogConstants.DEFAULT_PARTITION_NAME;
                }
                values.add(value);
            }
        }

        return values;
    }

    /**
     * Validate and ensure Table is Partitioned.
     *
     * @param tablePath Fully Qualified TablePath.
     * @param glueTable Instance of {@link Table} from glue data catalog.
     * @throws TableNotPartitionedException In case of table is not partitioned.
     */
    public void ensurePartitionedTable(ObjectPath tablePath, Table glueTable)
            throws TableNotPartitionedException {
        if (!glueTable.hasPartitionKeys()) {
            throw new TableNotPartitionedException(catalogName, tablePath);
        }
    }

    /**
     * Derive {@link CatalogPartitionSpec} instance from {@link Partition}.
     *
     * @param partition Glue Partition instance
     * @return {@link CatalogPartitionSpec} instance
     */
    private CatalogPartitionSpec getCatalogPartitionSpec(Partition partition) {
        Map<String, String> params = new HashMap<>(partition.storageDescriptor().parameters());
        return new CatalogPartitionSpec(params);
    }
}
