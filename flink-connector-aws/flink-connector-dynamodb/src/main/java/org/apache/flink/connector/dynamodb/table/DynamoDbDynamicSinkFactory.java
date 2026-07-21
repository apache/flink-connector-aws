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

package org.apache.flink.connector.dynamodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connector.dynamodb.table.DynamoDbConnectorOptions.AWS_REGION;
import static org.apache.flink.connector.dynamodb.table.DynamoDbConnectorOptions.TABLE_NAME;

/** Factory for creating {@link DynamoDbDynamicSink}. */
@Internal
public class DynamoDbDynamicSinkFactory extends AsyncDynamicTableSinkFactory {

    public static final String FACTORY_IDENTIFIER = "dynamodb";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper factoryHelper =
                FactoryUtil.createTableFactoryHelper(this, context);
        ResolvedCatalogTable catalogTable = context.getCatalogTable();

        FactoryUtil.validateFactoryOptions(this, factoryHelper.getOptions());

        DynamoDbConfiguration dynamoDbConfiguration =
                new DynamoDbConfiguration(catalogTable.getOptions(), factoryHelper.getOptions());

        List<String> primaryKey = getValidatedPrimaryKey(catalogTable);
        Set<String> overwriteByPartitionKeys =
                getValidatedOverwriteByPartitionKeys(catalogTable, primaryKey);

        DynamoDbDynamicSink.DynamoDbDynamicTableSinkBuilder builder =
                DynamoDbDynamicSink.builder()
                        .setTableName(dynamoDbConfiguration.getTableName())
                        .setFailOnError(dynamoDbConfiguration.getFailOnError())
                        .setIgnoreNulls(dynamoDbConfiguration.getIgnoreNulls())
                        .setPhysicalDataType(
                                catalogTable.getResolvedSchema().toPhysicalRowDataType())
                        .setOverwriteByPartitionKeys(overwriteByPartitionKeys)
                        .setPrimaryKey(primaryKey)
                        .setDynamoDbClientProperties(
                                dynamoDbConfiguration.getSinkClientProperties());

        addAsyncOptionsToBuilder(dynamoDbConfiguration.getAsyncSinkProperties(), builder);

        return builder.build();
    }

    /**
     * Returns the declared PRIMARY KEY columns in declaration order (partition key first, optional
     * sort key second), validating that at most two columns are declared.
     */
    private static List<String> getValidatedPrimaryKey(ResolvedCatalogTable catalogTable) {
        List<String> primaryKey =
                catalogTable
                        .getResolvedSchema()
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(List.of());

        if (primaryKey.size() > 2) {
            throw new ValidationException(
                    String.format(
                            "The DynamoDB sink supports a PRIMARY KEY of at most two columns (a "
                                    + "partition key and an optional sort key), but %d columns were "
                                    + "declared: %s. Please declare a PRIMARY KEY that matches the "
                                    + "DynamoDB table's key schema.",
                            primaryKey.size(), primaryKey));
        }
        return primaryKey;
    }

    /**
     * Returns the partition keys used for client-side deduplication, defaulting to the primary key
     * when PARTITIONED BY is not specified. When both are declared they must match; otherwise a CDC
     * batch could keep an upsert and a delete that map to the same DynamoDB key, which DynamoDB
     * rejects as duplicates.
     */
    private static Set<String> getValidatedOverwriteByPartitionKeys(
            ResolvedCatalogTable catalogTable, List<String> primaryKey) {
        List<String> declaredPartitionKeys = catalogTable.getPartitionKeys();

        if (!declaredPartitionKeys.isEmpty()
                && !primaryKey.isEmpty()
                && !new HashSet<>(declaredPartitionKeys).equals(new HashSet<>(primaryKey))) {
            throw new ValidationException(
                    String.format(
                            "When both PARTITIONED BY and PRIMARY KEY are specified for a DynamoDB "
                                    + "table they must reference the same columns, but PARTITIONED "
                                    + "BY was %s and PRIMARY KEY was %s. Either align them or "
                                    + "specify only the PRIMARY KEY.",
                            declaredPartitionKeys, primaryKey));
        }

        return declaredPartitionKeys.isEmpty()
                ? new HashSet<>(primaryKey)
                : new HashSet<>(declaredPartitionKeys);
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(AWS_REGION);

        return requiredOptions;
    }
}
