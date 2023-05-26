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
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
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

        DynamoDbDynamicSink.DynamoDbDynamicTableSinkBuilder builder =
                DynamoDbDynamicSink.builder()
                        .setTableName(dynamoDbConfiguration.getTableName())
                        .setFailOnError(dynamoDbConfiguration.getFailOnError())
                        .setPhysicalDataType(
                                catalogTable.getResolvedSchema().toPhysicalRowDataType())
                        .setOverwriteByPartitionKeys(new HashSet<>(catalogTable.getPartitionKeys()))
                        .setDynamoDbClientProperties(
                                dynamoDbConfiguration.getSinkClientProperties());

        addAsyncOptionsToBuilder(dynamoDbConfiguration.getAsyncSinkProperties(), builder);

        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return ImmutableSet.of(TABLE_NAME, AWS_REGION);
    }
}
