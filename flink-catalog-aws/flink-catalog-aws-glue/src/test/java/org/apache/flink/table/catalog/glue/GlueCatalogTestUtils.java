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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.glue.GlueCatalogTest.WAREHOUSE_PATH;

/** Contains Utilities for Glue Catalog Tests. */
public class GlueCatalogTestUtils {

    public static final String DATABASE_DESCRIPTION = "Test database";
    public static final String DATABASE_1 = "db1";
    public static final String DATABASE_2 = "db2";
    public static final String TABLE_1 = "t1";
    public static final String TABLE_2 = "t2";
    public static final String TABLE_3 = "t3";
    public static final String TABLE_4 = "t4";
    public static final String TABLE_5 = "t5";
    public static final String VIEW_1 = "v1";
    public static final String VIEW_2 = "v2";
    public static final String COLUMN_1 = "name";
    public static final String COLUMN_2 = "age";
    public static final String COMMENT = "comment";
    public static final String EXPANDED_TEXT = "TEST EXPANDED_TEXT";
    public static final String ORIGINAL_TEXT = "TEST ORIGINAL_TEXT";
    public static final String FUNCTION_1 = "f1";

    public static Map<String, String> getDatabaseParams() {
        return new HashMap<String, String>() {
            {
                put("key", "value");
                put("location-uri", WAREHOUSE_PATH);
            }
        };
    }

    public static Map<String, String> getDummyTableParams() {
        return new HashMap<String, String>() {
            {
                put("tableParam1", "v1");
                put("tableParam2", "v2");
                put("tableParams3", "v3");
                put("tableParams4", "v4");
            }
        };
    }

    public static Map<String, String> getPartitionSpecParams() {
        return new HashMap<String, String>() {
            {
                put(COLUMN_1, "v1");
                put(COLUMN_2, "v2");
            }
        };
    }

    /**
     * Parameter related to partition.
     *
     * @return Partition Properties
     */
    public static Map<String, String> getCatalogPartitionParams() {
        return new HashMap<String, String>() {
            {
                put("k1", "v1");
                put("k2", "v2");
            }
        };
    }

    public static SdkHttpResponse dummySdkHttpResponse(int statusCode) {
        return SdkHttpResponse.builder().statusCode(statusCode).build();
    }

    public static Database getDatabaseFromCreateDatabaseRequest(CreateDatabaseRequest request) {
        return Database.builder()
                .catalogId(request.catalogId())
                .name(request.databaseInput().name())
                .parameters(request.databaseInput().parameters())
                .description(request.databaseInput().description())
                .locationUri(request.databaseInput().locationUri())
                .build();
    }

    public static Table getTableFromCreateTableRequest(CreateTableRequest request) {
        return Table.builder()
                .catalogId(request.catalogId())
                .databaseName(request.databaseName())
                .name(request.tableInput().name())
                .parameters(request.tableInput().parameters())
                .createdBy(request.tableInput().owner())
                .description(request.tableInput().description())
                .createTime(Instant.now())
                .partitionKeys(request.tableInput().partitionKeys())
                .storageDescriptor(request.tableInput().storageDescriptor())
                .tableType(request.tableInput().tableType())
                .updateTime(Instant.now())
                .viewExpandedText(request.tableInput().viewExpandedText())
                .viewOriginalText(request.tableInput().viewOriginalText())
                .build();
    }

    public static Table getTableFromUpdateTableRequest(UpdateTableRequest request) {
        return Table.builder()
                .catalogId(request.catalogId())
                .databaseName(request.databaseName())
                .name(request.tableInput().name())
                .parameters(request.tableInput().parameters())
                .createdBy(request.tableInput().owner())
                .description(request.tableInput().description())
                .createTime(Instant.now())
                .partitionKeys(request.tableInput().partitionKeys())
                .storageDescriptor(request.tableInput().storageDescriptor())
                .tableType(request.tableInput().tableType())
                .updateTime(Instant.now())
                .viewExpandedText(request.tableInput().viewExpandedText())
                .viewOriginalText(request.tableInput().viewOriginalText())
                .build();
    }

    public static String getFullyQualifiedName(String databaseName, String tableName) {
        return databaseName + "." + tableName;
    }

    public static Partition getPartitionFromCreatePartitionRequest(CreatePartitionRequest request) {
        return Partition.builder()
                .databaseName(request.databaseName())
                .parameters(request.partitionInput().parameters())
                .tableName(request.tableName())
                .storageDescriptor(request.partitionInput().storageDescriptor())
                .values(request.partitionInput().values())
                .build();
    }

    public static Partition getPartitionFromUpdatePartitionRequest(UpdatePartitionRequest request) {
        return Partition.builder()
                .storageDescriptor(request.partitionInput().storageDescriptor())
                .tableName(request.tableName())
                .databaseName(request.databaseName())
                .parameters(request.partitionInput().parameters())
                .values(request.partitionInput().values())
                .build();
    }

    public static CatalogDatabase getDummyCatalogDatabase() {
        return new CatalogDatabaseImpl(getDatabaseParams(), DATABASE_DESCRIPTION);
    }

    public static UserDefinedFunction getUDFFromCreateUserDefinedFunctionRequest(
            CreateUserDefinedFunctionRequest request) {
        return UserDefinedFunction.builder()
                .functionName(request.functionInput().functionName())
                .databaseName(request.databaseName())
                .className(request.functionInput().className())
                .resourceUris(request.functionInput().resourceUris())
                .build();
    }

    public static List<org.apache.flink.table.resource.ResourceUri> dummyFlinkResourceUri() {
        List<org.apache.flink.table.resource.ResourceUri> resourceUris = new ArrayList<>();
        resourceUris.add(
                new org.apache.flink.table.resource.ResourceUri(
                        org.apache.flink.table.resource.ResourceType.JAR, "URI-JAR"));
        resourceUris.add(
                new org.apache.flink.table.resource.ResourceUri(
                        org.apache.flink.table.resource.ResourceType.FILE, "URI-FILE"));
        resourceUris.add(
                new org.apache.flink.table.resource.ResourceUri(
                        org.apache.flink.table.resource.ResourceType.ARCHIVE, "URI-ARCHIVE"));
        return resourceUris;
    }

    public static Database getDatabaseFromUpdateDatabaseRequest(
            UpdateDatabaseRequest updateDatabaseRequest) {
        return Database.builder()
                .catalogId(updateDatabaseRequest.catalogId())
                .name(updateDatabaseRequest.name())
                .locationUri(updateDatabaseRequest.databaseInput().locationUri())
                .description(updateDatabaseRequest.databaseInput().description())
                .parameters(updateDatabaseRequest.databaseInput().parameters())
                .build();
    }

    public static ResolvedCatalogTable getDummyCatalogTable() {
        Column column1 = Column.physical(COLUMN_1, DataTypes.STRING());
        Column column2 = Column.physical(COLUMN_2, DataTypes.STRING());
        ResolvedSchema schema = ResolvedSchema.of(Arrays.asList(column1, column2));
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(COLUMN_1, DataTypes.STRING())
                                .column(COLUMN_2, DataTypes.STRING())
                                .build(),
                        COMMENT,
                        new ArrayList<>(),
                        getDummyTableParams());
        return new ResolvedCatalogTable(catalogTable, schema);
    }

    public static CatalogBaseTable getDummyCatalogTableWithPartition() {
        Column column1 = Column.physical(COLUMN_1, DataTypes.STRING());
        Column column2 = Column.physical(COLUMN_2, DataTypes.STRING());
        ResolvedSchema schema = ResolvedSchema.of(Arrays.asList(column1, column2));
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(COLUMN_1, DataTypes.STRING())
                                .column(COLUMN_2, DataTypes.STRING())
                                .build(),
                        COMMENT,
                        Arrays.asList(COLUMN_1, COLUMN_2),
                        getDummyTableParams());
        return new ResolvedCatalogTable(catalogTable, schema);
    }

    public static CatalogFunction getDummyCatalogFunction() {
        return new CatalogFunctionImpl("Test Function", FunctionLanguage.JAVA);
    }
}
