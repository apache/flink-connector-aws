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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.glue.GlueCatalogConfig;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.GlueResponse;
import software.amazon.awssdk.services.glue.model.ResourceUri;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.glue.GlueCatalogConfig.TABLE_LOCATION_URI;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
Utilities for Glue catalog operations.
*/
public class GlueUtils {
    /*
        Defines the location URI for database.
     */
    public final String locationUri;

    public final AwsProperties awsProperties;

    /*
    http client for glue client.
    Current implementation for client is sync type.
     */
    public  final GlueClient glueClient;

    public static final String LOCATION_URI = "location_uri";

    private static final String FLINK_SCALA_FUNCTION_PREFIX = "flink:scala:";
    private static final String FLINK_PYTHON_FUNCTION_PREFIX = "flink:python:";
    private static final String FLINK_JAVA_FUNCTION_PREFIX = "flink:java:";

    private static final Logger LOG = LoggerFactory.getLogger(GlueUtils.class);

    public GlueUtils(String locationUri, AwsProperties awsProperties, GlueClient glueClient) {
        this.locationUri = locationUri;
        this.awsProperties = awsProperties;
        this.glueClient = glueClient;
    }

    public static void validateGlueResponse(GlueResponse response) {
        if (!response.sdkHttpResponse().isSuccessful()) {
            throw new CatalogException(
                    String.format(
                            "Exception in glue call. RequestId: %s",
                            response.responseMetadata().requestId()));
        }
    }

    public void deleteAllTablesInDatabase(String databaseName){
        BatchDeleteTableRequest batchTableRequest =
                BatchDeleteTableRequest.builder()
                        .databaseName(databaseName)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        BatchDeleteTableResponse response = glueClient.batchDeleteTable(batchTableRequest);
        if (!response.sdkHttpResponse().isSuccessful()) {
            throw new CatalogException(
                    String.format(
                            "Glue Table errors:- %s",
                            response.errors().stream()
                                    .map(
                                            e ->
                                                    "Table: "
                                                            + e.tableName()
                                                            + "\nErrorDetail:  "
                                                            + e.errorDetail()
                                                            .errorMessage())
                                    .collect(Collectors.joining("\n"))));
        }
    }

    public void deleteAllFunctionInDatabase(String databaseName) {
        // todo implement detailed functionality
    }

    public boolean isDatabaseEmpty(String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        GetTablesRequest tablesRequest =
                GetTablesRequest.builder()
                        .catalogId(awsProperties.getGlueCatalogId())
                        .databaseName(databaseName)
                        .build();
        GetTablesResponse response = glueClient.getTables(tablesRequest);
        if (response.sdkHttpResponse().isSuccessful()) {
            return response.tableList().size() == 0;
        }
        return false;
    }

    /**
     * Delete a database from Glue data catalog when it is empty. *
     */
    public void deleteGlueDatabase(String name) throws CatalogException {
        DeleteDatabaseRequest deleteDatabaseRequest =
                DeleteDatabaseRequest.builder()
                        .name(name)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        DeleteDatabaseResponse response = glueClient.deleteDatabase(deleteDatabaseRequest);
        if (!response.sdkHttpResponse().isSuccessful()) {
            throw new CatalogException("Exception in glue while deleting database.");
        }
    }

    public CatalogDatabase getExistingDatabase(String name) {
        GetDatabaseRequest getDatabaseRequest =
                GetDatabaseRequest.builder()
                        .name(name)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        GetDatabaseResponse response = glueClient.getDatabase(getDatabaseRequest);
        if (!response.sdkHttpResponse().isSuccessful()) {
            LOG.warn(
                    "Glue Exception existing database. Client call response :- "
                            + response.sdkHttpResponse().statusText());
            return null;
        }

        return getCatalogDatabase(response);
    }

    private static CatalogDatabase getCatalogDatabase(GetDatabaseResponse response) {
        Map<String, String> properties = response.database().parameters();
        String comment = response.database().description();
        return new CatalogDatabaseImpl(properties, comment);
    }

    public void updateGlueDatabase(String databaseName, CatalogDatabase newDatabase)
            throws CatalogException {
        DatabaseInput input = buildGlueDatabaseInput(databaseName, newDatabase);
        UpdateDatabaseRequest updateRequest =
                UpdateDatabaseRequest.builder()
                        .databaseInput(input)
                        .name(databaseName)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        UpdateDatabaseResponse response = glueClient.updateDatabase(updateRequest);
        GlueUtils.validateGlueResponse(response);
    }

    static DatabaseInput buildGlueDatabaseInput(String name, CatalogDatabase database) {
        return DatabaseInput.builder()
                .parameters(database.getProperties())
                .description(database.getDescription().orElse(null))
                .name(name)
                .build();
    }

    public static Map<String, String> getDatabaseProperties(GetDatabaseResponse awsDatabaseResponse) {
        Map<String, String> properties = new HashMap<>(awsDatabaseResponse.database().parameters());
        properties.put("name", awsDatabaseResponse.database().name());
        properties.put("catalogId", awsDatabaseResponse.database().catalogId());
        properties.put(LOCATION_URI, awsDatabaseResponse.database().locationUri());
        properties.put(
                "createTime",
                String.valueOf(awsDatabaseResponse.database().createTime().getEpochSecond()));
        properties.put(
                "sdkFields",
                awsDatabaseResponse.database().sdkFields().stream()
                        .map(field -> field.location().name())
                        .collect(Collectors.joining(",")));
        return properties;
    }

    public List<Column> getGlueColumnsFromCatalogTable(CatalogBaseTable table) {
        List<Column> glueColumns = new ArrayList<>();
        for (Schema.UnresolvedColumn column : table.getUnresolvedSchema().getColumns()) {
            glueColumns.add(
                    Column.builder()
                            .name(column.getName())
                            .type(getDataTypeMaping(column))
                            .build());
        }
        return glueColumns;
    }

    public static String getDataTypeMaping(Schema.UnresolvedColumn column) {
        StringBuilder dataType = new StringBuilder();
        // todo implement logic
        return dataType.toString();
    }

    public StorageDescriptor getTableStorageDescriptor(
            ObjectPath tablePath, Collection<Column> glueColumns, Map<String, String> options) {
        return StorageDescriptor.builder()
                .columns(glueColumns)
                .location(locationUri + "/" + tablePath.getFullName())
                .parameters(options)
                .build();
    }

    CreateTableRequest createTableRequest(ObjectPath tablePath, TableInput tableInput) {
        return CreateTableRequest.builder()
                .tableInput(tableInput)
                .databaseName(tablePath.getDatabaseName())
                .catalogId(awsProperties.getGlueCatalogId())
                .build();
    }

    public TableInput createTableInput(
            ObjectPath tablePath, CatalogBaseTable table, StorageDescriptor storageDescriptor) {
        return TableInput.builder()
                .tableType(table.getTableKind().name())
                .name(tablePath.getObjectName())
                .description(table.getDescription().orElse(null))
                .storageDescriptor(storageDescriptor)
                .parameters(table.getOptions())
                .build();
    }

    public void createGlueTable(ObjectPath tablePath, CatalogBaseTable table)
            throws TableAlreadyExistException, CatalogException {

        Collection<Column> glueColumns = getGlueColumnsFromCatalogTable(table);
        StorageDescriptor storageDescriptor =
                getTableStorageDescriptor(tablePath, glueColumns, table.getOptions());
        TableInput tableInput = createTableInput(tablePath, table, storageDescriptor);
        CreateTableRequest tableRequest = createTableRequest(tablePath, tableInput);
        CreateTableResponse response = glueClient.createTable(tableRequest);
        validateGlueResponse(response);
    }

    public UserDefinedFunctionInput createFunctionInput(ObjectPath functionPath, CatalogFunction function) {
        // Glue Function does not have properties map
        // thus, use a prefix in class name to distinguish Java/Scala and Python functions
        String functionClassName;
        if (function.getFunctionLanguage() == FunctionLanguage.JAVA) {
            functionClassName = FLINK_JAVA_FUNCTION_PREFIX + function.getClassName();
        } else if (function.getFunctionLanguage() == FunctionLanguage.SCALA) {
            functionClassName = FLINK_SCALA_FUNCTION_PREFIX + function.getClassName();
        } else if (function.getFunctionLanguage() == FunctionLanguage.PYTHON) {
            functionClassName = FLINK_PYTHON_FUNCTION_PREFIX + function.getClassName();
        } else {
            throw new UnsupportedOperationException(
                    "HiveCatalog supports only creating"
                            + " JAVA/SCALA or PYTHON based function for now");
        }
        Collection<software.amazon.awssdk.services.glue.model.ResourceUri> resourceUris = new LinkedList<>();
        for (org.apache.flink.table.resource.ResourceUri resourceUri : function.getFunctionResources()) {
            switch (resourceUri.getResourceType()) {
                case JAR:

                case FILE:
                case ARCHIVE:
                default:
            }
            resourceUris.add(ResourceUri.builder().resourceType(resourceUri.getResourceType().name()).uri(resourceUri.getUri()).build());
        }
        return UserDefinedFunctionInput.builder().functionName(functionPath.getObjectName())
                .className(function.getClassName())
                .resourceUris(resourceUris)
                .build();
    }

    public void alterFunctionInGlue(ObjectPath functionPath, CatalogFunction newFunction) {
        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, newFunction);
        UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest = UpdateUserDefinedFunctionRequest.builder()
                .functionName(functionPath.getObjectName())
                .databaseName(functionPath.getDatabaseName())
                .catalogId(awsProperties.getGlueCatalogId())
                .functionInput(functionInput).build();
        UpdateUserDefinedFunctionResponse response = glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
        GlueUtils.validateGlueResponse(response);
    }

    public void updateGlueTablePartition(ObjectPath tablePath, CatalogBaseTable table) {
        // todo
    }

    public void updateGluePartitionStats(ObjectPath tablePath, CatalogBaseTable table) {
        // todo

    }

    public void updateGluePartitionColumnStats(ObjectPath tablePath, CatalogBaseTable table) {
        // todo
    }

    public void alterGlueTable(ObjectPath tablePath, CatalogBaseTable newTable) {
        Collection<Column> columns = getGlueColumnsFromCatalogTable(newTable);
        StorageDescriptor storageDesc =
                getTableStorageDescriptor(tablePath, columns, newTable.getOptions());
        TableInput tableInput = createTableInput(tablePath, newTable, storageDesc);
        UpdateTableRequest tableRequest =
                UpdateTableRequest.builder().tableInput(tableInput).build();
        UpdateTableResponse response = glueClient.updateTable(tableRequest);
        GlueUtils.validateGlueResponse(response);
    }

    public void deleteGlueTable(ObjectPath tablePath) {
        DeleteTableRequest.Builder tableRequestBuilder =
                DeleteTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(awsProperties.getGlueCatalogId());

        DeleteTableResponse response = glueClient.deleteTable(tableRequestBuilder.build());
        GlueUtils.validateGlueResponse(response);
    }

    public void updateGlueTable(CatalogBaseTable exisitingTable, ObjectPath newTablePath)
            throws TableNotExistException {
        StorageDescriptor storageDescriptor =
                getTableStorageDescriptor(
                        newTablePath,
                        getGlueColumnsFromCatalogTable(exisitingTable),
                        exisitingTable.getOptions());
        TableInput tableInput = createTableInput(newTablePath, exisitingTable, storageDescriptor);
        UpdateTableRequest tableRequest =
                UpdateTableRequest.builder()
                        .databaseName(newTablePath.getDatabaseName())
                        .tableInput(tableInput)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        UpdateTableResponse response = glueClient.updateTable(tableRequest);
        GlueUtils.validateGlueResponse(response);
    }

    public List<String> getGlueTableList(String databaseName, String type) {
        GetTablesRequest tablesRequest =
                GetTablesRequest.builder()
                        .databaseName(databaseName)
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();

        GetTablesResponse response = glueClient.getTables(tablesRequest);
        GlueUtils.validateGlueResponse(response);
        List<Table> finalTableList = new ArrayList<>(response.tableList());
        String tableResultNextToken = response.nextToken();

        if (Optional.ofNullable(tableResultNextToken).isPresent()) {
            do {
                // creating a new GetTablesResult using next token.
                tablesRequest =
                        GetTablesRequest.builder()
                                .databaseName(databaseName)
                                .nextToken(tableResultNextToken)
                                .catalogId(awsProperties.getGlueCatalogId())
                                .build();
                response = glueClient.getTables(tablesRequest);
                finalTableList.addAll(response.tableList());
                tableResultNextToken = response.nextToken();
            } while (Optional.ofNullable(tableResultNextToken).isPresent());
        }
        if (type.equalsIgnoreCase("catalogtable")) {
            return finalTableList.stream()
                    .filter(
                            table ->
                                    Objects.equals(
                                            table.tableType().toLowerCase(Locale.ROOT), "table"))
                    .map(Table::name)
                    .collect(Collectors.toList());
        } else {
            return finalTableList.stream()
                    .filter(
                            table ->
                                    !Objects.equals(
                                            table.tableType().toLowerCase(Locale.ROOT), "table"))
                    .map(Table::name)
                    .collect(Collectors.toList());
        }
    }

    public Table getGlueTable(ObjectPath tablePath) {
        GetTableRequest tablesRequest =
                GetTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        GetTableResponse response = glueClient.getTable(tablesRequest);
        GlueUtils.validateGlueResponse(response);
        return response.table();
    }

    public Tuple2<Schema, String> getSchemaFromGlueTable(Table glueTable) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        StringBuilder comment = new StringBuilder();
        for (Column col : glueTable.storageDescriptor().columns()) {
            schemaBuilder.column(col.name(), col.type());
            comment.append(col.name()).append(":").append(col.comment()).append("\n");
        }
        return new Tuple2<>(schemaBuilder.build(), comment.toString());
    }

    public List<String> getPartitionKeysFromGlueTable(Table glueTable) {
        return glueTable.partitionKeys().stream().map(Column::name).collect(Collectors.toList());
    }

    public boolean glueTableExists(ObjectPath tablePath) {
        GetTableRequest tableRequest =
                GetTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(awsProperties.getGlueCatalogId())
                        .build();
        GetTableResponse response = glueClient.getTable(tableRequest);
        GlueUtils.validateGlueResponse(response);
        return response.table().name().equals(tablePath.getObjectName());
    }

    public void createGlueFunction(ObjectPath functionPath, CatalogFunction function) {
        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, function);
        CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest = CreateUserDefinedFunctionRequest.builder()
                .databaseName(functionPath.getDatabaseName())
                .catalogId(awsProperties.getGlueCatalogId())
                .functionInput(functionInput)
                .build();
        CreateUserDefinedFunctionResponse response = glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest);
        GlueUtils.validateGlueResponse(response);
    }

    public void removeGlueFunction(ObjectPath functionPath) {
        DeleteUserDefinedFunctionRequest request = DeleteUserDefinedFunctionRequest.builder()
                .catalogId(awsProperties.getGlueCatalogId())
                .functionName(functionPath.getObjectName())
                .databaseName(functionPath.getDatabaseName())
                .build();
        DeleteUserDefinedFunctionResponse response = glueClient.deleteUserDefinedFunction(request);
        GlueUtils.validateGlueResponse(response);
    }

    public List<String> listGlueFunctions(String databaseName) {
        GetUserDefinedFunctionsRequest request = GetUserDefinedFunctionsRequest.builder().databaseName(databaseName)
                .catalogId(awsProperties.getGlueCatalogId())
                .build();
        GetUserDefinedFunctionsResponse response = glueClient.getUserDefinedFunctions(request);
        GlueUtils.validateGlueResponse(response);
        String token = response.nextToken();
        List<String> glueFunctions = response
                .userDefinedFunctions()
                .stream()
                .map(
                        UserDefinedFunction::functionName)
                .collect(Collectors.toCollection(LinkedList::new));
        if (Optional.ofNullable(token).isPresent()) {
            do {
                request = GetUserDefinedFunctionsRequest.builder()
                        .catalogId(awsProperties.getGlueCatalogId())
                        .nextToken(token)
                        .build();
                response = glueClient.getUserDefinedFunctions(request);
                GlueUtils.validateGlueResponse(response);
                glueFunctions.addAll(response
                        .userDefinedFunctions()
                        .stream()
                        .map(
                                UserDefinedFunction::functionName)
                        .collect(Collectors.toCollection(LinkedList::new)));
                token = response.nextToken();
            } while (Optional.ofNullable(token).isPresent());
        }
        return glueFunctions;
    }

    public FunctionLanguage getFunctionalLanguage(UserDefinedFunction glueFunction) {
        // todo implement getfuntionalLanguage
        switch (glueFunction.functionName().toLowerCase()) {
//            case FunctionLanguage.JAVA:
//
//            case FunctionLanguage.PYTHON:
//
//            case FunctionLanguage.SCALA:

            default:
                throw new CatalogException("Invalid Functional Language");
        }
    }

    public  boolean isGlueFunctionExists(ObjectPath functionPath) {
        GetUserDefinedFunctionRequest request = GetUserDefinedFunctionRequest.builder()
                .functionName(functionPath.getObjectName())
                .catalogId(awsProperties.getGlueCatalogId())
                .databaseName(functionPath.getDatabaseName()).build();
        GetUserDefinedFunctionResponse response = glueClient.getUserDefinedFunction(request);
        GlueUtils.validateGlueResponse(response);
        return response.userDefinedFunction().functionName().equals(functionPath.getObjectName());
    }

    public void ensurePartitionedTable(ObjectPath tablePath, Table glueTable) {
        // todo provide implemetation
    }

    public CreatePartitionRequest instantiateGluePartition(
            Table glueTable, CatalogPartitionSpec partitionSpec, CatalogPartition catalogPartition, String catalogName)
            throws PartitionSpecInvalidException {
        List<String> partCols = getFieldNames(glueTable);
        List<String> partValues =
                getOrderedFullPartitionValues(
                        partitionSpec,
                        partCols,
                        new ObjectPath(glueTable.databaseName(), glueTable.name()));
        // validate partition values
        for (int i = 0; i < partCols.size(); i++) {
            if (isNullOrWhitespaceOnly(partValues.get(i))) {
                throw new PartitionSpecInvalidException(
                        catalogName,
                        partCols,
                        new ObjectPath(glueTable.databaseName(), glueTable.name()),
                        partitionSpec);
            }
        }
        // TODO: handle GenericCatalogPartition
        StorageDescriptor.Builder sd = glueTable.storageDescriptor().toBuilder();
        sd.location(catalogPartition.getProperties().remove(TABLE_LOCATION_URI));

        Map<String, String> properties = new HashMap<>(catalogPartition.getProperties());
        String comment = catalogPartition.getComment();
        if (comment != null) {
            properties.put(GlueCatalogConfig.COMMENT, comment);
        }
        return createGluePartition(glueTable, partValues, sd.build(), properties);
    }

    public List<String> getFieldNames(Table gluetable) {
        List<String> fields = new ArrayList<>();
        // todo provide implementations
        return fields;
    }

    public List<String> getOrderedFullPartitionValues(CatalogPartitionSpec partitionSpec, List<String> partCols, ObjectPath tablePath) {
        // todo provide implementations
        return new ArrayList<>();
    }

    public software.amazon.awssdk.services.glue.model.CreatePartitionRequest createGluePartition(Table glueTable,
                                                                                                  List<String> partValues,
                                                                                                  StorageDescriptor sd, Map<String, String> properties) {
        // todo implement properly
        return software.amazon.awssdk.services.glue.model.CreatePartitionRequest.builder().build();
    }

    //    public static PartitionInput convertToPartitionInput(com.amazonaws.services.glue.model.Partition src) {
//        PartitionInput partitionInput = new PartitionInput();
//
//        partitionInput.setLastAccessTime(src.getLastAccessTime());
//        partitionInput.setParameters(src.getParameters());
//        partitionInput.setStorageDescriptor(src.getStorageDescriptor());
//        partitionInput.setValues(src.getValues());
//
//        return partitionInput;
//    }
}
