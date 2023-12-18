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

import lombok.Data;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueServiceClientConfiguration;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.GlueEncryptionException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.InvalidStateException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.ResourceNotReadyException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.dummySdkHttpResponse;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getFullyQualifiedName;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getPartitionFromCreatePartitionRequest;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getTableFromCreateTableRequest;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getTableFromUpdateTableRequest;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.getUDFFromCreateUserDefinedFunctionRequest;

/** Dummy Glue client for Test. */
@Data
public class DummyGlueClient implements GlueClient {

    public Map<String, Database> databaseMap;

    public Map<String, Table> tableMap;

    public Map<String, UserDefinedFunction> userDefinedFunctionMap;

    public Map<String, List<Partition>> partitionMap;

    @Override
    public UpdateUserDefinedFunctionResponse updateUserDefinedFunction(
            UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {
        String functionName =
                getFullyQualifiedName(
                        updateUserDefinedFunctionRequest.databaseName(),
                        updateUserDefinedFunctionRequest.functionName());
        if (!databaseMap.containsKey(updateUserDefinedFunctionRequest.databaseName())
                || !userDefinedFunctionMap.containsKey(functionName)) {
            throw EntityNotFoundException.builder().build();
        }
        UserDefinedFunction udf = userDefinedFunctionMap.get(functionName);
        UserDefinedFunction updatedUDF =
                udf.toBuilder()
                        .catalogId(updateUserDefinedFunctionRequest.catalogId())
                        .functionName(updateUserDefinedFunctionRequest.functionName())
                        .databaseName(updateUserDefinedFunctionRequest.databaseName())
                        .ownerName(updateUserDefinedFunctionRequest.functionInput().ownerName())
                        .ownerType(updateUserDefinedFunctionRequest.functionInput().ownerType())
                        .className(updateUserDefinedFunctionRequest.functionInput().className())
                        .resourceUris(
                                updateUserDefinedFunctionRequest.functionInput().resourceUris())
                        .build();
        userDefinedFunctionMap.put(functionName, updatedUDF);
        return (UpdateUserDefinedFunctionResponse)
                UpdateUserDefinedFunctionResponse.builder()
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    public DummyGlueClient() {
        databaseMap = new HashMap<>();
        tableMap = new HashMap<>();
        userDefinedFunctionMap = new HashMap<>();
        partitionMap = new HashMap<>();
    }

    @Override
    public String serviceName() {
        return "Glue";
    }

    @Override
    public void close() {}

    @Override
    public GlueServiceClientConfiguration serviceClientConfiguration() {
        return null;
    }

    @Override
    public CreateDatabaseResponse createDatabase(CreateDatabaseRequest createDatabaseRequest)
            throws InvalidInputException, AlreadyExistsException,
                    ResourceNumberLimitExceededException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException,
                    ConcurrentModificationException, AwsServiceException, SdkClientException,
                    GlueException {
        CreateDatabaseResponse.Builder responseBuilder = CreateDatabaseResponse.builder();

        if (databaseMap.containsKey(createDatabaseRequest.databaseInput().name())) {
            throw AlreadyExistsException.builder().build();
        }
        databaseMap.put(
                createDatabaseRequest.databaseInput().name(),
                GlueCatalogTestUtils.getDatabaseFromCreateDatabaseRequest(createDatabaseRequest));
        return (CreateDatabaseResponse)
                responseBuilder.sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }

    @Override
    public UpdateDatabaseResponse updateDatabase(UpdateDatabaseRequest updateDatabaseRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException,
                    ConcurrentModificationException, AwsServiceException, SdkClientException,
                    GlueException {

        if (!databaseMap.containsKey(updateDatabaseRequest.name())) {
            throw EntityNotFoundException.builder().build();
        }
        databaseMap.remove(updateDatabaseRequest.name());
        databaseMap.put(
                updateDatabaseRequest.name(),
                GlueCatalogTestUtils.getDatabaseFromUpdateDatabaseRequest(updateDatabaseRequest));

        return (UpdateDatabaseResponse)
                UpdateDatabaseResponse.builder().sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }

    @Override
    public GetDatabaseResponse getDatabase(GetDatabaseRequest getDatabaseRequest)
            throws InvalidInputException, EntityNotFoundException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {

        GetDatabaseResponse.Builder responseBuilder =
                (GetDatabaseResponse.Builder)
                        GetDatabaseResponse.builder().sdkHttpResponse(dummySdkHttpResponse(200));

        if (!databaseMap.containsKey(getDatabaseRequest.name())) {
            throw EntityNotFoundException.builder().build();
        }
        return responseBuilder.database(databaseMap.get(getDatabaseRequest.name())).build();
    }

    @Override
    public GetDatabasesResponse getDatabases(GetDatabasesRequest getDatabasesRequest)
            throws InvalidInputException, InternalServiceException, OperationTimeoutException,
                    GlueEncryptionException, AwsServiceException, SdkClientException,
                    GlueException {
        return (GetDatabasesResponse)
                GetDatabasesResponse.builder()
                        .databaseList(databaseMap.values())
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    @Override
    public DeleteDatabaseResponse deleteDatabase(DeleteDatabaseRequest deleteDatabaseRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, ConcurrentModificationException, AwsServiceException,
                    SdkClientException, GlueException {

        if (databaseMap.containsKey(deleteDatabaseRequest.name())) {
            databaseMap.remove(deleteDatabaseRequest.name());
            return (DeleteDatabaseResponse)
                    DeleteDatabaseResponse.builder()
                            .sdkHttpResponse(dummySdkHttpResponse(200))
                            .build();
        }
        throw EntityNotFoundException.builder().build();
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest createTableRequest)
            throws AlreadyExistsException, InvalidInputException, EntityNotFoundException,
                    ResourceNumberLimitExceededException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException,
                    ConcurrentModificationException, ResourceNotReadyException, AwsServiceException,
                    SdkClientException, GlueException {

        Table table = getTableFromCreateTableRequest(createTableRequest);
        String tableName =
                getFullyQualifiedName(
                        createTableRequest.databaseName(), createTableRequest.tableInput().name());
        if (tableMap.containsKey(tableName)) {
            throw AlreadyExistsException.builder().build();
        }

        tableMap.put(tableName, table);
        return (CreateTableResponse)
                CreateTableResponse.builder().sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }

    @Override
    public UpdateTableResponse updateTable(UpdateTableRequest updateTableRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, ConcurrentModificationException,
                    ResourceNumberLimitExceededException, GlueEncryptionException,
                    ResourceNotReadyException, AwsServiceException, SdkClientException,
                    GlueException {

        String tableName =
                getFullyQualifiedName(
                        updateTableRequest.databaseName(), updateTableRequest.tableInput().name());
        if (!databaseMap.containsKey(updateTableRequest.databaseName())
                || !tableMap.containsKey(tableName)) {
            throw EntityNotFoundException.builder().build();
        }

        tableMap.put(tableName, getTableFromUpdateTableRequest(updateTableRequest));
        return (UpdateTableResponse)
                UpdateTableResponse.builder().sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }

    @Override
    public GetTableResponse getTable(GetTableRequest getTableRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, ResourceNotReadyException,
                    AwsServiceException, SdkClientException, GlueException {

        String tableName =
                getFullyQualifiedName(getTableRequest.databaseName(), getTableRequest.name());

        if (!tableMap.containsKey(tableName)) {
            throw EntityNotFoundException.builder().build();
        }

        Table table = tableMap.get(tableName);
        return (GetTableResponse)
                GetTableResponse.builder()
                        .table(table)
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    @Override
    public GetTablesResponse getTables(GetTablesRequest getTablesRequest)
            throws EntityNotFoundException, InvalidInputException, OperationTimeoutException,
                    InternalServiceException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {
        String databaseName = getTablesRequest.databaseName();

        if (!databaseMap.containsKey(databaseName)) {
            throw EntityNotFoundException.builder().build();
        }

        List<Table> tables =
                tableMap.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(databaseName))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
        return (GetTablesResponse)
                GetTablesResponse.builder()
                        .tableList(tables)
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    @Override
    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, ConcurrentModificationException,
                    ResourceNotReadyException, AwsServiceException, SdkClientException,
                    GlueException {

        String tableName =
                getFullyQualifiedName(deleteTableRequest.databaseName(), deleteTableRequest.name());
        if (!databaseMap.containsKey(deleteTableRequest.databaseName())
                || !tableMap.containsKey(tableName)) {
            throw EntityNotFoundException.builder().build();
        }

        tableMap.remove(tableName);
        return (DeleteTableResponse)
                DeleteTableResponse.builder().sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }

    @Override
    public BatchDeleteTableResponse batchDeleteTable(
            BatchDeleteTableRequest batchDeleteTableRequest)
            throws InvalidInputException, EntityNotFoundException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, ResourceNotReadyException,
                    AwsServiceException, SdkClientException, GlueException {

        if (!databaseMap.containsKey(batchDeleteTableRequest.databaseName())) {
            throw EntityNotFoundException.builder().build();
        }
        for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
            if (entry.getKey().startsWith(batchDeleteTableRequest.databaseName())) {
                tableMap.remove(entry.getKey());
            }
        }
        return (BatchDeleteTableResponse)
                BatchDeleteTableResponse.builder()
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    // -- partition
    @Override
    public CreatePartitionResponse createPartition(CreatePartitionRequest createPartitionRequest)
            throws InvalidInputException, AlreadyExistsException,
                    ResourceNumberLimitExceededException, InternalServiceException,
                    EntityNotFoundException, OperationTimeoutException, GlueEncryptionException,
                    AwsServiceException, SdkClientException, GlueException {
        Partition partition = getPartitionFromCreatePartitionRequest(createPartitionRequest);
        String tableName =
                getFullyQualifiedName(
                        createPartitionRequest.databaseName(), createPartitionRequest.tableName());
        List<Partition> partitionList = partitionMap.getOrDefault(tableName, new ArrayList<>());
        String partValues = String.join(":", partition.values());
        for (Partition part : partitionList) {
            if (String.join(":", part.values()).equals(partValues)) {
                throw AlreadyExistsException.builder().build();
            }
        }

        partitionList.add(partition);
        partitionMap.put(tableName, partitionList);
        return (CreatePartitionResponse)
                CreatePartitionResponse.builder()
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    @Override
    public UpdatePartitionResponse updatePartition(UpdatePartitionRequest updatePartitionRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {

        String tableName =
                getFullyQualifiedName(
                        updatePartitionRequest.databaseName(), updatePartitionRequest.tableName());
        if (!partitionMap.containsKey(tableName)) {
            throw EntityNotFoundException.builder().build();
        }
        List<Partition> partitionList = partitionMap.get(tableName);
        String values = String.join(":", updatePartitionRequest.partitionInput().values());
        for (int i = 0; i < partitionList.size(); i++) {
            if (values.equals(String.join(":", partitionList.get(i).values()))) {
                partitionList.remove(i);
            }
        }
        partitionList.add(
                GlueCatalogTestUtils.getPartitionFromUpdatePartitionRequest(
                        updatePartitionRequest));
        partitionMap.put(tableName, partitionList);
        return (UpdatePartitionResponse)
                UpdatePartitionResponse.builder()
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    @Override
    public GetPartitionResponse getPartition(GetPartitionRequest getPartitionRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {
        String tableName =
                getFullyQualifiedName(
                        getPartitionRequest.databaseName(), getPartitionRequest.tableName());
        if (!partitionMap.containsKey(tableName)) {
            throw EntityNotFoundException.builder().build();
        }
        List<Partition> partitionList = partitionMap.get(tableName);
        String partitionValues = String.join(":", getPartitionRequest.partitionValues());
        for (Partition partition : partitionList) {
            if (partitionValues.equals(String.join(":", partition.values()))) {
                return (GetPartitionResponse)
                        GetPartitionResponse.builder()
                                .partition(partition)
                                .sdkHttpResponse(dummySdkHttpResponse(200))
                                .build();
            }
        }
        return (GetPartitionResponse)
                GetPartitionResponse.builder().sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }

    @Override
    public DeletePartitionResponse deletePartition(DeletePartitionRequest deletePartitionRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, AwsServiceException, SdkClientException,
                    GlueException {

        String tableName =
                getFullyQualifiedName(
                        deletePartitionRequest.databaseName(), deletePartitionRequest.tableName());

        if (!databaseMap.containsKey(deletePartitionRequest.databaseName())
                || !tableMap.containsKey(tableName)
                || !partitionMap.containsKey(tableName)) {
            throw EntityNotFoundException.builder().build();
        }

        List<Partition> partitions = partitionMap.get(tableName);
        int pos = 0;
        for (Partition partition : partitions) {
            if (matchValues(partition.values(), deletePartitionRequest.partitionValues())) {
                break;
            }
            pos++;
        }
        if (pos < partitions.size()) {
            partitions.remove(pos);
            partitionMap.remove(tableName);
            partitionMap.put(tableName, partitions);
        }
        return (DeletePartitionResponse)
                DeletePartitionResponse.builder()
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    private boolean matchValues(List<String> gluePartValues, List<String> partValues) {
        Set<String> gluePartitionValueSet = new HashSet<>(gluePartValues);
        int count = 0;
        for (String partVal : partValues) {
            if (gluePartitionValueSet.contains(partVal)) {
                count++;
            }
        }

        return count == partValues.size();
    }

    @Override
    public GetPartitionsResponse getPartitions(GetPartitionsRequest getPartitionsRequest)
            throws EntityNotFoundException, InvalidInputException, OperationTimeoutException,
                    InternalServiceException, GlueEncryptionException, InvalidStateException,
                    ResourceNotReadyException, AwsServiceException, SdkClientException,
                    GlueException {

        String tableName =
                getFullyQualifiedName(
                        getPartitionsRequest.databaseName(), getPartitionsRequest.tableName());
        if (!databaseMap.containsKey(getPartitionsRequest.databaseName())
                || !tableMap.containsKey(tableName)) {
            throw EntityNotFoundException.builder().build();
        }

        return (GetPartitionsResponse)
                GetPartitionsResponse.builder()
                        .partitions(partitionMap.getOrDefault(tableName, new ArrayList<>()))
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    // -- functions
    @Override
    public CreateUserDefinedFunctionResponse createUserDefinedFunction(
            CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest)
            throws AlreadyExistsException, InvalidInputException, InternalServiceException,
                    EntityNotFoundException, OperationTimeoutException,
                    ResourceNumberLimitExceededException, GlueEncryptionException,
                    AwsServiceException, SdkClientException, GlueException {
        if (!databaseMap.containsKey(createUserDefinedFunctionRequest.databaseName())) {
            throw EntityNotFoundException.builder().build();
        }
        String functionName =
                getFullyQualifiedName(
                        createUserDefinedFunctionRequest.databaseName(),
                        createUserDefinedFunctionRequest.functionInput().functionName());
        if (userDefinedFunctionMap.containsKey(functionName)) {
            throw AlreadyExistsException.builder().build();
        }
        UserDefinedFunction udf =
                getUDFFromCreateUserDefinedFunctionRequest(createUserDefinedFunctionRequest);
        userDefinedFunctionMap.put(functionName, udf);
        return (CreateUserDefinedFunctionResponse)
                CreateUserDefinedFunctionResponse.builder()
                        .sdkHttpResponse(dummySdkHttpResponse(200))
                        .build();
    }

    @Override
    public GetUserDefinedFunctionResponse getUserDefinedFunction(
            GetUserDefinedFunctionRequest getUserDefinedFunctionRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {
        if (!databaseMap.containsKey(getUserDefinedFunctionRequest.databaseName())) {
            throw EntityNotFoundException.builder().build();
        }
        String functionName =
                getFullyQualifiedName(
                        getUserDefinedFunctionRequest.databaseName(),
                        getUserDefinedFunctionRequest.functionName());
        GetUserDefinedFunctionResponse.Builder response = GetUserDefinedFunctionResponse.builder();
        if (userDefinedFunctionMap.containsKey(functionName)) {
            response.userDefinedFunction(userDefinedFunctionMap.get(functionName));
        }

        return (GetUserDefinedFunctionResponse)
                response.sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }

    @Override
    public GetUserDefinedFunctionsResponse getUserDefinedFunctions(
            GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest)
            throws EntityNotFoundException, InvalidInputException, OperationTimeoutException,
                    InternalServiceException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {

        GetUserDefinedFunctionsResponse.Builder response =
                (GetUserDefinedFunctionsResponse.Builder)
                        GetUserDefinedFunctionsResponse.builder()
                                .sdkHttpResponse(dummySdkHttpResponse(200));
        if (!databaseMap.containsKey(getUserDefinedFunctionsRequest.databaseName())) {
            throw EntityNotFoundException.builder().build();
        }

        List<UserDefinedFunction> udfs =
                userDefinedFunctionMap.entrySet().stream()
                        .filter(
                                e ->
                                        e.getKey()
                                                .startsWith(
                                                        getUserDefinedFunctionsRequest
                                                                .databaseName()))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
        return response.userDefinedFunctions(udfs).build();
    }

    @Override
    public DeleteUserDefinedFunctionResponse deleteUserDefinedFunction(
            DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, AwsServiceException, SdkClientException,
                    GlueException {

        String functionName =
                getFullyQualifiedName(
                        deleteUserDefinedFunctionRequest.databaseName(),
                        deleteUserDefinedFunctionRequest.functionName());

        if (!databaseMap.containsKey(deleteUserDefinedFunctionRequest.databaseName())
                || !userDefinedFunctionMap.containsKey(functionName)) {
            throw EntityNotFoundException.builder().build();
        }

        DeleteUserDefinedFunctionResponse.Builder response =
                DeleteUserDefinedFunctionResponse.builder();
        userDefinedFunctionMap.remove(functionName);
        return (DeleteUserDefinedFunctionResponse)
                response.sdkHttpResponse(dummySdkHttpResponse(200)).build();
    }
}
