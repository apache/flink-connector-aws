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
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.glue.util.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueFunctionsUtil;
import org.apache.flink.table.resource.ResourceUri;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.PrincipalType;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities for Glue catalog Function related operations. */
@Internal
public class GlueFunctionOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlueFunctionOperator.class);

    /**
     * Constructor to initialize the shared fields.
     *
     * @param glueClient  The Glue client used for interacting with the AWS Glue service.
     * @param catalogName The catalog name associated with the Glue operations.
     */
    public GlueFunctionOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Create a function. Function name should be handled in a case-insensitive way.
     *
     * @param functionPath path of the function
     * @param function Flink function to be created
     * @throws CatalogException in case of any runtime exception
     */
    public void createGlueFunction(ObjectPath functionPath, CatalogFunction function)
            throws CatalogException, FunctionAlreadyExistException {
        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, function);
        CreateUserDefinedFunctionRequest.Builder createUDFRequest =
                CreateUserDefinedFunctionRequest.builder()
                        .databaseName(functionPath.getDatabaseName())
                        .functionInput(functionInput);
        try {
            CreateUserDefinedFunctionResponse response =
                    glueClient.createUserDefinedFunction(createUDFRequest.build());
            if (response == null || (response.sdkHttpResponse() != null && !response.sdkHttpResponse().isSuccessful())) {
                throw new CatalogException("Error creating function: " + functionPath.getFullName());
            }
            LOG.info("Created Function: {}", functionPath.getFullName());
        } catch (AlreadyExistsException e) {
            LOG.error(
                    String.format(
                            "%s already Exists. Function language of type: %s. \n%s",
                            functionPath.getFullName(), function.getFunctionLanguage(), e));
            throw new FunctionAlreadyExistException(catalogName, functionPath, e);
        } catch (GlueException e) {
            LOG.error("Error creating glue function: {}\n{}", functionPath.getFullName(), e);
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Modify an existing function. Function name should be handled in a case-insensitive way.
     *
     * @param functionPath path of function.
     * @param newFunction modified function.
     * @throws CatalogException on runtime errors.
     * @throws FunctionNotExistException if the function doesn't exist.
     */
    public void alterGlueFunction(ObjectPath functionPath, CatalogFunction newFunction)
            throws CatalogException, FunctionNotExistException {

        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, newFunction);

        UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest =
                UpdateUserDefinedFunctionRequest.builder()
                        .functionName(functionPath.getObjectName())
                        .databaseName(functionPath.getDatabaseName())
                        .functionInput(functionInput)
                        .build();
        try {
            UpdateUserDefinedFunctionResponse response =
                    glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
            if (response == null || (response.sdkHttpResponse() != null && !response.sdkHttpResponse().isSuccessful())) {
                throw new CatalogException("Error altering function: " + functionPath.getFullName());
            }
            LOG.info("Altered Function: {}", functionPath.getFullName());
        } catch (EntityNotFoundException e) {
            LOG.error("Function not found: {}", functionPath.getFullName());
            throw new FunctionNotExistException(catalogName, functionPath, e);
        } catch (GlueException e) {
            LOG.error("Error altering glue function: {}\n{}", functionPath.getFullName(), e);
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Get the user defined function from glue Catalog. Function name should be handled in a
     * case-insensitive way.
     *
     * @param functionPath path of the function
     * @return the requested function
     * @throws CatalogException in case of any runtime exception
     * @throws FunctionNotExistException if the function doesn't exist
     */
    public CatalogFunction getGlueFunction(ObjectPath functionPath) throws CatalogException, FunctionNotExistException {
        GetUserDefinedFunctionRequest request =
                GetUserDefinedFunctionRequest.builder()
                        .databaseName(functionPath.getDatabaseName())
                        .functionName(functionPath.getObjectName())
                        .build();
        try {
            GetUserDefinedFunctionResponse response = glueClient.getUserDefinedFunction(request);
            UserDefinedFunction udf = response.userDefinedFunction();
            List<ResourceUri> resourceUris =
                    udf.resourceUris().stream()
                            .map(
                                    resourceUri ->
                                            new org.apache.flink.table.resource.ResourceUri(
                                                    org.apache.flink.table.resource.ResourceType
                                                            .valueOf(resourceUri.resourceType().name()),
                                                    resourceUri.uri()))
                            .collect(Collectors.toList());
            return new CatalogFunctionImpl(
                    GlueFunctionsUtil.getCatalogFunctionClassName(udf),
                    GlueFunctionsUtil.getFunctionalLanguage(udf),
                    resourceUris);
        } catch (EntityNotFoundException e) {
            LOG.error("Function not found: {}", functionPath.getFullName());
            throw new FunctionNotExistException(catalogName, functionPath, e);
        } catch (GlueException e) {
            LOG.error("Error fetching function {}: {}", functionPath.getFullName(), e);
            throw new CatalogException(
                String.format("Error getting function %s: %s", functionPath.getFullName(), e.getMessage()), e);
        }
    }

    public List<String> listGlueFunctions(String databaseName) {
        GetUserDefinedFunctionsRequest.Builder functionsRequest =
                GetUserDefinedFunctionsRequest.builder()
                        .databaseName(databaseName);
        List<String> glueFunctions;
        try {
            GetUserDefinedFunctionsResponse functionsResponse =
                    glueClient.getUserDefinedFunctions(functionsRequest.build());
            String token = functionsResponse.nextToken();
            glueFunctions =
                    functionsResponse.userDefinedFunctions().stream()
                            .map(UserDefinedFunction::functionName)
                            .collect(Collectors.toCollection(LinkedList::new));
            while (Optional.ofNullable(token).isPresent()) {
                functionsRequest.nextToken(token);
                functionsResponse = glueClient.getUserDefinedFunctions(functionsRequest.build());
                glueFunctions.addAll(
                        functionsResponse.userDefinedFunctions().stream()
                                .map(UserDefinedFunction::functionName)
                                .collect(Collectors.toCollection(LinkedList::new)));
                token = functionsResponse.nextToken();
            }
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
        return glueFunctions;
    }

    public boolean glueFunctionExists(ObjectPath functionPath) {
        GetUserDefinedFunctionRequest request =
                GetUserDefinedFunctionRequest.builder()
                        .functionName(functionPath.getObjectName())
                        .databaseName(functionPath.getDatabaseName())
                        .build();

        try {
            GetUserDefinedFunctionResponse response = glueClient.getUserDefinedFunction(request);
            return response.userDefinedFunction() != null;
        } catch (EntityNotFoundException e) {
            return false;
        } catch (GlueException e) {
            LOG.error(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Drop / Delete UserDefinedFunction from glue data catalog.
     *
     * @param functionPath fully qualified function path
     * @throws CatalogException In case of Unexpected errors.
     * @throws FunctionNotExistException if the function does not exist.
     */
    public void dropGlueFunction(ObjectPath functionPath) throws CatalogException, FunctionNotExistException {
        DeleteUserDefinedFunctionRequest request =
                DeleteUserDefinedFunctionRequest.builder()
                        .functionName(functionPath.getObjectName())
                        .databaseName(functionPath.getDatabaseName())
                        .build();
        try {
            DeleteUserDefinedFunctionResponse response = glueClient.deleteUserDefinedFunction(request);
            if (response == null || (response.sdkHttpResponse() != null && !response.sdkHttpResponse().isSuccessful())) {
                throw new CatalogException("Error dropping function: " + functionPath.getFullName());
            }
            LOG.info("Dropped Function: {}", functionPath.getFullName());
        } catch (EntityNotFoundException e) {
            throw new FunctionNotExistException(catalogName, functionPath, e);
        } catch (GlueException e) {
            LOG.error("Error dropping glue function: {}\n{}", functionPath.getFullName(), e);
            throw new CatalogException(
                String.format("Error dropping function %s: %s", functionPath.getFullName(), e.getMessage()), e);
        }
    }

    /**
     * Utility method to Create UserDefinedFunctionInput instance.
     *
     * @param functionPath fully qualified for function path.
     * @param function Catalog Function instance.
     * @return User defined function input instance for Glue.
     * @throws UnsupportedOperationException in case of unsupported operation encountered.
     */
    public static UserDefinedFunctionInput createFunctionInput(
            final ObjectPath functionPath, final CatalogFunction function)
            throws UnsupportedOperationException {
        Collection<software.amazon.awssdk.services.glue.model.ResourceUri> resourceUris =
                new LinkedList<>();
        for (org.apache.flink.table.resource.ResourceUri resourceUri :
                function.getFunctionResources()) {
            switch (resourceUri.getResourceType()) {
                case JAR:
                case FILE:
                case ARCHIVE:
                    resourceUris.add(
                            software.amazon.awssdk.services.glue.model.ResourceUri.builder()
                                    .resourceType(resourceUri.getResourceType().name())
                                    .uri(resourceUri.getUri())
                                    .build());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "GlueCatalog supports only creating resources JAR/FILE or ARCHIVE.");
            }
        }
        return UserDefinedFunctionInput.builder()
                .functionName(functionPath.getObjectName())
                .className(GlueFunctionsUtil.getGlueFunctionClassName(function))
                .ownerType(PrincipalType.USER)
                .ownerName(GlueCatalogConstants.FLINK_CATALOG)
                .resourceUris(resourceUris)
                .build();
    }
}
