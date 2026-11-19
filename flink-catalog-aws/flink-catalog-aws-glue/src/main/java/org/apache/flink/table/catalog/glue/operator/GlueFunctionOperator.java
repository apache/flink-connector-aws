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
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.PrincipalType;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles all user-defined-function operations for the Glue catalog. All paths passed to this class
 * are addressed by Glue storage names (the catalog resolves the Flink database name once, before
 * delegating here) and the function name is already normalized by the catalog.
 */
@Internal
public class GlueFunctionOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlueFunctionOperator.class);

    /**
     * Constructor to initialize the shared fields.
     *
     * @param glueClient The Glue client used for interacting with the AWS Glue service.
     * @param catalogName The catalog name associated with the Glue operations.
     */
    public GlueFunctionOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Creates a user-defined function in Glue.
     *
     * @param functionPath the function path (Glue database storage name, normalized function name)
     * @param function the Flink function definition
     * @throws FunctionAlreadyExistException if a function with this name already exists
     * @throws CatalogException on any other Glue error
     */
    public void createGlueFunction(ObjectPath functionPath, CatalogFunction function)
            throws FunctionAlreadyExistException {
        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, function);
        CreateUserDefinedFunctionRequest request =
                CreateUserDefinedFunctionRequest.builder()
                        .databaseName(functionPath.getDatabaseName())
                        .functionInput(functionInput)
                        .build();
        try {
            // The SDK throws a typed exception for any service error; reaching the next
            // statement means the call succeeded.
            glueClient.createUserDefinedFunction(request);
            LOG.info("Created function {}", functionPath.getFullName());
        } catch (AlreadyExistsException e) {
            throw new FunctionAlreadyExistException(catalogName, functionPath, e);
        } catch (EntityNotFoundException e) {
            throw new CatalogException(
                    "Database does not exist in Glue: " + functionPath.getDatabaseName(), e);
        } catch (InvalidInputException e) {
            throw new CatalogException(
                    String.format(
                            "Invalid function definition for %s: %s",
                            functionPath.getFullName(), e.getMessage()),
                    e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error creating function %s: %s",
                            functionPath.getFullName(), e.getMessage()),
                    e);
        }
    }

    /**
     * Replaces an existing user-defined function in Glue.
     *
     * @param functionPath the function path (Glue database storage name, normalized function name)
     * @param newFunction the new function definition
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException on any other Glue error
     */
    public void alterGlueFunction(ObjectPath functionPath, CatalogFunction newFunction)
            throws FunctionNotExistException {
        UserDefinedFunctionInput functionInput = createFunctionInput(functionPath, newFunction);
        UpdateUserDefinedFunctionRequest request =
                UpdateUserDefinedFunctionRequest.builder()
                        .functionName(functionPath.getObjectName())
                        .databaseName(functionPath.getDatabaseName())
                        .functionInput(functionInput)
                        .build();
        try {
            glueClient.updateUserDefinedFunction(request);
            LOG.info("Altered function {}", functionPath.getFullName());
        } catch (EntityNotFoundException e) {
            throw new FunctionNotExistException(catalogName, functionPath, e);
        } catch (InvalidInputException e) {
            throw new CatalogException(
                    String.format(
                            "Invalid function definition for %s: %s",
                            functionPath.getFullName(), e.getMessage()),
                    e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error altering function %s: %s",
                            functionPath.getFullName(), e.getMessage()),
                    e);
        }
    }

    /**
     * Fetches a user-defined function from Glue with a single {@code GetUserDefinedFunction} call.
     *
     * @param functionPath the function path (Glue database storage name, normalized function name)
     * @return the Glue function, or {@code null} if it does not exist
     * @throws CatalogException on any Glue error other than "not found"
     */
    public UserDefinedFunction getGlueFunctionOrNull(ObjectPath functionPath) {
        GetUserDefinedFunctionRequest request =
                GetUserDefinedFunctionRequest.builder()
                        .databaseName(functionPath.getDatabaseName())
                        .functionName(functionPath.getObjectName())
                        .build();
        try {
            return glueClient.getUserDefinedFunction(request).userDefinedFunction();
        } catch (EntityNotFoundException e) {
            LOG.debug("Function {} not found in Glue", functionPath.getFullName());
            return null;
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error getting function %s: %s",
                            functionPath.getFullName(), e.getMessage()),
                    e);
        }
    }

    /**
     * Gets a user-defined function from Glue and converts it to a Flink {@link CatalogFunction}.
     *
     * @param functionPath the function path (Glue database storage name, normalized function name)
     * @return the requested function
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException on any other Glue error, or if the function cannot be represented
     */
    public CatalogFunction getGlueFunction(ObjectPath functionPath)
            throws FunctionNotExistException {
        UserDefinedFunction udf = getGlueFunctionOrNull(functionPath);
        if (udf == null) {
            throw new FunctionNotExistException(catalogName, functionPath);
        }
        return toCatalogFunction(functionPath, udf);
    }

    /**
     * Checks whether a user-defined function exists.
     *
     * @param functionPath the function path (Glue database storage name, normalized function name)
     * @return true if the function exists
     * @throws CatalogException on any Glue error other than "not found"
     */
    public boolean glueFunctionExists(ObjectPath functionPath) {
        return getGlueFunctionOrNull(functionPath) != null;
    }

    /**
     * Lists the normalized names of all user-defined functions in a database, following pagination
     * via the SDK paginator.
     *
     * @param glueDatabaseName the Glue storage name of the database
     * @return the function names, normalized the same way the catalog normalizes lookups
     * @throws CatalogException on any Glue error
     */
    public List<String> listGlueFunctions(String glueDatabaseName) {
        try {
            List<String> functionNames = new ArrayList<>();
            for (GetUserDefinedFunctionsResponse page :
                    glueClient.getUserDefinedFunctionsPaginator(
                            GetUserDefinedFunctionsRequest.builder()
                                    .databaseName(glueDatabaseName)
                                    .build())) {
                for (UserDefinedFunction udf : page.userDefinedFunctions()) {
                    functionNames.add(FunctionIdentifier.normalizeName(udf.functionName()));
                }
            }
            return functionNames;
        } catch (EntityNotFoundException e) {
            throw new CatalogException("Database does not exist in Glue: " + glueDatabaseName, e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error listing functions in %s: %s", glueDatabaseName, e.getMessage()),
                    e);
        }
    }

    /**
     * Drops a user-defined function from Glue.
     *
     * @param functionPath the function path (Glue database storage name, normalized function name)
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException on any other Glue error
     */
    public void dropGlueFunction(ObjectPath functionPath) throws FunctionNotExistException {
        DeleteUserDefinedFunctionRequest request =
                DeleteUserDefinedFunctionRequest.builder()
                        .functionName(functionPath.getObjectName())
                        .databaseName(functionPath.getDatabaseName())
                        .build();
        try {
            glueClient.deleteUserDefinedFunction(request);
            LOG.info("Dropped function {}", functionPath.getFullName());
        } catch (EntityNotFoundException e) {
            throw new FunctionNotExistException(catalogName, functionPath, e);
        } catch (GlueException e) {
            throw new CatalogException(
                    String.format(
                            "Error dropping function %s: %s",
                            functionPath.getFullName(), e.getMessage()),
                    e);
        }
    }

    /**
     * Converts a Glue function into a Flink {@link CatalogFunction}.
     *
     * @param functionPath the function path, used for error messages
     * @param udf the Glue function
     * @return the Flink function
     * @throws CatalogException if a resource type is not supported by Flink
     */
    static CatalogFunction toCatalogFunction(ObjectPath functionPath, UserDefinedFunction udf) {
        List<ResourceUri> resourceUris = new ArrayList<>();
        if (udf.hasResourceUris()) {
            for (software.amazon.awssdk.services.glue.model.ResourceUri glueResource :
                    udf.resourceUris()) {
                // Use the raw string: a resource type added to Glue after this SDK version
                // would otherwise surface as UNKNOWN_TO_SDK_VERSION with an opaque failure.
                String resourceType = glueResource.resourceTypeAsString();
                ResourceType flinkType;
                try {
                    flinkType = ResourceType.valueOf(resourceType);
                } catch (IllegalArgumentException | NullPointerException e) {
                    throw new CatalogException(
                            String.format(
                                    "Function %s references a resource of type '%s' (%s) that "
                                            + "Flink does not support; supported types are JAR, "
                                            + "FILE and ARCHIVE",
                                    functionPath.getFullName(), resourceType, glueResource.uri()),
                            e);
                }
                resourceUris.add(new ResourceUri(flinkType, glueResource.uri()));
            }
        }
        return new CatalogFunctionImpl(
                GlueFunctionsUtil.getCatalogFunctionClassName(udf),
                GlueFunctionsUtil.getFunctionalLanguage(udf),
                resourceUris);
    }

    /**
     * Builds the Glue {@link UserDefinedFunctionInput} for a Flink function.
     *
     * @param functionPath the function path
     * @param function the Flink function
     * @return the Glue function input
     * @throws CatalogException if a resource type is not supported by Glue
     */
    public static UserDefinedFunctionInput createFunctionInput(
            final ObjectPath functionPath, final CatalogFunction function) {
        List<software.amazon.awssdk.services.glue.model.ResourceUri> resourceUris =
                new ArrayList<>();
        for (ResourceUri resourceUri : function.getFunctionResources()) {
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
                    throw new CatalogException(
                            String.format(
                                    "Function %s uses resource type %s; AWS Glue supports only "
                                            + "JAR, FILE and ARCHIVE resources",
                                    functionPath.getFullName(), resourceUri.getResourceType()));
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
