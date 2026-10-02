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

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Unit tests for {@link GlueFunctionOperator}: the function CRUD lifecycle, the Flink-to-Glue
 * function encoding, and the translation of Glue errors into Flink catalog exceptions. Runs against
 * the in-memory fake by default and against real AWS Glue when credentials are supplied (see {@link
 * GlueTestClientFactory}).
 */
@ExtendWith(RealGlueCleanupExtension.class)
class GlueFunctionOperatorTest {

    private static final String CATALOG_NAME = "testcatalog";

    private GlueClient glueClient;
    private GlueFunctionOperator functionOperator;
    private String databaseName;

    @BeforeEach
    void setUp() {
        glueClient = GlueTestClientFactory.createClient();
        functionOperator = new GlueFunctionOperator(glueClient, CATALOG_NAME);
        databaseName = GlueTestClientFactory.uniqueName("funcdb");
        glueClient.createDatabase(b -> b.databaseInput(db -> db.name(databaseName)));
    }

    @AfterEach
    void tearDown() {
        glueClient.close();
    }

    @Test
    void testCreateGetListAndDropFunction() throws Exception {
        ObjectPath path = new ObjectPath(databaseName, "my_upper");
        CatalogFunction function =
                new CatalogFunctionImpl(
                        "com.example.MyUpper",
                        FunctionLanguage.JAVA,
                        Arrays.asList(
                                new ResourceUri(ResourceType.JAR, "s3://bucket/udfs/my-upper.jar"),
                                new ResourceUri(ResourceType.FILE, "s3://bucket/udfs/dict.txt")));

        assertThat(functionOperator.glueFunctionExists(path)).isFalse();
        functionOperator.createGlueFunction(path, function);
        assertThat(functionOperator.glueFunctionExists(path)).isTrue();

        CatalogFunction readBack = functionOperator.getGlueFunction(path);
        assertThat(readBack.getClassName()).isEqualTo("com.example.MyUpper");
        assertThat(readBack.getFunctionLanguage()).isEqualTo(FunctionLanguage.JAVA);
        assertThat(readBack.getFunctionResources())
                .containsExactlyInAnyOrderElementsOf(function.getFunctionResources());

        // The stored Glue class name carries the Flink language prefix.
        UserDefinedFunction stored = functionOperator.getGlueFunctionOrNull(path);
        assertThat(stored.className()).isEqualTo("flink:java:com.example.MyUpper");

        assertThat(functionOperator.listGlueFunctions(databaseName)).containsExactly("my_upper");

        functionOperator.dropGlueFunction(path);
        assertThat(functionOperator.glueFunctionExists(path)).isFalse();
        assertThat(functionOperator.listGlueFunctions(databaseName)).isEmpty();
    }

    @Test
    void testLanguagesRoundTrip() throws Exception {
        ObjectPath scala = new ObjectPath(databaseName, "scala_fn");
        ObjectPath python = new ObjectPath(databaseName, "python_fn");
        functionOperator.createGlueFunction(
                scala, new CatalogFunctionImpl("com.example.ScalaFn", FunctionLanguage.SCALA));
        functionOperator.createGlueFunction(
                python, new CatalogFunctionImpl("pkg.module.python_fn", FunctionLanguage.PYTHON));

        assertThat(functionOperator.getGlueFunction(scala).getFunctionLanguage())
                .isEqualTo(FunctionLanguage.SCALA);
        assertThat(functionOperator.getGlueFunction(scala).getClassName())
                .isEqualTo("com.example.ScalaFn");
        assertThat(functionOperator.getGlueFunction(python).getFunctionLanguage())
                .isEqualTo(FunctionLanguage.PYTHON);
        assertThat(functionOperator.getGlueFunction(python).getClassName())
                .isEqualTo("pkg.module.python_fn");
    }

    @Test
    void testAlterFunctionReplacesDefinition() throws Exception {
        ObjectPath path = new ObjectPath(databaseName, "versioned");
        functionOperator.createGlueFunction(
                path, new CatalogFunctionImpl("com.example.V1", FunctionLanguage.JAVA));

        functionOperator.alterGlueFunction(
                path,
                new CatalogFunctionImpl(
                        "com.example.V2",
                        FunctionLanguage.JAVA,
                        Collections.singletonList(
                                new ResourceUri(ResourceType.JAR, "s3://bucket/v2.jar"))));

        CatalogFunction readBack = functionOperator.getGlueFunction(path);
        assertThat(readBack.getClassName()).isEqualTo("com.example.V2");
        assertThat(readBack.getFunctionResources())
                .extracting(ResourceUri::getUri)
                .containsExactly("s3://bucket/v2.jar");
    }

    @Test
    void testCreateExistingFunctionThrowsFunctionAlreadyExist() throws Exception {
        ObjectPath path = new ObjectPath(databaseName, "dup");
        CatalogFunction function =
                new CatalogFunctionImpl("com.example.Dup", FunctionLanguage.JAVA);
        functionOperator.createGlueFunction(path, function);

        assertThatThrownBy(() -> functionOperator.createGlueFunction(path, function))
                .isInstanceOf(FunctionAlreadyExistException.class);
    }

    @Test
    void testMissingFunctionThrowsFunctionNotExist() {
        ObjectPath missing = new ObjectPath(databaseName, "missing");
        CatalogFunction function =
                new CatalogFunctionImpl("com.example.Missing", FunctionLanguage.JAVA);

        assertThat(functionOperator.getGlueFunctionOrNull(missing)).isNull();
        assertThatThrownBy(() -> functionOperator.getGlueFunction(missing))
                .isInstanceOf(FunctionNotExistException.class);
        assertThatThrownBy(() -> functionOperator.alterGlueFunction(missing, function))
                .isInstanceOf(FunctionNotExistException.class);
        assertThatThrownBy(() -> functionOperator.dropGlueFunction(missing))
                .isInstanceOf(FunctionNotExistException.class);
    }

    @Test
    void testCreateFunctionInMissingDatabaseFailsClearly() {
        ObjectPath path = new ObjectPath("no_such_database_" + databaseName, "fn");
        // The fake is lenient about unknown databases; real Glue is the authority here.
        assumeThat(GlueTestClientFactory.REAL_GLUE).isTrue();

        assertThatThrownBy(
                        () ->
                                functionOperator.createGlueFunction(
                                        path,
                                        new CatalogFunctionImpl(
                                                "com.example.Fn", FunctionLanguage.JAVA)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Database does not exist in Glue");
    }

    @Test
    void testCreateFunctionInputEncodesLanguageAndResources() {
        ObjectPath path = new ObjectPath(databaseName, "encoded");
        UserDefinedFunctionInput input =
                GlueFunctionOperator.createFunctionInput(
                        path,
                        new CatalogFunctionImpl(
                                "com.example.Encoded",
                                FunctionLanguage.SCALA,
                                Collections.singletonList(
                                        new ResourceUri(
                                                ResourceType.ARCHIVE, "s3://bucket/a.zip"))));

        assertThat(input.functionName()).isEqualTo("encoded");
        assertThat(input.className()).isEqualTo("flink:scala:com.example.Encoded");
        assertThat(input.resourceUris()).hasSize(1);
        assertThat(input.resourceUris().get(0).resourceTypeAsString()).isEqualTo("ARCHIVE");
        assertThat(input.resourceUris().get(0).uri()).isEqualTo("s3://bucket/a.zip");
    }

    @Test
    void testInvalidInputIsTranslatedWithFunctionName() {
        ObjectPath path = new ObjectPath(databaseName, "invalid");
        fakeClient()
                .setNextException(
                        InvalidInputException.builder().message("class name too long").build());
        assertThatThrownBy(
                        () ->
                                functionOperator.createGlueFunction(
                                        path,
                                        new CatalogFunctionImpl(
                                                "com.example.Invalid", FunctionLanguage.JAVA)))
                .isInstanceOf(CatalogException.class)
                .hasMessage(
                        "Invalid function definition for "
                                + path.getFullName()
                                + ": class name too long")
                .hasCauseInstanceOf(InvalidInputException.class);
    }

    @Test
    void testTimeoutOnLookupIsNotReportedAsMissingFunction() {
        ObjectPath path = new ObjectPath(databaseName, "slow");
        fakeClient()
                .setNextException(
                        OperationTimeoutException.builder().message("Operation timed out").build());
        assertThatThrownBy(() -> functionOperator.glueFunctionExists(path))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Error getting function " + path.getFullName())
                .hasCauseInstanceOf(OperationTimeoutException.class);
    }

    private FakeGlueClient fakeClient() {
        assumeThat(glueClient)
                .as("Fault-injection tests require the in-memory FakeGlueClient")
                .isInstanceOf(FakeGlueClient.class);
        return (FakeGlueClient) glueClient;
    }
}
