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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.DATABASE_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_1;

/** Test methods in GlueUtils . */
public class GlueUtilsTest {

    private static final String WAREHOUSE_PATH = "s3://bucket";

    @Test
    public void testGetGlueConventionalName() {
        String name = "MyName";
        Assertions.assertEquals("myname", GlueUtils.getGlueConventionalName(name));
        String name1 = "Mtx@ndfv";
        Assertions.assertThrows(IllegalArgumentException.class, () -> GlueUtils.validate(name1));
    }

    @Test
    public void testExtractDatabaseLocation() {
        HashMap<String, String> propertiesWithLocationUri =
                new HashMap<String, String>() {
                    {
                        put(GlueCatalogConstants.LOCATION_URI, "s3://some-path/myDb/");
                        put("k1", "v1");
                    }
                };

        String location =
                GlueUtils.extractDatabaseLocation(
                        propertiesWithLocationUri, DATABASE_1, WAREHOUSE_PATH);
        Assertions.assertEquals("s3://some-path/myDb/", location);

        String newLocation =
                GlueUtils.extractDatabaseLocation(
                        propertiesWithLocationUri, DATABASE_1, WAREHOUSE_PATH);
        Assertions.assertNotEquals("s3://some-path/myDb/", newLocation);
        Assertions.assertEquals(
                WAREHOUSE_PATH + GlueCatalogConstants.LOCATION_SEPARATOR + DATABASE_1, newLocation);
        newLocation =
                GlueUtils.extractDatabaseLocation(new HashMap<>(), DATABASE_1, WAREHOUSE_PATH);
        Assertions.assertEquals(
                newLocation, WAREHOUSE_PATH + GlueCatalogConstants.LOCATION_SEPARATOR + DATABASE_1);
    }

    @Test
    public void testExtractTableLocation() {
        Map<String, String> propertiesWithLocationUri =
                new HashMap<String, String>() {
                    {
                        put(GlueCatalogConstants.LOCATION_URI, "s3://some-path/myDb/myTable/");
                        put("k1", "v1");
                    }
                };
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        String location =
                GlueUtils.extractTableLocation(
                        propertiesWithLocationUri, tablePath, WAREHOUSE_PATH);
        Assertions.assertEquals("s3://some-path/myDb/myTable/", location);

        String newLocation =
                GlueUtils.extractTableLocation(
                        propertiesWithLocationUri, tablePath, WAREHOUSE_PATH);
        Assertions.assertNotEquals("s3://some-path/myDb/myTable", newLocation);
        Assertions.assertEquals(
                WAREHOUSE_PATH
                        + GlueCatalogConstants.LOCATION_SEPARATOR
                        + DATABASE_1
                        + GlueCatalogConstants.LOCATION_SEPARATOR
                        + TABLE_1,
                newLocation);
    }

    @Test
    public void testGetCatalogDatabase() {
        Map<String, String> params =
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                        put("k2", "v2");
                    }
                };
        String description = "Test description";
        Database database = Database.builder().parameters(params).description(description).build();
        CatalogDatabase catalogDatabase = GlueUtils.getCatalogDatabase(database);
        Assertions.assertInstanceOf(CatalogDatabase.class, catalogDatabase);
        Assertions.assertEquals(catalogDatabase.getProperties(), params);
        Assertions.assertEquals(catalogDatabase.getDescription().orElse(null), description);
    }

    @Test
    public void testGetCatalogFunctionClassName() {
        UserDefinedFunction.Builder udfBuilder =
                UserDefinedFunction.builder().functionName("Dummy").databaseName(DATABASE_1);
        UserDefinedFunction udf1 = udfBuilder.className("org.test.Class").build();
        Assertions.assertThrows(
                org.apache.flink.table.api.ValidationException.class,
                () -> GlueUtils.getCatalogFunctionClassName(udf1));
        String className = GlueUtils.getGlueFunctionClassName(new CatalogFunctionImpl("TestClass"));
        UserDefinedFunction udf2 = udfBuilder.className(className).build();
        Assertions.assertDoesNotThrow(() -> GlueUtils.getCatalogFunctionClassName(udf2));
    }

    @Test
    public void testGetFunctionalLanguage() {
        UserDefinedFunction.Builder udfBuilder =
                UserDefinedFunction.builder().functionName("Dummy").databaseName(DATABASE_1);
        Assertions.assertThrows(
                CatalogException.class,
                () ->
                        GlueUtils.getFunctionalLanguage(
                                udfBuilder.className("org.test.Class").build()));
        String className = GlueUtils.getGlueFunctionClassName(new CatalogFunctionImpl("TestClass"));
        UserDefinedFunction udf1 = udfBuilder.className(className).build();
        FunctionLanguage functionLanguage = GlueUtils.getFunctionalLanguage(udf1);
        Assertions.assertEquals(functionLanguage, FunctionLanguage.JAVA);
    }

    @Test
    public void testExtractTableOwner() {
        Map<String, String> properties =
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                        put("k2", "v2");
                    }
                };

        Assertions.assertNull(GlueUtils.extractTableOwner(properties));
        properties.put(GlueCatalogConstants.TABLE_OWNER, "testOwner");
        Assertions.assertEquals(GlueUtils.extractTableOwner(properties), "testOwner");
    }

    @Test
    public void testExpressionString() {
        Expression expression = ResolvedExpressionMock.of(DataTypes.INT(), "column1");
        Assertions.assertEquals("column1", GlueUtils.getExpressionString(expression));
    }
}
