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

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link GlueFunctionsUtil}. */
class GlueFunctionsUtilTest {

    @Test
    void testLanguageDetectionForFlinkPrefixedClassNames() {
        assertThat(languageOf("flink:java:com.example.MyFunction"))
                .isEqualTo(FunctionLanguage.JAVA);
        assertThat(languageOf("flink:scala:com.example.MyFunction"))
                .isEqualTo(FunctionLanguage.SCALA);
        assertThat(languageOf("flink:python:my_module.my_function"))
                .isEqualTo(FunctionLanguage.PYTHON);
    }

    @Test
    void testUnprefixedClassNameFallsBackToJava() {
        // Functions created by other engines (Hive, Spark) have no Flink language prefix;
        // they must not fail the lookup.
        assertThat(languageOf("com.example.hive.HiveUdf")).isEqualTo(FunctionLanguage.JAVA);
    }

    @Test
    void testClassNameExtraction() {
        assertThat(
                        GlueFunctionsUtil.getCatalogFunctionClassName(
                                udf("flink:java:com.example.MyFunction")))
                .isEqualTo("com.example.MyFunction");
        // Foreign class names without a prefix are returned unchanged.
        assertThat(GlueFunctionsUtil.getCatalogFunctionClassName(udf("com.example.hive.HiveUdf")))
                .isEqualTo("com.example.hive.HiveUdf");
    }

    @Test
    void testGlueFunctionClassNameRoundTrip() {
        String glueClassName =
                GlueFunctionsUtil.getGlueFunctionClassName(
                        new CatalogFunctionImpl("com.example.MyFunction", FunctionLanguage.JAVA));
        assertThat(glueClassName).isEqualTo("flink:java:com.example.MyFunction");
        assertThat(languageOf(glueClassName)).isEqualTo(FunctionLanguage.JAVA);
        assertThat(GlueFunctionsUtil.getCatalogFunctionClassName(udf(glueClassName)))
                .isEqualTo("com.example.MyFunction");
    }

    private static FunctionLanguage languageOf(String className) {
        return GlueFunctionsUtil.getFunctionalLanguage(udf(className));
    }

    private static UserDefinedFunction udf(String className) {
        return UserDefinedFunction.builder().functionName("f").className(className).build();
    }
}
