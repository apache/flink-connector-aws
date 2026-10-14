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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionLanguage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

/**
 * Utility class for handling Functions in AWS Glue Catalog integration. Provides methods for
 * converting between Flink and Glue function representation.
 */
@Internal
public class GlueFunctionsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(GlueFunctionsUtil.class);

    /**
     * Extracts the Flink class name from a Glue UserDefinedFunction by stripping the {@code
     * flink:<language>:} prefix this catalog writes. Class names created by other engines carry no
     * such prefix and are returned unchanged.
     *
     * @param udf The Glue UserDefinedFunction
     * @return The Flink class name
     */
    public static String getCatalogFunctionClassName(final UserDefinedFunction udf) {
        String className = udf.className();
        for (String prefix :
                new String[] {
                    GlueCatalogConstants.FLINK_JAVA_FUNCTION_PREFIX,
                    GlueCatalogConstants.FLINK_PYTHON_FUNCTION_PREFIX,
                    GlueCatalogConstants.FLINK_SCALA_FUNCTION_PREFIX
                }) {
            if (className.startsWith(prefix)) {
                return className.substring(prefix.length());
            }
        }
        return className;
    }

    /**
     * Determines the function language from a Glue UserDefinedFunction.
     *
     * @param glueFunction The Glue UserDefinedFunction
     * @return The corresponding Flink FunctionLanguage; class names without a Flink language prefix
     *     (e.g. functions created by Hive or Spark) are treated as JAVA
     */
    public static FunctionLanguage getFunctionalLanguage(final UserDefinedFunction glueFunction) {
        if (glueFunction.className().startsWith(GlueCatalogConstants.FLINK_JAVA_FUNCTION_PREFIX)) {
            return FunctionLanguage.JAVA;
        } else if (glueFunction
                .className()
                .startsWith(GlueCatalogConstants.FLINK_PYTHON_FUNCTION_PREFIX)) {
            return FunctionLanguage.PYTHON;
        } else if (glueFunction
                .className()
                .startsWith(GlueCatalogConstants.FLINK_SCALA_FUNCTION_PREFIX)) {
            return FunctionLanguage.SCALA;
        } else {
            // Functions created by other engines (e.g. Hive or Spark) store their class name
            // without a Flink language prefix. Treat them as JAVA functions, which is the
            // representation both engines use, instead of failing the whole lookup.
            LOG.warn(
                    "Function class name '{}' has no Flink language prefix; assuming JAVA. "
                            + "Functions created by other engines may not be loadable by Flink.",
                    glueFunction.className());
            return FunctionLanguage.JAVA;
        }
    }

    /**
     * Creates a Glue function class name from a Flink CatalogFunction.
     *
     * @param function The Flink CatalogFunction
     * @return The formatted function class name for Glue
     * @throws UnsupportedOperationException if the function language is not supported
     */
    public static String getGlueFunctionClassName(CatalogFunction function) {
        switch (function.getFunctionLanguage()) {
            case JAVA:
                return GlueCatalogConstants.FLINK_JAVA_FUNCTION_PREFIX + function.getClassName();
            case SCALA:
                return GlueCatalogConstants.FLINK_SCALA_FUNCTION_PREFIX + function.getClassName();
            case PYTHON:
                return GlueCatalogConstants.FLINK_PYTHON_FUNCTION_PREFIX + function.getClassName();
            default:
                // Unreachable for the current FunctionLanguage values; guards against a new
                // language being added to Flink without a Glue prefix being defined here.
                throw new UnsupportedOperationException(
                        "GlueCatalog does not support function language "
                                + function.getFunctionLanguage());
        }
    }
}
