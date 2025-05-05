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

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Utility class for handling Functions in AWS Glue Catalog integration.
 * Provides methods for converting between Flink and Glue function representation.
 */
public class GlueFunctionsUtil {

    /**
     * Extracts the class name from a Glue UserDefinedFunction.
     *
     * @param udf The Glue UserDefinedFunction
     * @return The extracted class name
     */
    public static String getCatalogFunctionClassName(final UserDefinedFunction udf) {
        String[] splitName = udf.className().split(GlueCatalogConstants.DEFAULT_SEPARATOR);
        return splitName[splitName.length - 1];
    }

    /**
     * Determines the function language from a Glue UserDefinedFunction.
     *
     * @param glueFunction The Glue UserDefinedFunction
     * @return The corresponding Flink FunctionLanguage
     * @throws CatalogException if the function language cannot be determined
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
            throw new CatalogException(
                    "Invalid Functional Language for className: " + glueFunction.className());
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
                throw new UnsupportedOperationException(
                        "GlueCatalog supports only creating: "
                                + Arrays.stream(FunctionLanguage.values())
                                .map(FunctionLanguage::name)
                                .collect(
                                        Collectors.joining(
                                                GlueCatalogConstants.NEXT_LINE)));
        }
    }
}
