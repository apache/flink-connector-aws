package org.apache.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.Arrays;
import java.util.stream.Collectors;

public class GlueFunctionsUtil {

    public static String getCatalogFunctionClassName(final UserDefinedFunction udf) {
        String[] splitName = udf.className().split(GlueCatalogConstants.DEFAULT_SEPARATOR);
        return splitName[splitName.length - 1];
    }
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
