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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.glue.GlueCatalogOptions;
import org.apache.flink.table.catalog.glue.TypeMapper;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.AbstractDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GlueResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.BooleanUtils.FALSE;
import static org.apache.commons.lang3.BooleanUtils.TRUE;
import static org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants.EXPLAIN_EXTRAS;
import static org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants.IS_PERSISTED;
import static org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants.IS_PHYSICAL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Utilities related glue Operation. */
@Internal
public class GlueUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GlueUtils.class);

    /**
     * Glue supports lowercase naming convention.
     *
     * @param name fully qualified name.
     * @return modified name according to glue convention.
     */
    public static String getGlueConventionalName(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    /**
     * Extract database location from properties and remove location from properties. fallback to
     * create default location if not present
     *
     * @param databaseProperties database properties.
     * @param databaseName fully qualified name for database.
     * @param catalogPath catalog path.
     * @return location for database.
     */
    public static String extractDatabaseLocation(
            final Map<String, String> databaseProperties,
            final String databaseName,
            final String catalogPath) {
        if (databaseProperties.containsKey(GlueCatalogConstants.LOCATION_URI)) {
            return databaseProperties.remove(GlueCatalogConstants.LOCATION_URI);
        } else {
            LOG.info("No location URI Set. Using Catalog Path as default");
            return catalogPath + GlueCatalogConstants.LOCATION_SEPARATOR + databaseName;
        }
    }

    /**
     * Extract table location from table properties and remove location from properties. fallback to
     * create default location if not present
     *
     * @param tableProperties table properties.
     * @param tablePath fully qualified object for table.
     * @param catalogPath catalog path.
     * @return location for table.
     */
    public static String extractTableLocation(
            final Map<String, String> tableProperties,
            final ObjectPath tablePath,
            final String catalogPath) {
        if (tableProperties.containsKey(GlueCatalogConstants.LOCATION_URI)) {
            return tableProperties.remove(GlueCatalogConstants.LOCATION_URI);
        } else {
            return catalogPath
                    + GlueCatalogConstants.LOCATION_SEPARATOR
                    + tablePath.getDatabaseName()
                    + GlueCatalogConstants.LOCATION_SEPARATOR
                    + tablePath.getObjectName();
        }
    }

    /**
     * Build CatalogDatabase instance using information from glue Database instance.
     *
     * @param glueDatabase {@link Database }
     * @return {@link CatalogDatabase } instance.
     */
    public static CatalogDatabase getCatalogDatabase(final Database glueDatabase) {
        Map<String, String> properties = new HashMap<>(glueDatabase.parameters());
        return new CatalogDatabaseImpl(properties, glueDatabase.description());
    }

    /**
     * A Glue database name cannot be longer than 255 characters. The only acceptable characters are
     * lowercase letters, numbers, and the underscore character. More details: <a
     * href="https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html">...</a>
     *
     * @param name name
     */
    public static void validate(String name) {
        checkArgument(
                name != null && name.matches(GlueCatalogConstants.GLUE_DB_PATTERN.pattern()),
                "Database name does not comply with the Glue naming convention. "
                        + "Check here https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html");
    }

    /** validate response from client call. */
    public static void validateGlueResponse(GlueResponse response) {
        if (response != null && !response.sdkHttpResponse().isSuccessful()) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER);
        }
    }

    /**
     * @param udf Instance of UserDefinedFunction
     * @return ClassName for function
     */
    public static String getCatalogFunctionClassName(final UserDefinedFunction udf) {
        validateUDFClassName(udf.className());
        String[] splitName = udf.className().split(GlueCatalogConstants.DEFAULT_SEPARATOR);
        return splitName[splitName.length - 1];
    }

    /**
     * Validates UDF class name from glue.
     *
     * @param name name of UDF.
     */
    private static void validateUDFClassName(final String name) {
        checkArgument(!isNullOrWhitespaceOnly(name));

        if (name.split(GlueCatalogConstants.DEFAULT_SEPARATOR).length
                != GlueCatalogConstants.UDF_CLASS_NAME_SIZE) {
            throw new ValidationException("Improper ClassName: " + name);
        }
    }

    /**
     * Derive functionalLanguage from glue function name. Glue doesn't have any attribute to save
     * the functionalLanguage Name. Thus, storing FunctionalLanguage in the name itself.
     *
     * @param glueFunction Function name from glue.
     * @return Identifier for FunctionalLanguage.
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
     * Get expanded Query from CatalogBaseTable.
     *
     * @param table Instance of catalogBaseTable.
     * @return expandedQuery for Glue Table.
     */
    public static String getExpandedQuery(CatalogBaseTable table) {
        // https://issues.apache.org/jira/browse/FLINK-31961
        return "";
    }

    /**
     * Get Original Query from CatalogBaseTable.
     *
     * @param table Instance of CatalogBaseTable.
     * @return OriginalQuery for Glue Table.
     */
    public static String getOriginalQuery(CatalogBaseTable table) {
        // https://issues.apache.org/jira/browse/FLINK-31961
        return "";
    }

    /**
     * Extract table owner name and remove from properties.
     *
     * @param properties Map of properties.
     * @return fully qualified owner name.
     */
    public static String extractTableOwner(Map<String, String> properties) {
        return properties.containsKey(GlueCatalogConstants.TABLE_OWNER)
                ? properties.remove(GlueCatalogConstants.TABLE_OWNER)
                : null;
    }

    /**
     * Derive Instance of Glue Column from {@link CatalogBaseTable}.
     *
     * @param flinkColumn Instance of {@link org.apache.flink.table.catalog.Column}.
     * @throws CatalogException Throws exception in case of failure.
     */
    public static Column getGlueColumn(org.apache.flink.table.catalog.Column flinkColumn)
            throws CatalogException {
        return Column.builder()
                .comment(flinkColumn.asSummaryString())
                .type(TypeMapper.mapFlinkTypeToGlueType(flinkColumn.getDataType().getLogicalType()))
                .name(flinkColumn.getName())
                .parameters(buildGlueColumnParams(flinkColumn))
                .build();
    }

    public static Map<String, String> buildGlueColumnParams(
            org.apache.flink.table.catalog.Column column) {
        Map<String, String> params = new HashMap<>();
        params.put(IS_PERSISTED, column.isPersisted() ? TRUE : FALSE);
        params.put(EXPLAIN_EXTRAS, column.explainExtras().orElse(null));
        params.put(IS_PHYSICAL, column.isPhysical() ? TRUE : FALSE);
        return params;
    }

    /**
     * Build set of {@link Column} associated with table.
     *
     * @param table instance of {@link CatalogBaseTable}.
     * @return List of Column
     */
    public static List<Column> getGlueColumnsFromCatalogTable(final CatalogBaseTable table) {
        ResolvedCatalogBaseTable<?> resolvedTable = (ResolvedCatalogBaseTable<?>) table;
        return resolvedTable.getResolvedSchema().getColumns().stream()
                .map(GlueUtils::getGlueColumn)
                .collect(Collectors.toList());
    }

    /**
     * Extract InputFormat from properties if present and remove inputFormat from properties.
     * fallback to default format if not present
     *
     * @param tableProperties Key/Value properties
     * @return input Format.
     */
    public static String extractInputFormat(final Map<String, String> tableProperties) {
        return tableProperties.containsKey(GlueCatalogConstants.TABLE_INPUT_FORMAT)
                ? tableProperties.remove(GlueCatalogConstants.TABLE_INPUT_FORMAT)
                : GlueCatalogOptions.INPUT_FORMAT.defaultValue();
    }

    /**
     * Extract OutputFormat from properties if present and remove outputFormat from properties.
     * fallback to default format if not present
     *
     * @param tableProperties Key/Value properties
     * @return output Format.
     */
    public static String extractOutputFormat(Map<String, String> tableProperties) {
        return tableProperties.containsKey(GlueCatalogConstants.TABLE_OUTPUT_FORMAT)
                ? tableProperties.remove(GlueCatalogConstants.TABLE_OUTPUT_FORMAT)
                : GlueCatalogOptions.OUTPUT_FORMAT.defaultValue();
    }

    /**
     * Get list of filtered columns which are partition columns.
     *
     * @param catalogTable {@link CatalogTable} instance.
     * @param columns List of all column in table.
     * @return List of column marked as partition key.
     */
    public static Collection<Column> getPartitionKeys(
            CatalogTable catalogTable, Collection<Column> columns) {
        Set<String> partitionKeys = new HashSet<>(catalogTable.getPartitionKeys());
        return columns.stream()
                .filter(column -> partitionKeys.contains(column.name()))
                .collect(Collectors.toList());
    }

    public static String getDebugLog(final GlueResponse response) {
        return String.format(
                "Glue response : status = %s \n" + "Details = %s \nMetadataResponse = %s",
                response.sdkHttpResponse().isSuccessful(),
                response.sdkHttpResponse().toString(),
                response.responseMetadata());
    }

    /**
     * Derive {@link Schema} from Glue {@link Table}.
     *
     * @param glueTable Instance of {@link Table}
     * @return {@link Schema} of table.
     */
    public static Schema getSchemaFromGlueTable(Table glueTable) {
        List<Column> columns = glueTable.storageDescriptor().columns();
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (Column column : columns) {
            String columnName = column.name();
            String columnType = column.type().toLowerCase();
            AbstractDataType<?> flinkDataType = TypeMapper.glueTypeToFlinkType(columnType);
            schemaBuilder.column(columnName, flinkDataType);
        }
        return schemaBuilder.build();
    }

    /**
     * Get column names from List of {@link Column}.
     *
     * @param columns List of {@link Column}.
     * @return Names of all Columns.
     */
    public static List<String> getColumnNames(final List<Column> columns) {
        return columns.stream().map(Column::name).collect(Collectors.toList());
    }

    /**
     * Function ClassName pattern to be kept in Glue Data Catalog.
     *
     * @param function Catalog Function.
     * @return function class name.
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

    /**
     * Derive the expression string from given {@link Expression}.
     *
     * @param expression Instance of {@link Expression}.
     * @return Derived String from {@link Expression}.
     */
    public static String getExpressionString(Expression expression) {
        return getExpressionString(expression, new StringBuilder());
    }

    /**
     * Recursively derive the expression string from given {@link Expression}.
     *
     * @param expression Instance of {@link Expression}.
     * @param sb Used to build the derived expression string during recursion.
     * @return Derived String from {@link Expression}.
     */
    private static String getExpressionString(Expression expression, StringBuilder sb) {
        for (Expression childExpression : expression.getChildren()) {
            if (childExpression.getChildren() != null && !childExpression.getChildren().isEmpty()) {
                getExpressionString(childExpression, sb);
            }
        }

        // If the StringBuilder is not empty, append "AND "
        if (sb.length() > 0) {
            sb.append(GlueCatalogConstants.SPACE)
                    .append(GlueCatalogConstants.AND)
                    .append(GlueCatalogConstants.SPACE);
        }

        // Append the current expression summary
        sb.append(expression.asSummaryString());

        return sb.toString();
    }
}
