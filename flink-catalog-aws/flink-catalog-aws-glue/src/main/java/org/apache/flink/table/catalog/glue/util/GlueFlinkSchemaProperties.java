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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Persists the parts of a Flink schema that AWS Glue columns cannot represent - computed columns,
 * metadata columns, watermarks, and the primary key - as Glue table parameters, and restores them
 * when a table is read back.
 *
 * <p>Physical columns are stored as native Glue columns (so other engines can read the table),
 * while everything else round-trips through {@code flink.schema.*} parameters. Without this, {@code
 * CREATE TABLE} statements with watermarks or primary keys would silently lose them on read back,
 * and computed or metadata columns would be corrupted into physical columns.
 *
 * <p>Every per-column parameter is keyed by the <em>column name</em>, and the full declared column
 * order is persisted separately in {@link #COLUMN_ORDER}. This is deliberate: Glue returns physical
 * columns in storage-descriptor order followed by partition keys, which is not the declared order
 * when a partition key is not last in the DDL. Keying by ordinal position would then read an
 * override back onto the wrong column (e.g. a {@code TIMESTAMP(3)} override meant for {@code ts}
 * landing on an {@code INT} column). Names are unique within a schema, so keying by name is
 * position-independent and safe under reordering.
 */
@Internal
public final class GlueFlinkSchemaProperties {

    private static final Logger LOG = LoggerFactory.getLogger(GlueFlinkSchemaProperties.class);

    /** Prefix shared by all schema-fidelity parameters written by this class. */
    public static final String SCHEMA_PARAMETER_PREFIX = "flink.schema.";

    /** Comma-joined declared column order (by name), used to reconstruct the schema exactly. */
    private static final String COLUMN_ORDER = SCHEMA_PARAMETER_PREFIX + "column-order";

    private static final String COLUMN_PREFIX = SCHEMA_PARAMETER_PREFIX + "column.";
    private static final String COMPUTED_EXPR_SUFFIX = ".computed.expr";
    private static final String METADATA_DATA_TYPE_SUFFIX = ".metadata.data-type";
    private static final String METADATA_KEY_SUFFIX = ".metadata.key";
    private static final String METADATA_VIRTUAL_SUFFIX = ".metadata.virtual";
    private static final String PHYSICAL_DATA_TYPE_SUFFIX = ".data-type";

    /**
     * Comment of a computed or metadata column. Physical column comments are stored natively on the
     * Glue column and need no parameter.
     */
    private static final String COMMENT_SUFFIX = ".comment";

    private static final String WATERMARK_PREFIX = SCHEMA_PARAMETER_PREFIX + "watermark.";
    private static final String WATERMARK_ROWTIME_SUFFIX = ".rowtime";
    private static final String WATERMARK_STRATEGY_EXPR_SUFFIX = ".strategy.expr";

    private static final String PRIMARY_KEY_NAME = SCHEMA_PARAMETER_PREFIX + "primary-key.name";
    private static final String PRIMARY_KEY_COLUMNS =
            SCHEMA_PARAMETER_PREFIX + "primary-key.columns";

    /**
     * Comma-joined names of physical columns declared NOT NULL. Glue type strings carry no
     * nullability, so without this the restored primary key would fail schema resolution ("Invalid
     * primary key: column is nullable").
     */
    private static final String NOT_NULL_COLUMNS = SCHEMA_PARAMETER_PREFIX + "not-null-columns";

    private static final String LIST_SEPARATOR = ",";

    /**
     * Escape character inside a {@link #LIST_SEPARATOR}-joined list of column names. Flink
     * identifiers may contain any character when backtick-quoted, including the separator itself,
     * so {@code ,} and {@code \} in a name are written as {@code \,} and {@code \\}. Values written
     * before escaping existed contain neither character and decode unchanged.
     */
    private static final char LIST_ESCAPE = '\\';

    private static final GlueTypeConverter TYPE_CONVERTER = new GlueTypeConverter();

    private GlueFlinkSchemaProperties() {}

    /**
     * Serializes the non-physical parts of the given schema into the target parameter map. All
     * per-column entries are keyed by column name; {@link #COLUMN_ORDER} records the declared
     * order.
     *
     * @param resolvedSchema the resolved Flink schema
     * @param targetParameters the mutable Glue table parameter map to write into
     * @throws CatalogException if an expression cannot be serialized to SQL
     */
    public static void serializeNonPhysicalSchema(
            ResolvedSchema resolvedSchema, Map<String, String> targetParameters) {
        List<Column> columns = resolvedSchema.getColumns();
        List<String> columnOrder = new ArrayList<>(columns.size());
        List<String> notNullColumns = new ArrayList<>();

        for (Column column : columns) {
            String name = column.getName();
            columnOrder.add(name);

            if (column instanceof Column.ComputedColumn) {
                Column.ComputedColumn computed = (Column.ComputedColumn) column;
                targetParameters.put(
                        COLUMN_PREFIX + name + COMPUTED_EXPR_SUFFIX,
                        serializeExpression(name, computed.getExpression()));
                column.getComment()
                        .ifPresent(
                                c ->
                                        targetParameters.put(
                                                COLUMN_PREFIX + name + COMMENT_SUFFIX, c));
            } else if (column instanceof Column.MetadataColumn) {
                Column.MetadataColumn metadata = (Column.MetadataColumn) column;
                targetParameters.put(
                        COLUMN_PREFIX + name + METADATA_DATA_TYPE_SUFFIX,
                        column.getDataType().getLogicalType().asSerializableString());
                metadata.getMetadataKey()
                        .ifPresent(
                                key ->
                                        targetParameters.put(
                                                COLUMN_PREFIX + name + METADATA_KEY_SUFFIX, key));
                targetParameters.put(
                        COLUMN_PREFIX + name + METADATA_VIRTUAL_SUFFIX,
                        String.valueOf(metadata.isVirtual()));
                column.getComment()
                        .ifPresent(
                                c ->
                                        targetParameters.put(
                                                COLUMN_PREFIX + name + COMMENT_SUFFIX, c));
            } else {
                // Physical column.
                LogicalType declared = column.getDataType().getLogicalType();
                if (!declared.isNullable()) {
                    notNullColumns.add(name);
                }
                // Glue's Hive-style type strings are lossy for some types (for example
                // TIMESTAMP(3) has no Glue representation and would come back as TIMESTAMP(6)).
                // When the Glue round-trip does not reproduce the declared type, record the
                // original so the read path can restore it exactly.
                LogicalType glueRoundTrip =
                        TYPE_CONVERTER
                                .toFlinkDataType(
                                        TYPE_CONVERTER.toGlueDataType(column.getDataType()))
                                .getLogicalType();
                if (!declared.copy(true).equals(glueRoundTrip.copy(true))) {
                    targetParameters.put(
                            COLUMN_PREFIX + name + PHYSICAL_DATA_TYPE_SUFFIX,
                            declared.asSerializableString());
                }
            }
        }

        targetParameters.put(COLUMN_ORDER, joinNames(columnOrder));

        if (!notNullColumns.isEmpty()) {
            targetParameters.put(NOT_NULL_COLUMNS, joinNames(notNullColumns));
        }

        List<WatermarkSpec> watermarkSpecs = resolvedSchema.getWatermarkSpecs();
        for (int i = 0; i < watermarkSpecs.size(); i++) {
            WatermarkSpec spec = watermarkSpecs.get(i);
            targetParameters.put(
                    WATERMARK_PREFIX + i + WATERMARK_ROWTIME_SUFFIX, spec.getRowtimeAttribute());
            targetParameters.put(
                    WATERMARK_PREFIX + i + WATERMARK_STRATEGY_EXPR_SUFFIX,
                    serializeExpression(spec.getRowtimeAttribute(), spec.getWatermarkExpression()));
        }

        resolvedSchema
                .getPrimaryKey()
                .ifPresent(
                        primaryKey -> {
                            targetParameters.put(PRIMARY_KEY_NAME, primaryKey.getName());
                            targetParameters.put(
                                    PRIMARY_KEY_COLUMNS, joinNames(primaryKey.getColumns()));
                        });
    }

    /**
     * Returns whether the given Glue table parameter key was written by {@link
     * #serializeNonPhysicalSchema} and must therefore be hidden from the table options exposed to
     * users, and rejected if a user tries to supply it as a table option.
     *
     * @param parameterKey the Glue table parameter key
     * @return true when the key is an internal schema-fidelity parameter
     */
    public static boolean isSchemaParameter(String parameterKey) {
        return parameterKey.startsWith(SCHEMA_PARAMETER_PREFIX);
    }

    /**
     * Rebuilds the full Flink schema from the physical columns read out of Glue plus the {@code
     * flink.schema.*} parameters written by {@link #serializeNonPhysicalSchema}. Columns are
     * emitted in the persisted declared order (by name), so computed/metadata columns land at their
     * original positions and physical columns keep their declared types and nullability regardless
     * of the order Glue returns them in.
     *
     * @param parameters the Glue table parameters (may be null)
     * @param glueColumns the physical columns read from Glue, keyed by Flink-facing name, in the
     *     order Glue returned them (storage-descriptor columns followed by partition keys)
     * @param glueColumnComments comments of the physical columns read from Glue, keyed by
     *     Flink-facing name (may be null or partial)
     * @param schemaBuilder the schema builder to populate
     */
    public static void restoreSchema(
            Map<String, String> parameters,
            LinkedHashMap<String, DataType> glueColumns,
            Map<String, String> glueColumnComments,
            Schema.Builder schemaBuilder) {

        Map<String, String> comments =
                glueColumnComments == null ? Collections.emptyMap() : glueColumnComments;

        // Tables written by another engine, or before this format existed, carry no declared
        // order: emit the Glue columns as-is.
        if (parameters == null || !parameters.containsKey(COLUMN_ORDER)) {
            glueColumns.forEach(
                    (name, dataType) -> {
                        schemaBuilder.column(name, dataType);
                        applyComment(schemaBuilder, comments.get(name));
                    });
            return;
        }

        Set<String> notNullColumns = getNotNullColumns(parameters);
        Set<String> restored = new LinkedHashSet<>();

        for (String name : splitNames(parameters.get(COLUMN_ORDER))) {
            if (name.isEmpty()) {
                continue;
            }
            String computedExpr = parameters.get(COLUMN_PREFIX + name + COMPUTED_EXPR_SUFFIX);
            String metadataType = parameters.get(COLUMN_PREFIX + name + METADATA_DATA_TYPE_SUFFIX);
            String declaredType = parameters.get(COLUMN_PREFIX + name + PHYSICAL_DATA_TYPE_SUFFIX);

            if (computedExpr != null) {
                schemaBuilder.columnByExpression(name, computedExpr);
                applyComment(schemaBuilder, parameters.get(COLUMN_PREFIX + name + COMMENT_SUFFIX));
            } else if (metadataType != null) {
                schemaBuilder.columnByMetadata(
                        name,
                        DataTypes.of(metadataType),
                        parameters.get(COLUMN_PREFIX + name + METADATA_KEY_SUFFIX),
                        Boolean.parseBoolean(
                                parameters.get(COLUMN_PREFIX + name + METADATA_VIRTUAL_SUFFIX)));
                applyComment(schemaBuilder, parameters.get(COLUMN_PREFIX + name + COMMENT_SUFFIX));
            } else if (declaredType != null && glueColumns.containsKey(name)) {
                // The declared type could not be represented exactly in Glue; restore the
                // recorded original (it carries its own nullability). Like any physical column,
                // it must still exist in Glue: see the final branch.
                schemaBuilder.column(name, DataTypes.of(declaredType));
                applyComment(schemaBuilder, comments.get(name));
            } else if (glueColumns.containsKey(name)) {
                DataType dataType = glueColumns.get(name);
                schemaBuilder.column(
                        name, notNullColumns.contains(name) ? dataType.notNull() : dataType);
                applyComment(schemaBuilder, comments.get(name));
            } else {
                // Recorded in the declared order but no longer present in the Glue table: another
                // engine dropped the column. Follow Glue (it is the source of truth for physical
                // columns) but say so, since the Flink schema now differs from what was declared.
                LOG.warn(
                        "Column '{}' is recorded in {} but no longer exists in the Glue table; "
                                + "it was dropped by another engine and is omitted from the "
                                + "Flink schema.",
                        name,
                        COLUMN_ORDER);
                continue;
            }
            restored.add(name);
        }

        // Columns added to the Glue table by another engine after Flink created it: append them
        // so nothing is silently dropped.
        glueColumns.forEach(
                (name, dataType) -> {
                    if (!restored.contains(name)) {
                        schemaBuilder.column(
                                name,
                                notNullColumns.contains(name) ? dataType.notNull() : dataType);
                        applyComment(schemaBuilder, comments.get(name));
                    }
                });

        // Restore watermarks in declared order.
        for (int i = 0; ; i++) {
            String rowtime = parameters.get(WATERMARK_PREFIX + i + WATERMARK_ROWTIME_SUFFIX);
            String expression =
                    parameters.get(WATERMARK_PREFIX + i + WATERMARK_STRATEGY_EXPR_SUFFIX);
            if (rowtime == null || expression == null) {
                break;
            }
            schemaBuilder.watermark(rowtime, expression);
        }

        // Restore the primary key.
        String primaryKeyColumns = parameters.get(PRIMARY_KEY_COLUMNS);
        if (primaryKeyColumns != null && !primaryKeyColumns.isEmpty()) {
            List<String> columns = splitNames(primaryKeyColumns);
            String constraintName = parameters.get(PRIMARY_KEY_NAME);
            if (constraintName != null && !constraintName.isEmpty()) {
                schemaBuilder.primaryKeyNamed(constraintName, columns);
            } else {
                schemaBuilder.primaryKey(columns);
            }
        }
    }

    /**
     * Returns the names of physical columns that were declared NOT NULL, as recorded by {@link
     * #serializeNonPhysicalSchema}.
     *
     * @param parameters the Glue table parameters (may be null)
     * @return the NOT NULL column names; empty when none were recorded
     */
    public static Set<String> getNotNullColumns(Map<String, String> parameters) {
        if (parameters == null || !parameters.containsKey(NOT_NULL_COLUMNS)) {
            return Collections.emptySet();
        }
        return new LinkedHashSet<>(splitNames(parameters.get(NOT_NULL_COLUMNS)));
    }

    private static void applyComment(Schema.Builder schemaBuilder, String comment) {
        if (comment != null && !comment.isEmpty()) {
            schemaBuilder.withComment(comment);
        }
    }

    /** Joins column names with {@link #LIST_SEPARATOR}, escaping separators inside a name. */
    @VisibleForTesting
    static String joinNames(Collection<String> names) {
        StringBuilder joined = new StringBuilder();
        boolean first = true;
        for (String name : names) {
            if (!first) {
                joined.append(LIST_SEPARATOR);
            }
            first = false;
            for (int i = 0; i < name.length(); i++) {
                char c = name.charAt(i);
                if (c == LIST_ESCAPE || LIST_SEPARATOR.indexOf(c) >= 0) {
                    joined.append(LIST_ESCAPE);
                }
                joined.append(c);
            }
        }
        return joined.toString();
    }

    /** Inverse of {@link #joinNames}: splits on unescaped separators and removes the escapes. */
    @VisibleForTesting
    static List<String> splitNames(String joined) {
        List<String> names = new ArrayList<>();
        if (joined.isEmpty()) {
            return names;
        }
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < joined.length(); i++) {
            char c = joined.charAt(i);
            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == LIST_ESCAPE) {
                escaped = true;
            } else if (LIST_SEPARATOR.indexOf(c) >= 0) {
                names.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (escaped) {
            // A trailing escape has nothing to escape; keep it literally rather than drop it.
            current.append(LIST_ESCAPE);
        }
        names.add(current.toString());
        return names;
    }

    private static String serializeExpression(
            String columnOrAttributeName,
            org.apache.flink.table.expressions.ResolvedExpression expression) {
        try {
            return expression.asSerializableString();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Expression for '%s' cannot be persisted to AWS Glue because it is "
                                    + "not serializable to SQL: %s",
                            columnOrAttributeName, expression.asSummaryString()),
                    e);
        }
    }
}
