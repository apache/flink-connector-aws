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

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.annotation.Internal;

import org.apache.avro.Schema;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class that creates a new Avro {@link Schema} with overridden namespace and/or record name
 * while preserving all field definitions from the original schema.
 *
 * <p>This addresses the Avro schema namespace bug where Flink's {@code AvroSchemaConverter}
 * auto-generates a namespace (e.g. {@code org.apache.flink.avro.generated}) that may differ from
 * schemas already registered in AWS Glue Schema Registry.
 *
 * <p>The patcher recursively patches all nested record types to use the same namespace, ensuring
 * consistent namespace usage throughout the schema hierarchy.
 */
@Internal
public class AvroSchemaPatcher {

    /**
     * Creates a new Avro Schema with overridden namespace and/or record name, preserving all field
     * definitions from the original schema. Recursively patches all nested record types to use the
     * same namespace.
     *
     * @param original the original Avro schema (can be a RECORD or UNION type)
     * @param namespace the namespace override, or {@code null} to keep the original namespace
     * @param recordName the record name override, or {@code null} to keep the original record name
     * @return a new Schema with the overridden namespace/name and the same fields as the original
     */
    public static Schema patchSchema(
            Schema original, @Nullable String namespace, @Nullable String recordName) {
        if (namespace == null && recordName == null) {
            return original;
        }

        // Track already-patched schemas to handle recursive references
        Map<String, Schema> patchedSchemas = new HashMap<>();

        // Special handling for top-level UNION: apply recordName to the main RECORD in the union
        if (original.getType() == Schema.Type.UNION) {
            return patchTopLevelUnion(original, namespace, recordName, patchedSchemas);
        }

        return patchSchemaRecursive(original, namespace, recordName, patchedSchemas);
    }

    /**
     * Patches a top-level UNION schema, applying the recordName override to the main RECORD type.
     * This handles the common case where AvroSchemaConverter produces ["null", record] unions.
     */
    private static Schema patchTopLevelUnion(
            Schema unionSchema,
            @Nullable String namespace,
            @Nullable String recordName,
            Map<String, Schema> patchedSchemas) {

        List<Schema> patchedTypes = new ArrayList<>();
        boolean recordNameApplied = false;

        for (Schema unionType : unionSchema.getTypes()) {
            if (unionType.getType() == Schema.Type.RECORD && !recordNameApplied) {
                // Apply recordName to the first RECORD in the union
                patchedTypes.add(
                        patchSchemaRecursive(unionType, namespace, recordName, patchedSchemas));
                recordNameApplied = true;
            } else {
                // For other types (null, primitives, nested records), only apply namespace
                patchedTypes.add(
                        patchSchemaRecursive(unionType, namespace, null, patchedSchemas));
            }
        }
        return Schema.createUnion(patchedTypes);
    }

    private static Schema patchSchemaRecursive(
            Schema schema,
            @Nullable String namespace,
            @Nullable String recordName,
            Map<String, Schema> patchedSchemas) {

        switch (schema.getType()) {
            case RECORD:
                return patchRecordSchema(schema, namespace, recordName, patchedSchemas);

            case ARRAY:
                Schema patchedElement =
                        patchSchemaRecursive(
                                schema.getElementType(), namespace, null, patchedSchemas);
                return Schema.createArray(patchedElement);

            case MAP:
                Schema patchedValue =
                        patchSchemaRecursive(
                                schema.getValueType(), namespace, null, patchedSchemas);
                return Schema.createMap(patchedValue);

            case UNION:
                List<Schema> patchedTypes = new ArrayList<>();
                for (Schema unionType : schema.getTypes()) {
                    patchedTypes.add(
                            patchSchemaRecursive(unionType, namespace, null, patchedSchemas));
                }
                return Schema.createUnion(patchedTypes);

            default:
                // Primitive types and other types don't need patching
                return schema;
        }
    }

    private static Schema patchRecordSchema(
            Schema original,
            @Nullable String namespace,
            @Nullable String recordName,
            Map<String, Schema> patchedSchemas) {

        String originalFullName = original.getFullName();

        // Check if we've already patched this schema (handles recursive references)
        if (patchedSchemas.containsKey(originalFullName)) {
            return patchedSchemas.get(originalFullName);
        }

        String effectiveNamespace = namespace != null ? namespace : original.getNamespace();
        String effectiveName = recordName != null ? recordName : original.getName();

        // Create the new schema first (without fields) to handle recursive references
        Schema patched =
                Schema.createRecord(
                        effectiveName,
                        original.getDoc(),
                        effectiveNamespace,
                        original.isError());

        // Register before processing fields to handle self-references
        patchedSchemas.put(originalFullName, patched);

        // Now patch all fields recursively
        List<Schema.Field> patchedFields =
                original.getFields().stream()
                        .map(
                                f -> {
                                    Schema patchedFieldSchema =
                                            patchSchemaRecursive(
                                                    f.schema(), namespace, null, patchedSchemas);
                                    return new Schema.Field(
                                            f.name(),
                                            patchedFieldSchema,
                                            f.doc(),
                                            f.defaultVal());
                                })
                        .collect(Collectors.toList());

        patched.setFields(patchedFields);
        return patched;
    }

    private AvroSchemaPatcher() {}
}
