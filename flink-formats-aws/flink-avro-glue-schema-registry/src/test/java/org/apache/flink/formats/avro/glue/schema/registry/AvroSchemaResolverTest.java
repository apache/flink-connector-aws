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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AvroSchemaResolver}.
 *
 * <p>Tests the schema resolution flow:
 * <ol>
 *   <li>If fetchFromRegistry is true and fetcher succeeds, use fetched schema</li>
 *   <li>If fetchFromRegistry is true but fetcher fails, fall back to patched/auto-generated schema</li>
 *   <li>If namespace/record-name overrides are provided, patch the schema</li>
 *   <li>Otherwise, use the auto-generated schema unchanged</li>
 * </ol>
 */
class AvroSchemaResolverTest {

    private static final RowType TEST_ROW_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING()),
                                    DataTypes.FIELD("value", DataTypes.INT()))
                            .getLogicalType();

    // Note: AvroSchemaConverter.convertToSchema returns a UNION ["null", record] for nullable rows
    private static final Schema AUTO_GENERATED_SCHEMA =
            AvroSchemaConverter.convertToSchema(TEST_ROW_TYPE);

    private static final String REGISTRY_NAME = "test-registry";
    private static final String SCHEMA_NAME = "test-schema";

    /**
     * Extracts the RECORD schema from a potentially UNION schema.
     * AvroSchemaConverter wraps records in ["null", record] unions.
     */
    private static Schema extractRecordSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No RECORD in UNION"));
        }
        return schema;
    }

    @Test
    void testResolveSchemaWithFetchFromRegistrySuccess() {
        // Create a mock fetched schema with custom namespace using SchemaBuilder
        Schema fetchedSchema = SchemaBuilder.record("FetchedRecord")
                .namespace("com.example.fetched")
                .fields()
                .optionalString("id")
                .optionalInt("value")
                .endRecord();

        MockSchemaFetcher fetcher = new MockSchemaFetcher(fetchedSchema);

        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, true);

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, fetcher);

        assertThat(resolved.getNamespace()).isEqualTo("com.example.fetched");
        assertThat(resolved.getName()).isEqualTo("FetchedRecord");
        assertThat(fetcher.fetchCalled).isTrue();
        assertThat(fetcher.lastRegistryName).isEqualTo(REGISTRY_NAME);
        assertThat(fetcher.lastSchemaName).isEqualTo(SCHEMA_NAME);
    }

    @Test
    void testResolveSchemaWithFetchFromRegistryFailure() {
        // Fetcher that throws an exception
        MockSchemaFetcher fetcher = new MockSchemaFetcher(new RuntimeException("GSR unavailable"));

        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, true);
        config.set(AvroGlueFormatOptions.AVRO_NAMESPACE, "com.example.fallback");

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, fetcher);

        // Should fall back to patched schema (extract RECORD from UNION)
        Schema resolvedRecord = extractRecordSchema(resolved);
        assertThat(resolvedRecord.getNamespace()).isEqualTo("com.example.fallback");
        assertThat(fetcher.fetchCalled).isTrue();
    }

    @Test
    void testResolveSchemaWithFetchFromRegistryReturnsNull() {
        // Fetcher that returns null (schema not found)
        MockSchemaFetcher fetcher = new MockSchemaFetcher((Schema) null);

        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, true);
        config.set(AvroGlueFormatOptions.AVRO_RECORD_NAME, "FallbackRecord");

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, fetcher);

        // Should fall back to patched schema (extract RECORD from UNION)
        Schema resolvedRecord = extractRecordSchema(resolved);
        assertThat(resolvedRecord.getName()).isEqualTo("FallbackRecord");
        assertThat(fetcher.fetchCalled).isTrue();
    }

    @Test
    void testResolveSchemaWithNamespaceOverrideOnly() {
        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, false);
        config.set(AvroGlueFormatOptions.AVRO_NAMESPACE, "com.example.custom");

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, null);

        // Extract RECORD from UNION for assertions
        Schema resolvedRecord = extractRecordSchema(resolved);
        Schema originalRecord = extractRecordSchema(AUTO_GENERATED_SCHEMA);
        assertThat(resolvedRecord.getNamespace()).isEqualTo("com.example.custom");
        assertThat(resolvedRecord.getName()).isEqualTo(originalRecord.getName());
    }

    @Test
    void testResolveSchemaWithRecordNameOverrideOnly() {
        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, false);
        config.set(AvroGlueFormatOptions.AVRO_RECORD_NAME, "CustomRecord");

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, null);

        // Extract RECORD from UNION for assertions
        Schema resolvedRecord = extractRecordSchema(resolved);
        Schema originalRecord = extractRecordSchema(AUTO_GENERATED_SCHEMA);
        assertThat(resolvedRecord.getNamespace()).isEqualTo(originalRecord.getNamespace());
        assertThat(resolvedRecord.getName()).isEqualTo("CustomRecord");
    }

    @Test
    void testResolveSchemaWithBothOverrides() {
        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, false);
        config.set(AvroGlueFormatOptions.AVRO_NAMESPACE, "com.example.custom");
        config.set(AvroGlueFormatOptions.AVRO_RECORD_NAME, "CustomRecord");

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, null);

        // Extract RECORD from UNION for assertions
        Schema resolvedRecord = extractRecordSchema(resolved);
        assertThat(resolvedRecord.getNamespace()).isEqualTo("com.example.custom");
        assertThat(resolvedRecord.getName()).isEqualTo("CustomRecord");
    }

    @Test
    void testResolveSchemaWithNoOverrides() {
        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, false);

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, null);

        // Should return the original schema unchanged
        assertThat(resolved).isSameAs(AUTO_GENERATED_SCHEMA);
    }

    @Test
    void testResolveSchemaWithFetchDisabledIgnoresFetcher() {
        Schema fetchedSchema = SchemaBuilder.record("FetchedRecord")
                .namespace("com.example.fetched")
                .fields()
                .optionalString("id")
                .optionalInt("value")
                .endRecord();

        MockSchemaFetcher fetcher = new MockSchemaFetcher(fetchedSchema);

        Configuration config = new Configuration();
        config.set(GlueFormatOptions.REGISTRY_NAME, REGISTRY_NAME);
        config.set(GlueFormatOptions.SCHEMA_NAME, SCHEMA_NAME);
        config.set(AvroGlueFormatOptions.SCHEMA_FETCH_FROM_REGISTRY, false);

        Schema resolved = AvroSchemaResolver.resolveSchema(AUTO_GENERATED_SCHEMA, config, fetcher);

        // Should NOT call fetcher when fetchFromRegistry is false
        assertThat(fetcher.fetchCalled).isFalse();
        assertThat(resolved).isSameAs(AUTO_GENERATED_SCHEMA);
    }

    /**
     * Mock implementation of SchemaFetcher for testing.
     */
    private static class MockSchemaFetcher implements AvroSchemaResolver.SchemaFetcher {
        private final Schema schemaToReturn;
        private final Exception exceptionToThrow;
        boolean fetchCalled = false;
        String lastRegistryName;
        String lastSchemaName;

        MockSchemaFetcher(@Nullable Schema schemaToReturn) {
            this.schemaToReturn = schemaToReturn;
            this.exceptionToThrow = null;
        }

        MockSchemaFetcher(Exception exceptionToThrow) {
            this.schemaToReturn = null;
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        public Schema fetchSchema(String registryName, String schemaName) throws Exception {
            fetchCalled = true;
            lastRegistryName = registryName;
            lastSchemaName = schemaName;
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return schemaToReturn;
        }
    }
}
