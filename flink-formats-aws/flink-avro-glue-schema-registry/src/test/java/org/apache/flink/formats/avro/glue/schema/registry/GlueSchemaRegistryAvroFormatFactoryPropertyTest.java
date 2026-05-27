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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.TestDynamicTableFactory;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property-based tests for required option validation in {@link
 * GlueSchemaRegistryAvroFormatFactory}.
 *
 * <p><b>Validates: Requirements 1.2, 3.2, 4.2, 5.3</b>
 */
@Tag("Feature: gsr-flink-sql-formats, Property 1: Required option validation across all format"
        + " factories")
class GlueSchemaRegistryAvroFormatFactoryPropertyTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    /**
     * For any subset of required options that is missing at least one required option, creating a
     * table source should throw a ValidationException.
     *
     * <p>Required options: aws.region, registry.name, schema.name
     */
    @Property(tries = 100)
    void missingAnyRequiredOptionCausesValidationException(
            @ForAll("incompleteRequiredOptionSets") RequiredOptionSubset subset) {

        Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");
        options.put("format", GlueSchemaRegistryAvroFormatFactory.IDENTIFIER);

        if (subset.includeRegion) {
            options.put("avro-glue.aws.region", subset.regionValue);
        }
        if (subset.includeRegistryName) {
            options.put("avro-glue.registry.name", subset.registryNameValue);
        }
        if (subset.includeSchemaName) {
            options.put("avro-glue.schema.name", subset.schemaNameValue);
        }

        assertThatThrownBy(() -> createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class);
    }

    @Provide
    Arbitrary<RequiredOptionSubset> incompleteRequiredOptionSets() {
        Arbitrary<Boolean> bools = Arbitraries.of(true, false);
        Arbitrary<String> regionValues =
                Arbitraries.of("us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1");
        Arbitrary<String> registryValues =
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
        Arbitrary<String> schemaValues =
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);

        return Combinators.combine(bools, bools, bools, regionValues, registryValues, schemaValues)
                .as(RequiredOptionSubset::new)
                // Filter to only keep subsets where at least one required option is missing
                .filter(s -> !(s.includeRegion && s.includeRegistryName && s.includeSchemaName));
    }

    /** Value object representing a subset of required options. */
    static class RequiredOptionSubset {
        final boolean includeRegion;
        final boolean includeRegistryName;
        final boolean includeSchemaName;
        final String regionValue;
        final String registryNameValue;
        final String schemaNameValue;

        RequiredOptionSubset(
                boolean includeRegion,
                boolean includeRegistryName,
                boolean includeSchemaName,
                String regionValue,
                String registryNameValue,
                String schemaNameValue) {
            this.includeRegion = includeRegion;
            this.includeRegistryName = includeRegistryName;
            this.includeSchemaName = includeSchemaName;
            this.regionValue = regionValue;
            this.registryNameValue = registryNameValue;
            this.schemaNameValue = schemaNameValue;
        }

        @Override
        public String toString() {
            return "RequiredOptionSubset{"
                    + "region="
                    + (includeRegion ? regionValue : "<missing>")
                    + ", registryName="
                    + (includeRegistryName ? registryNameValue : "<missing>")
                    + ", schemaName="
                    + (includeSchemaName ? schemaNameValue : "<missing>")
                    + '}';
        }
    }
}
