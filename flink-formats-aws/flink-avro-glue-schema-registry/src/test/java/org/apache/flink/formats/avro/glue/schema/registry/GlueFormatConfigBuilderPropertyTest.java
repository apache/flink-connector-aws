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

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link GlueFormatConfigBuilder}.
 *
 * <p><b>Property 6: Config map builder correctly maps all provided options</b>
 *
 * <p><b>Validates: Requirements 5.1, 5.2</b>
 */
@Tag(
        "Feature: gsr-flink-sql-formats, Property 6: Config map builder correctly maps all provided"
                + " options")
class GlueFormatConfigBuilderPropertyTest {

    /**
     * For any valid combination of GSR config option values, building the config map via
     * GlueFormatConfigBuilder.buildConfigMap() should produce a map where each provided option maps
     * to the correct AWSSchemaRegistryConstants key with the same value, and absent options are not
     * present in the map.
     */
    @Property(tries = 100)
    void configMapBuilderCorrectlyMapsAllProvidedOptions(
            @ForAll("gsrConfigCombinations") GsrConfigInput input) {

        Configuration config = new Configuration();

        input.region.ifPresent(v -> config.set(GlueFormatOptions.AWS_REGION, v));
        input.endpoint.ifPresent(v -> config.set(GlueFormatOptions.AWS_ENDPOINT, v));
        input.registryName.ifPresent(v -> config.set(GlueFormatOptions.REGISTRY_NAME, v));
        input.schemaName.ifPresent(v -> config.set(GlueFormatOptions.SCHEMA_NAME, v));
        input.cacheSize.ifPresent(v -> config.set(GlueFormatOptions.CACHE_SIZE, v));
        input.cacheTtlMs.ifPresent(v -> config.set(GlueFormatOptions.CACHE_TTL_MS, v));
        input.autoRegistration.ifPresent(
                v -> config.set(GlueFormatOptions.SCHEMA_AUTO_REGISTRATION, v));
        input.compatibility.ifPresent(v -> config.set(GlueFormatOptions.SCHEMA_COMPATIBILITY, v));
        input.compression.ifPresent(v -> config.set(GlueFormatOptions.SCHEMA_COMPRESSION, v));

        Map<String, Object> result = GlueFormatConfigBuilder.buildConfigMap(config);

        // Verify present options map to correct keys with correct values
        assertOptionalMapping(result, input.region, AWSSchemaRegistryConstants.AWS_REGION);
        assertOptionalMapping(result, input.endpoint, AWSSchemaRegistryConstants.AWS_ENDPOINT);
        assertOptionalMapping(result, input.registryName, AWSSchemaRegistryConstants.REGISTRY_NAME);
        assertOptionalMapping(result, input.schemaName, AWSSchemaRegistryConstants.SCHEMA_NAME);
        assertOptionalMapping(result, input.cacheSize, AWSSchemaRegistryConstants.CACHE_SIZE);
        assertOptionalMapping(
                result, input.cacheTtlMs, AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS);
        assertOptionalMapping(
                result,
                input.autoRegistration,
                AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING);
        assertOptionalMapping(
                result, input.compatibility, AWSSchemaRegistryConstants.COMPATIBILITY_SETTING);
        assertOptionalMapping(
                result, input.compression, AWSSchemaRegistryConstants.COMPRESSION_TYPE);

        // Verify map size equals number of provided options
        long providedCount =
                countPresent(
                        input.region,
                        input.endpoint,
                        input.registryName,
                        input.schemaName,
                        input.cacheSize,
                        input.cacheTtlMs,
                        input.autoRegistration,
                        input.compatibility,
                        input.compression);
        assertThat(result).hasSize((int) providedCount);
    }

    @Provide
    Arbitrary<GsrConfigInput> gsrConfigCombinations() {
        Arbitrary<Optional<String>> optRegion =
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20).optional();
        Arbitrary<Optional<String>> optEndpoint =
                Arbitraries.strings()
                        .alpha()
                        .ofMinLength(1)
                        .ofMaxLength(30)
                        .map(s -> "https://" + s)
                        .optional();
        Arbitrary<Optional<String>> optRegistryName =
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30).optional();
        Arbitrary<Optional<String>> optSchemaName =
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30).optional();
        Arbitrary<Optional<Integer>> optCacheSize =
                Arbitraries.integers().between(1, 10000).optional();
        Arbitrary<Optional<Long>> optCacheTtlMs =
                Arbitraries.longs().between(1000L, 172800000L).optional();
        Arbitrary<Optional<Boolean>> optAutoReg = Arbitraries.of(true, false).optional();
        Arbitrary<Optional<Compatibility>> optCompat =
                Arbitraries.of(Compatibility.knownValues().toArray(new Compatibility[0]))
                        .optional();
        Arbitrary<Optional<AWSSchemaRegistryConstants.COMPRESSION>> optCompress =
                Arbitraries.of(AWSSchemaRegistryConstants.COMPRESSION.values()).optional();

        // jqwik Combinators.combine supports up to 8 params, so we nest via flatMap
        return Combinators.combine(
                        optRegion,
                        optEndpoint,
                        optRegistryName,
                        optSchemaName,
                        optCacheSize,
                        optCacheTtlMs,
                        optAutoReg,
                        optCompat)
                .flatAs(
                        (region, endpoint, registry, schema, cache, ttl, autoReg, compat) ->
                                optCompress.map(
                                        compress ->
                                                new GsrConfigInput(
                                                        region, endpoint, registry, schema, cache,
                                                        ttl, autoReg, compat, compress)));
    }

    private static <T> void assertOptionalMapping(
            Map<String, Object> result, Optional<T> optionalValue, String expectedKey) {
        if (optionalValue.isPresent()) {
            assertThat(result).containsEntry(expectedKey, optionalValue.get());
        } else {
            assertThat(result).doesNotContainKey(expectedKey);
        }
    }

    private static long countPresent(Optional<?>... optionals) {
        long count = 0;
        for (Optional<?> opt : optionals) {
            if (opt.isPresent()) {
                count++;
            }
        }
        return count;
    }

    /** Value object holding an arbitrary combination of GSR config options. */
    static class GsrConfigInput {
        final Optional<String> region;
        final Optional<String> endpoint;
        final Optional<String> registryName;
        final Optional<String> schemaName;
        final Optional<Integer> cacheSize;
        final Optional<Long> cacheTtlMs;
        final Optional<Boolean> autoRegistration;
        final Optional<Compatibility> compatibility;
        final Optional<AWSSchemaRegistryConstants.COMPRESSION> compression;

        GsrConfigInput(
                Optional<String> region,
                Optional<String> endpoint,
                Optional<String> registryName,
                Optional<String> schemaName,
                Optional<Integer> cacheSize,
                Optional<Long> cacheTtlMs,
                Optional<Boolean> autoRegistration,
                Optional<Compatibility> compatibility,
                Optional<AWSSchemaRegistryConstants.COMPRESSION> compression) {
            this.region = region;
            this.endpoint = endpoint;
            this.registryName = registryName;
            this.schemaName = schemaName;
            this.cacheSize = cacheSize;
            this.cacheTtlMs = cacheTtlMs;
            this.autoRegistration = autoRegistration;
            this.compatibility = compatibility;
            this.compression = compression;
        }

        @Override
        public String toString() {
            return "GsrConfigInput{"
                    + "region="
                    + region
                    + ", endpoint="
                    + endpoint
                    + ", registryName="
                    + registryName
                    + ", schemaName="
                    + schemaName
                    + ", cacheSize="
                    + cacheSize
                    + ", cacheTtlMs="
                    + cacheTtlMs
                    + ", autoRegistration="
                    + autoRegistration
                    + ", compatibility="
                    + compatibility
                    + ", compression="
                    + compression
                    + '}';
        }
    }
}
