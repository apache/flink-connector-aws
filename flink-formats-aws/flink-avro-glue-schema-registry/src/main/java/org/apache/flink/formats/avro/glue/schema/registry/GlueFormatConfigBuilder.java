/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class that builds the {@code Map<String, Object>} configuration required by the AWS Glue
 * Schema Registry SDK from Flink's {@link ReadableConfig} format options.
 */
@Internal
public class GlueFormatConfigBuilder {

    /**
     * Builds a GSR SDK configuration map from the shared {@link GlueFormatOptions}.
     *
     * @param formatOptions the Flink format options from SQL DDL
     * @return a map of GSR SDK configuration keys to their values
     */
    public static Map<String, Object> buildConfigMap(ReadableConfig formatOptions) {
        final Map<String, Object> properties = new HashMap<>();

        formatOptions
                .getOptional(GlueFormatOptions.AWS_REGION)
                .ifPresent(v -> properties.put(AWSSchemaRegistryConstants.AWS_REGION, v));
        formatOptions
                .getOptional(GlueFormatOptions.AWS_ENDPOINT)
                .ifPresent(v -> properties.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, v));
        formatOptions
                .getOptional(GlueFormatOptions.REGISTRY_NAME)
                .ifPresent(v -> properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, v));
        formatOptions
                .getOptional(GlueFormatOptions.SCHEMA_NAME)
                .ifPresent(v -> properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, v));
        formatOptions
                .getOptional(GlueFormatOptions.CACHE_SIZE)
                .ifPresent(v -> properties.put(AWSSchemaRegistryConstants.CACHE_SIZE, v));
        formatOptions
                .getOptional(GlueFormatOptions.CACHE_TTL_MS)
                .ifPresent(
                        v ->
                                properties.put(
                                        AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, v));
        formatOptions
                .getOptional(GlueFormatOptions.SCHEMA_AUTO_REGISTRATION)
                .ifPresent(
                        v ->
                                properties.put(
                                        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
                                        v));
        formatOptions
                .getOptional(GlueFormatOptions.SCHEMA_COMPATIBILITY)
                .ifPresent(
                        v -> properties.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, v));
        formatOptions
                .getOptional(GlueFormatOptions.SCHEMA_COMPRESSION)
                .ifPresent(v -> properties.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, v));

        return properties;
    }

    private GlueFormatConfigBuilder() {}
}
