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

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;

import java.net.URI;
import java.util.Map;

/**
 * Fetches the latest Avro schema from AWS Glue Schema Registry using the Glue SDK.
 *
 * <p>Used when {@code schema.fetchFromRegistry=true} to resolve the actual schema from GSR instead
 * of relying on the auto-generated Flink schema (which defaults to namespace {@code
 * org.apache.flink.avro.generated} and record name {@code record}).
 */
@Internal
public class GlueSchemaRegistrySchemaFetcher implements AvroSchemaResolver.SchemaFetcher {

    private static final Logger LOG =
            LoggerFactory.getLogger(GlueSchemaRegistrySchemaFetcher.class);

    private final GlueClient glueClient;

    public GlueSchemaRegistrySchemaFetcher(Map<String, Object> configMap) {
        String region = (String) configMap.get(AWSSchemaRegistryConstants.AWS_REGION);
        GlueClientBuilder builder = GlueClient.builder();
        if (region != null) {
            builder.region(Region.of(region));
        }
        Object endpoint = configMap.get(AWSSchemaRegistryConstants.AWS_ENDPOINT);
        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint.toString()));
        }
        this.glueClient = builder.build();
        LOG.debug("GlueSchemaRegistrySchemaFetcher initialized for region: {}", region);
    }

    /** Package-private constructor for testing with a pre-built GlueClient. */
    GlueSchemaRegistrySchemaFetcher(GlueClient glueClient) {
        this.glueClient = glueClient;
    }

    @Override
    public Schema fetchSchema(String registryName, String schemaName) throws Exception {
        LOG.debug(
                "Fetching schema from GSR - registry: '{}', schema: '{}'",
                registryName,
                schemaName);

        GetSchemaVersionRequest request =
                GetSchemaVersionRequest.builder()
                        .schemaId(
                                SchemaId.builder()
                                        .registryName(registryName)
                                        .schemaName(schemaName)
                                        .build())
                        .schemaVersionNumber(
                                SchemaVersionNumber.builder().latestVersion(true).build())
                        .build();

        GetSchemaVersionResponse response = glueClient.getSchemaVersion(request);
        String schemaDefinition = response.schemaDefinition();

        if (schemaDefinition == null || schemaDefinition.isEmpty()) {
            LOG.warn(
                    "Schema definition is null or empty for registry: '{}', schema: '{}'",
                    registryName,
                    schemaName);
            return null;
        }

        Schema schema = new Schema.Parser().parse(schemaDefinition);
        LOG.debug(
                "Fetched schema - namespace: '{}', name: '{}', fields: {}",
                schema.getNamespace(),
                schema.getName(),
                schema.getFields().size());

        return schema;
    }
}
