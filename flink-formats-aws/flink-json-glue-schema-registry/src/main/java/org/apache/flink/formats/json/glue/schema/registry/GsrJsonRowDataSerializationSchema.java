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

package org.apache.flink.formats.json.glue.schema.registry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;

/**
 * Serialization schema that wraps Flink's {@link JsonRowDataSerializationSchema} and prepends AWS
 * Glue Schema Registry header bytes.
 *
 * <p>The serialization flow is:
 *
 * <ol>
 *   <li>Serialize {@link RowData} to JSON bytes via Flink's JSON serializer
 *   <li>Register the JSON Schema with GSR (derived from the Flink RowType)
 *   <li>Prepend GSR header bytes (version + compression + schema UUID) to the JSON payload
 * </ol>
 */
@Internal
public class GsrJsonRowDataSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final JsonRowDataSerializationSchema jsonSerializer;
    private final String transportName;
    private final String schemaName;
    private final String jsonSchemaDefinition;
    private final NotNullConstraintValidator notNullValidator;
    private final Map<String, Object> configs;

    private transient GlueSchemaRegistrySerializationFacade serializationFacade;

    /**
     * Creates a new GSR JSON serialization schema.
     *
     * @param rowType the Flink RowType describing the table schema
     * @param jsonSerializer the inner Flink JSON serializer
     * @param transportName the transport name (topic/stream) for GSR schema naming
     * @param schemaName the schema name for GSR registration
     * @param configs the GSR SDK configuration map
     */
    public GsrJsonRowDataSerializationSchema(
            RowType rowType,
            JsonRowDataSerializationSchema jsonSerializer,
            String transportName,
            String schemaName,
            Map<String, Object> configs) {
        this.jsonSerializer = jsonSerializer;
        this.transportName = transportName;
        this.schemaName = schemaName != null ? schemaName : transportName;
        this.configs = configs;
        this.jsonSchemaDefinition = JsonSchemaConverter.convertToJsonSchema(rowType);
        this.notNullValidator = new NotNullConstraintValidator(rowType);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonSerializer.open(context);
        if (serializationFacade == null) {
            AwsCredentialsProvider credentialsProvider =
                    AWSGeneralUtil.getCredentialsProvider(configs);
            serializationFacade =
                    GlueSchemaRegistrySerializationFacade.builder()
                            .credentialProvider(credentialsProvider)
                            .glueSchemaRegistryConfiguration(
                                    new GlueSchemaRegistryConfiguration(configs))
                            .build();
        }
    }

    @Override
    public byte[] serialize(RowData element) {
        // Flink's JSON serializer writes null for a null field whatever the declared type, and
        // encode() below skips payload validation, so a NOT NULL violation would otherwise be
        // published as a record contradicting the schema registered for it. Reject it here.
        notNullValidator.validate(element);
        byte[] jsonBytes = jsonSerializer.serialize(element);
        if (jsonBytes == null) {
            return null;
        }

        // Use encode() instead of serialize() to avoid JSON validation issues.
        // encode() takes raw bytes and prepends GSR header without validation.
        // serialize() expects a Java object and validates against JSON schema.
        return serializationFacade.encode(
                transportName,
                new Schema(jsonSchemaDefinition, DataFormat.JSON.name(), schemaName),
                jsonBytes);
    }

    @VisibleForTesting
    void setSerializationFacade(GlueSchemaRegistrySerializationFacade facade) {
        this.serializationFacade = facade;
    }
}
