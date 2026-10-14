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

package org.apache.flink.formats.protobuf.glue.schema.registry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;

/**
 * Serialization schema that converts Flink {@link RowData} to Protobuf bytes and prepends AWS Glue
 * Schema Registry header bytes.
 *
 * <p>The serialization flow is:
 *
 * <ol>
 *   <li>Convert {@link RowData} to a Protobuf {@link DynamicMessage} using the schema derived from
 *       the Flink RowType
 *   <li>Serialize the DynamicMessage to Protobuf bytes
 *   <li>Register the Protobuf schema definition with GSR
 *   <li>Prepend GSR header bytes (version + compression + schema UUID) to the Protobuf payload
 * </ol>
 */
@Internal
public class GsrProtobufRowDataSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final String transportName;
    private final String schemaName;
    private final String protobufSchemaDefinition;
    private final Map<String, Object> configs;

    private transient GlueSchemaRegistrySerializationFacade serializationFacade;
    private transient Descriptors.Descriptor messageDescriptor;

    /**
     * Creates a new GSR Protobuf serialization schema.
     *
     * @param rowType the Flink RowType describing the table schema
     * @param transportName the transport name (topic/stream) for GSR schema naming
     * @param schemaName the schema name for GSR registration
     * @param configs the GSR SDK configuration map
     */
    public GsrProtobufRowDataSerializationSchema(
            RowType rowType, String transportName, String schemaName, Map<String, Object> configs) {
        this.rowType = rowType;
        this.transportName = transportName;
        this.schemaName = schemaName != null ? schemaName : transportName;
        this.configs = configs;
        this.protobufSchemaDefinition =
                ProtobufSchemaConverter.convertToProtobufSchema(rowType, this.schemaName);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
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
        if (messageDescriptor == null) {
            messageDescriptor = buildDescriptor();
        }
    }

    @Override
    public byte[] serialize(RowData element) {
        if (element == null) {
            return null;
        }

        DynamicMessage message =
                RowDataToProtobufConverter.convertRowData(element, rowType, messageDescriptor);
        byte[] protobufBytes = message.toByteArray();

        return serializationFacade.encode(
                transportName,
                new Schema(protobufSchemaDefinition, DataFormat.PROTOBUF.name(), schemaName),
                protobufBytes);
    }

    @VisibleForTesting
    void setSerializationFacade(GlueSchemaRegistrySerializationFacade facade) {
        this.serializationFacade = facade;
    }

    @VisibleForTesting
    void setMessageDescriptor(Descriptors.Descriptor descriptor) {
        this.messageDescriptor = descriptor;
    }

    private Descriptors.Descriptor buildDescriptor() {
        try {
            DescriptorProtos.FileDescriptorProto fileProto =
                    ProtobufSchemaConverter.buildFileDescriptorProto(rowType, schemaName);
            Descriptors.FileDescriptor fileDescriptor =
                    Descriptors.FileDescriptor.buildFrom(
                            fileProto, new Descriptors.FileDescriptor[] {});
            return fileDescriptor.findMessageTypeByName(
                    ProtobufSchemaConverter.sanitizeMessageName(schemaName));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException("Failed to build Protobuf descriptor from RowType", e);
        }
    }
}
