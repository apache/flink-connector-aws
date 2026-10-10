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
import org.apache.flink.connector.aws.util.AWSGeneralUtil;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

/**
 * Production {@link GsrJsonReader} backed by {@link GlueSchemaRegistryDeserializationFacade}.
 *
 * <p>The facade strips the GSR header and, when the writer compressed the payload, decompresses it
 * in-library (review finding C1). The facade itself is not serializable and is built lazily on each
 * task manager from the serializable config map, mirroring the Protobuf format's {@code
 * FacadeGsrProtobufReader}.
 */
@Internal
final class FacadeGsrJsonReader implements GsrJsonReader {

    private static final long serialVersionUID = 1L;

    private final Map<String, Object> configs;

    private transient GlueSchemaRegistryDeserializationFacade facade;

    FacadeGsrJsonReader(Map<String, Object> configs) {
        this.configs = configs;
    }

    private GlueSchemaRegistryDeserializationFacade facade() {
        if (facade == null) {
            AwsCredentialsProvider credentialsProvider =
                    AWSGeneralUtil.getCredentialsProvider(configs);
            facade =
                    GlueSchemaRegistryDeserializationFacade.builder()
                            .credentialProvider(credentialsProvider)
                            .configs(configs)
                            .build();
        }
        return facade;
    }

    @Override
    public byte[] actualData(byte[] gsrEncoded) {
        return facade().getActualData(gsrEncoded);
    }
}
