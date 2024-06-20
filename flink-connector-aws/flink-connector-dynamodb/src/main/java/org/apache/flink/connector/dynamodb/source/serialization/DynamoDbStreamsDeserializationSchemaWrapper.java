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

package org.apache.flink.connector.dynamodb.source.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A simple wrapper for using the {@link DeserializationSchema} with the {@link
 * DynamoDbStreamsDeserializationSchema} interface.
 *
 * @param <T> The type created by the deserialization schema.
 */
@Internal
class DynamoDbStreamsDeserializationSchemaWrapper<T> implements DynamoDbStreamsDeserializationSchema<T> {

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private final DeserializationSchema<T> deserializationSchema;

    DynamoDbStreamsDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.deserializationSchema.open(context);
    }

    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<T> output)
            throws IOException {
        deserializationSchema.deserialize(MAPPER.writeValueAsString(record)
                .getBytes(StandardCharsets.UTF_8), output);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
