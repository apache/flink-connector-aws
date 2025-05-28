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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * An implementation of the {@link ElementConverter} that uses the AWS SQS SDK v2. The user only
 * needs to provide a {@link SerializationSchema} of the {@code InputT} to transform it into a
 * {@link SendMessageBatchRequestEntry} that may be persisted.
 */
@Internal
public class SqsSinkElementConverter<InputT>
        implements ElementConverter<InputT, SendMessageBatchRequestEntry> {

    /** A serialization schema to specify how the input element should be serialized. */
    private final SerializationSchema<InputT> serializationSchema;

    private SqsSinkElementConverter(SerializationSchema<InputT> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Override
    public SendMessageBatchRequestEntry apply(InputT element, SinkWriter.Context context) {
        final byte[] messageBody = serializationSchema.serialize(element);
        return SendMessageBatchRequestEntry.builder()
                .id(UUID.randomUUID().toString())
                .messageBody(new String(messageBody, StandardCharsets.UTF_8))
                .build();
    }

    @Override
    public void open(WriterInitContext context) {
        try {
            serializationSchema.open(context.asSerializationSchemaInitializationContext());
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
        }
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the SqsSinkElementConverter. */
    public static class Builder<InputT> {

        private SerializationSchema<InputT> serializationSchema;

        public Builder<InputT> setSerializationSchema(
                SerializationSchema<InputT> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        public SqsSinkElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    serializationSchema,
                    "No SerializationSchema was supplied to the SQS Sink builder.");
            return new SqsSinkElementConverter<>(serializationSchema);
        }
    }
}
