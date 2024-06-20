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

package org.apache.flink.connector.dynamodb.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.dynamodb.source.serialization.DynamoDbStreamsDeserializationSchema;
import org.apache.flink.util.Collector;

import software.amazon.awssdk.services.dynamodb.model.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * An example {@link DynamoDbStreamsDeserializationSchema} which retrieves the sequence number from
 * a DynamoDB Streams {@link Record}.
 */
public class ExampleDynamoDbStreamsDeserializationSchema
        implements DynamoDbStreamsDeserializationSchema<String> {

    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<String> output)
            throws IOException {
        STRING_SCHEMA.deserialize(
                record.dynamodb().sequenceNumber().getBytes(StandardCharsets.UTF_8), output);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
