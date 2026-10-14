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

package org.apache.flink.connector.dynamodb.testutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.dynamodb.source.serialization.DynamoDbStreamsDeserializationSchema;
import org.apache.flink.util.Collector;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.util.HashMap;
import java.util.Map;

/** An example {@link DynamoDbStreamsDeserializationSchema} which for DDB Streams Source IT Case. */
public class DynamoDbStreamsSourceITDeserializationSchema
        implements DynamoDbStreamsDeserializationSchema<DynamoDbStreamsSourceITEvent> {

    @Override
    public void deserialize(
            Record record,
            String stream,
            String shardId,
            Collector<DynamoDbStreamsSourceITEvent> output) {

        String eventType = record.eventNameAsString();
        String sequenceNumber = record.dynamodb().sequenceNumber();
        Map<String, AttributeValue> oldImage = new HashMap<>(record.dynamodb().oldImage());
        Map<String, AttributeValue> newImage = new HashMap<>(record.dynamodb().newImage());

        DynamoDbStreamsSourceITEvent event =
                new DynamoDbStreamsSourceITEvent(
                        shardId, eventType, sequenceNumber, oldImage, newImage);
        output.collect(event);
    }

    @Override
    public TypeInformation<DynamoDbStreamsSourceITEvent> getProducedType() {
        return TypeInformation.of(DynamoDbStreamsSourceITEvent.class);
    }
}
