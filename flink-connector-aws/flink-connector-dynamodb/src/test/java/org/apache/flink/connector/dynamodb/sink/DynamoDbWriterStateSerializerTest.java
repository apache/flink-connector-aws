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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.dynamodb.util.DynamoDbSerializationUtil;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests for serializing and deserialzing a collection of {@link DynamoDbWriteRequest} with {@link
 * DynamoDbWriterStateSerializer}.
 */
public class DynamoDbWriterStateSerializerTest {

    private static final ElementConverter<String, DynamoDbWriteRequest> ELEMENT_CONVERTER =
            (element, context) ->
                    DynamoDbWriteRequest.builder()
                            .setType(DynamoDbWriteRequestType.PUT)
                            .setItem(
                                    singletonMap(
                                            "key", AttributeValue.builder().s(element).build()))
                            .build();

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<DynamoDbWriteRequest> expectedState =
                getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        DynamoDbWriterStateSerializer serializer = new DynamoDbWriterStateSerializer();
        BufferedRequestState<DynamoDbWriteRequest> actualState =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(expectedState));

        assertThat(actualState).usingRecursiveComparison().isEqualTo(expectedState);
    }

    @Test
    public void testSerializeAndDeserializeUpdateRequest() throws IOException {
        Map<String, AttributeValue> key =
                singletonMap("pk", AttributeValue.builder().s("key1").build());
        DynamoDbWriteRequest updateRequest =
                DynamoDbWriteRequest.builder()
                        .setType(DynamoDbWriteRequestType.UPDATE)
                        .setItem(key)
                        .setUpdateExpression("SET #c = :val")
                        .setExpressionAttributeNames(singletonMap("#c", "counter"))
                        .setExpressionAttributeValues(
                                singletonMap(":val", AttributeValue.builder().n("1").build()))
                        .build();

        BufferedRequestState<DynamoDbWriteRequest> expectedState =
                new BufferedRequestState<>(
                        Collections.singletonList(new RequestEntryWrapper<>(updateRequest, 100)));

        DynamoDbWriterStateSerializer serializer = new DynamoDbWriterStateSerializer();
        BufferedRequestState<DynamoDbWriteRequest> actualState =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(expectedState));

        assertThat(actualState).usingRecursiveComparison().isEqualTo(expectedState);
    }

    @Test
    public void testSerializeAndDeserializeConditionalPut() throws IOException {
        DynamoDbWriteRequest conditionalPut =
                DynamoDbWriteRequest.builder()
                        .setType(DynamoDbWriteRequestType.PUT)
                        .setItem(singletonMap("pk", AttributeValue.builder().s("key1").build()))
                        .setConditionExpression("attribute_not_exists(pk)")
                        .setExpressionAttributeNames(singletonMap("#pk", "pk"))
                        .build();

        BufferedRequestState<DynamoDbWriteRequest> expectedState =
                new BufferedRequestState<>(
                        Collections.singletonList(new RequestEntryWrapper<>(conditionalPut, 100)));

        DynamoDbWriterStateSerializer serializer = new DynamoDbWriterStateSerializer();
        BufferedRequestState<DynamoDbWriteRequest> actualState =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(expectedState));

        assertThat(actualState).usingRecursiveComparison().isEqualTo(expectedState);
    }

    @Test
    public void testDeserializeVersion1State() throws IOException {
        BufferedRequestState<DynamoDbWriteRequest> v1State =
                getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        DynamoDbWriterStateSerializerV1 v1Serializer = new DynamoDbWriterStateSerializerV1();
        byte[] v1Bytes = v1Serializer.serialize(v1State);

        DynamoDbWriterStateSerializer v2Serializer = new DynamoDbWriterStateSerializer();
        BufferedRequestState<DynamoDbWriteRequest> actualState =
                v2Serializer.deserialize(1, v1Bytes);

        assertThat(actualState).usingRecursiveComparison().isEqualTo(v1State);
    }

    @Test
    public void testVersion() {
        DynamoDbWriterStateSerializer serializer = new DynamoDbWriterStateSerializer();
        assertThat(serializer.getVersion()).isEqualTo(2);
    }

    private int getRequestSize(DynamoDbWriteRequest requestEntry) {
        return requestEntry.getItem().toString().getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * Simulates version 1 serializer that only writes type + item (no expression fields). Used to
     * test backward compatibility of the version 2 deserializer.
     */
    private static class DynamoDbWriterStateSerializerV1
            extends AsyncSinkWriterStateSerializer<DynamoDbWriteRequest> {

        @Override
        protected void serializeRequestToStream(
                DynamoDbWriteRequest request, java.io.DataOutputStream out)
                throws IOException {
            // V1 format: only type byte + item map (no expression fields)
            out.writeByte(request.getType().toByteValue());
            // Write item map: size + entries (key + attribute value)
            Map<String, AttributeValue> item = request.getItem();
            out.writeInt(item.size());
            for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeByte(0);
                out.writeUTF(entry.getValue().s());
            }
        }

        @Override
        protected DynamoDbWriteRequest deserializeRequestFromStream(
                long requestSize, java.io.DataInputStream in) throws IOException {
            return DynamoDbSerializationUtil.deserializeWriteRequest(in, 1);
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }
}
