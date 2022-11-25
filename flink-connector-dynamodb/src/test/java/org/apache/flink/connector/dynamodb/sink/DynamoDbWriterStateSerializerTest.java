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

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
                                    ImmutableMap.of(
                                            "key", AttributeValue.builder().s(element).build()))
                            .build();

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<DynamoDbWriteRequest> expectedState =
                getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        DynamoDbWriterStateSerializer serializer = new DynamoDbWriterStateSerializer();
        BufferedRequestState<DynamoDbWriteRequest> actualState =
                serializer.deserialize(1, serializer.serialize(expectedState));

        assertThat(actualState).usingRecursiveComparison().isEqualTo(expectedState);
    }

    @Test
    public void testVersion() {
        DynamoDbWriterStateSerializer serializer = new DynamoDbWriterStateSerializer();
        assertThat(serializer.getVersion()).isEqualTo(1);
    }

    private int getRequestSize(DynamoDbWriteRequest requestEntry) {
        return requestEntry.getItem().toString().getBytes(StandardCharsets.UTF_8).length;
    }
}
