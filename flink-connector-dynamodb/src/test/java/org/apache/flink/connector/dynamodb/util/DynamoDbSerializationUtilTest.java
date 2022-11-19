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

package org.apache.flink.connector.dynamodb.util;

import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for {@link DynamoDbSerializationUtil}. */
public class DynamoDbSerializationUtilTest {

    @Test
    public void testPutItemSerializeDeserialize() throws IOException {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("string", AttributeValue.builder().s("string").build());
        item.put("number", AttributeValue.builder().s("123.4212").build());
        item.put(
                "binary",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
        item.put("null", AttributeValue.builder().nul(true).build());
        item.put("boolean", AttributeValue.builder().bool(true).build());
        item.put("stringSet", AttributeValue.builder().ss("s1", "s2").build());
        item.put("numberSet", AttributeValue.builder().ns("123.1231", "123123").build());
        item.put(
                "binarySet",
                AttributeValue.builder()
                        .bs(
                                SdkBytes.fromByteArray(new byte[] {1, 2, 3}),
                                SdkBytes.fromByteArray(new byte[] {4, 5, 6}))
                        .build());
        item.put(
                "list",
                AttributeValue.builder()
                        .l(
                                AttributeValue.builder().s("s1").build(),
                                AttributeValue.builder().s("s2").build())
                        .build());
        item.put(
                "map",
                AttributeValue.builder()
                        .m(
                                ImmutableMap.of(
                                        "key1", AttributeValue.builder().s("string").build(),
                                        "key2", AttributeValue.builder().n("12345").build(),
                                        "binary",
                                                AttributeValue.builder()
                                                        .b(
                                                                SdkBytes.fromByteArray(
                                                                        new byte[] {1, 2, 3}))
                                                        .build(),
                                        "null", AttributeValue.builder().nul(true).build()))
                        .build());
        DynamoDbWriteRequest dynamoDbWriteRequest =
                DynamoDbWriteRequest.builder()
                        .setItem(item)
                        .setType(DynamoDbWriteRequestType.PUT)
                        .build();

        byte[] serialized;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(outputStream)) {
            DynamoDbSerializationUtil.serializeWriteRequest(dynamoDbWriteRequest, out);
            serialized = outputStream.toByteArray();
        }

        try (InputStream inputStream = new ByteArrayInputStream(serialized);
                DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            DynamoDbWriteRequest deserializedWriteRequest =
                    DynamoDbSerializationUtil.deserializeWriteRequest(dataInputStream);
            assertThat(deserializedWriteRequest).isEqualTo(dynamoDbWriteRequest);
        }
    }

    @Test
    public void testDeleteItemSerializeDeserialize() throws IOException {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("string", AttributeValue.builder().s("string").build());
        key.put("number", AttributeValue.builder().s("123.4212").build());
        DynamoDbWriteRequest dynamoDbWriteRequest =
                DynamoDbWriteRequest.builder()
                        .setItem(key)
                        .setType(DynamoDbWriteRequestType.DELETE)
                        .build();

        byte[] serialized;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(outputStream)) {
            DynamoDbSerializationUtil.serializeWriteRequest(dynamoDbWriteRequest, out);
            serialized = outputStream.toByteArray();
        }

        try (InputStream inputStream = new ByteArrayInputStream(serialized);
                DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            DynamoDbWriteRequest deserializedWriteRequest =
                    DynamoDbSerializationUtil.deserializeWriteRequest(dataInputStream);
            assertThat(deserializedWriteRequest).isEqualTo(dynamoDbWriteRequest);
        }
    }

    @Test
    public void testSerializeEmptyAttributeValueThrowsException() {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("empty", AttributeValue.builder().build());
        DynamoDbWriteRequest dynamoDbWriteRequest =
                DynamoDbWriteRequest.builder()
                        .setItem(item)
                        .setType(DynamoDbWriteRequestType.PUT)
                        .build();

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(outputStream)) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(
                            () ->
                                    DynamoDbSerializationUtil.serializeWriteRequest(
                                            dynamoDbWriteRequest, out))
                    .withMessageContaining("Attribute value must not be empty: AttributeValue()");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
