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

import org.apache.flink.connector.dynamodb.sink.InvalidConfigurationException;
import org.apache.flink.connector.dynamodb.sink.InvalidRequestException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Unit tests for {@link PrimaryKeyBuilder}. */
public class PrimaryKeyBuilderTest {

    private static final String PARTITION_KEY_NAME = "part_key_name";
    private static final String SORT_KEY_NAME = "sort_key_name";

    private ImmutableMap<String, AttributeValue> createItemValues() {
        return ImmutableMap.of(
                PARTITION_KEY_NAME,
                AttributeValue.builder()
                        .s("123")
                        .n("456")
                        .b(SdkBytes.fromString("789", StandardCharsets.UTF_8))
                        .build(),
                SORT_KEY_NAME,
                AttributeValue.builder().s("101112").build(),
                "some_item",
                AttributeValue.builder().bool(false).build());
    }

    public WriteRequest createPutItemRequest(Map<String, AttributeValue> itemValues) {
        return WriteRequest.builder()
                .putRequest(PutRequest.builder().item(itemValues).build())
                .build();
    }

    public WriteRequest createDeleteItemRequest(Map<String, AttributeValue> itemValues) {
        return WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder().key(itemValues).build())
                .build();
    }

    @Test
    public void testPrimaryKeyDelimited() {
        WriteRequest putRequestOne =
                createPutItemRequest(
                        ImmutableMap.of(
                                PARTITION_KEY_NAME,
                                AttributeValue.builder().s("ab").build(),
                                SORT_KEY_NAME,
                                AttributeValue.builder().s("cd").build()));

        WriteRequest putRequestTwo =
                createPutItemRequest(
                        ImmutableMap.of(
                                PARTITION_KEY_NAME,
                                AttributeValue.builder().s("a").build(),
                                SORT_KEY_NAME,
                                AttributeValue.builder().s("bcd").build()));

        PrimaryKeyBuilder keyBuilder =
                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME, SORT_KEY_NAME));
        assertThat(keyBuilder.build(putRequestOne)).isNotEqualTo(keyBuilder.build(putRequestTwo));
    }

    @Test
    public void testPartitionKeysOfTwoDifferentRequestsEqual() {
        PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME));
        assertThat(keyBuilder.build(createPutItemRequest(createItemValues())))
                .isEqualTo(keyBuilder.build(createDeleteItemRequest(createItemValues())));
    }

    @Test
    public void testCompositeKeysOfTwoDifferentRequestsEqual() {
        PrimaryKeyBuilder keyBuilder =
                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME, SORT_KEY_NAME));

        assertThat(keyBuilder.build(createPutItemRequest(createItemValues())))
                .isEqualTo(keyBuilder.build(createDeleteItemRequest(createItemValues())));
    }

    @Test
    public void testExceptOnEmptyPartitionKeys() {
        assertThatExceptionOfType(InvalidConfigurationException.class)
                .isThrownBy(() -> new PrimaryKeyBuilder(ImmutableList.of()))
                .withMessageContaining(
                        "Unable to construct partition key as overwriteByPartitionKeys configuration not provided.");
    }

    @Test
    public void testExceptOnEmptyWriteRequest() {
        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME))
                                        .build(WriteRequest.builder().build()))
                .withMessageContaining("Empty write request");
    }

    @Test
    public void testExceptOnEmptyPutRequest() {
        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME))
                                        .build(
                                                WriteRequest.builder()
                                                        .putRequest(PutRequest.builder().build())
                                                        .build()))
                .withMessageContaining("does not contain request items.");
    }

    @Test
    public void testExceptOnEmptyDeleteRequest() {
        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME))
                                        .build(
                                                WriteRequest.builder()
                                                        .deleteRequest(
                                                                DeleteRequest.builder().build())
                                                        .build()))
                .withMessageContaining("does not contain request key.");
    }

    @Test
    public void testExceptWhenNoPartitionKey() {
        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of("some_item", AttributeValue.builder().bool(false).build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining("does not contain partition key part_key_name.");
    }

    @Test
    public void testExceptWhenEmptyKey() {
        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of(PARTITION_KEY_NAME, AttributeValue.builder().s("").build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining(
                        "Partition key or sort key attributes require non-empty values.");
    }

    @Test
    public void testExceptWhenNoPartitionKeyCompositeKey() {
        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of(
                        SORT_KEY_NAME,
                        AttributeValue.builder().s("101112").build(),
                        "some_item",
                        AttributeValue.builder().bool(false).build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(ImmutableList.of(PARTITION_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining("does not contain partition key part_key_name.");
    }

    @Test
    public void testExceptWhenNoSortKey() {
        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of(
                        PARTITION_KEY_NAME,
                        AttributeValue.builder().s("101112").build(),
                        "some_item",
                        AttributeValue.builder().bool(false).build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(
                                                ImmutableList.of(PARTITION_KEY_NAME, SORT_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining("does not contain partition key sort_key_name.");
    }
}
