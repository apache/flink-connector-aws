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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Unit tests for {@link PrimaryKeyBuilder}. */
public class PrimaryKeyBuilderTest {

    private static final String PARTITION_KEY_NAME = "part_key_name";
    private static final String SORT_KEY_NAME = "sort_key_name";

    private Map<String, AttributeValue> createItemValues() {
        final Map<String, AttributeValue> values = new HashMap<>();
        values.put(
                PARTITION_KEY_NAME,
                AttributeValue.builder()
                        .s("123")
                        .n("456")
                        .b(SdkBytes.fromString("789", StandardCharsets.UTF_8))
                        .build());

        values.put(SORT_KEY_NAME, AttributeValue.builder().s("101112").build());

        values.put("some_item", AttributeValue.builder().bool(false).build());

        return values;
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
        Map<String, AttributeValue> itemValuesOne = new HashMap<>();
        itemValuesOne.put(PARTITION_KEY_NAME, AttributeValue.builder().s("ab").build());
        itemValuesOne.put(SORT_KEY_NAME, AttributeValue.builder().s("cd").build());

        WriteRequest putRequestOne = createPutItemRequest(itemValuesOne);

        Map<String, AttributeValue> itemValuesTwo = new HashMap<>();
        itemValuesTwo.put(PARTITION_KEY_NAME, AttributeValue.builder().s("a").build());
        itemValuesTwo.put(SORT_KEY_NAME, AttributeValue.builder().s("bcd").build());

        WriteRequest putRequestTwo = createPutItemRequest(itemValuesTwo);

        PrimaryKeyBuilder keyBuilder =
                new PrimaryKeyBuilder(Arrays.asList(PARTITION_KEY_NAME, SORT_KEY_NAME));

        assertThat(keyBuilder.build(putRequestOne)).isNotEqualTo(keyBuilder.build(putRequestTwo));
    }

    @Test
    public void testPartitionKeysOfTwoDifferentRequestsEqual() {
        PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder(singletonList(PARTITION_KEY_NAME));
        assertThat(keyBuilder.build(createPutItemRequest(createItemValues())))
                .isEqualTo(keyBuilder.build(createDeleteItemRequest(createItemValues())));
    }

    @Test
    public void testCompositeKeysOfTwoDifferentRequestsEqual() {
        PrimaryKeyBuilder keyBuilder =
                new PrimaryKeyBuilder(Arrays.asList(PARTITION_KEY_NAME, SORT_KEY_NAME));

        assertThat(keyBuilder.build(createPutItemRequest(createItemValues())))
                .isEqualTo(keyBuilder.build(createDeleteItemRequest(createItemValues())));
    }

    @Test
    public void testExceptOnEmptyPartitionKeys() {
        assertThatExceptionOfType(InvalidConfigurationException.class)
                .isThrownBy(() -> new PrimaryKeyBuilder(emptyList()))
                .withMessageContaining(
                        "Unable to construct partition key as overwriteByPartitionKeys configuration not provided.");
    }

    @Test
    public void testExceptOnEmptyWriteRequest() {
        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(singletonList(PARTITION_KEY_NAME))
                                        .build(WriteRequest.builder().build()))
                .withMessageContaining("Empty write request");
    }

    @Test
    public void testExceptOnEmptyPutRequest() {
        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(singletonList(PARTITION_KEY_NAME))
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
                                new PrimaryKeyBuilder(singletonList(PARTITION_KEY_NAME))
                                        .build(
                                                WriteRequest.builder()
                                                        .deleteRequest(
                                                                DeleteRequest.builder().build())
                                                        .build()))
                .withMessageContaining("does not contain request key.");
    }

    @Test
    public void testExceptWhenNoPartitionKey() {
        Map<String, AttributeValue> itemValues =
                singletonMap("some_item", AttributeValue.builder().bool(false).build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(singletonList(PARTITION_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining("does not contain partition key part_key_name.");
    }

    @Test
    public void testExceptWhenEmptyKey() {
        Map<String, AttributeValue> itemValues =
                singletonMap(PARTITION_KEY_NAME, AttributeValue.builder().s("").build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(singletonList(PARTITION_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining(
                        "Partition key or sort key attributes require non-empty values.");
    }

    @Test
    public void testExceptWhenNoPartitionKeyCompositeKey() {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(SORT_KEY_NAME, AttributeValue.builder().s("101112").build());
        itemValues.put("some_item", AttributeValue.builder().bool(false).build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(singletonList(PARTITION_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining("does not contain partition key part_key_name.");
    }

    @Test
    public void testExceptWhenNoSortKey() {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(PARTITION_KEY_NAME, AttributeValue.builder().s("101112").build());
        itemValues.put("some_item", AttributeValue.builder().bool(false).build());

        assertThatExceptionOfType(InvalidRequestException.class)
                .isThrownBy(
                        () ->
                                new PrimaryKeyBuilder(
                                                Arrays.asList(PARTITION_KEY_NAME, SORT_KEY_NAME))
                                        .build(createPutItemRequest(itemValues)))
                .withMessageContaining("does not contain partition key sort_key_name.");
    }
}
