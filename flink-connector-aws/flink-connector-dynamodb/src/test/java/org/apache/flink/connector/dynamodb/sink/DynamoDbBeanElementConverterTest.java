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

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.testutils.TestItem;
import org.apache.flink.connector.dynamodb.util.Order;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;

import static org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType.DELETE;
import static org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType.PUT;
import static org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType.UPDATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class DynamoDbBeanElementConverterTest {

    @Test
    void testBadType() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new DynamoDbBeanElementConverter<>(Integer.class))
                .withMessageContaining(
                        "A DynamoDb bean class must be annotated with @DynamoDbBean");
    }

    @Test
    void testConvertOrderToDynamoDbWriteRequest() {
        ElementConverter<Order, DynamoDbWriteRequest> elementConverter =
                new DynamoDbBeanElementConverter<>(Order.class);
        elementConverter.open(null);
        Order order = new Order("orderId", 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);

        assertThat(actual.getType()).isEqualTo(PUT);
        assertThat(actual.getItem()).containsOnlyKeys("orderId", "quantity", "total");
        assertThat(actual.getItem().get("orderId").s()).isEqualTo("orderId");
        assertThat(actual.getItem().get("quantity").n()).isEqualTo("1");
        assertThat(actual.getItem().get("total").n()).isEqualTo("2.0");
    }

    @Test
    void testConvertOrderToDynamoDbWriteRequestWithIgnoresNull() {
        ElementConverter<Order, DynamoDbWriteRequest> elementConverter =
                new DynamoDbBeanElementConverter<>(Order.class, true);
        elementConverter.open(null);
        Order order = new Order(null, 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);

        assertThat(actual.getItem()).containsOnlyKeys("quantity", "total");
    }

    @Test
    void testConvertOrderToDynamoDbWriteRequestWritesNull() {
        ElementConverter<Order, DynamoDbWriteRequest> elementConverter =
                new DynamoDbBeanElementConverter<>(Order.class, false);
        elementConverter.open(null);
        Order order = new Order(null, 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);

        assertThat(actual.getItem()).containsOnlyKeys("orderId", "quantity", "total");
        assertThat(actual.getItem().get("orderId").nul()).isTrue();
    }

    @Test
    void testConvertWithClosedConvertedThrowsException() {
        ElementConverter<Order, DynamoDbWriteRequest> elementConverter =
                new DynamoDbBeanElementConverter<>(Order.class);
        Order order = new Order(null, 1, 2.0);

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> elementConverter.apply(order, null))
                .withMessageContaining("Table schema has not been initialized");
    }

    @Test
    void testConditionalPutSetsConditionExpression() {
        ElementConverter<TestItem, DynamoDbWriteRequest> elementConverter =
                DynamoDbBeanElementConverter.builder(TestItem.class)
                        .setType(PUT)
                        .setConditionExpression("attribute_not_exists(pk)")
                        .setExpressionAttributeNames(Collections.singletonMap("#pk", "pk"))
                        .build();
        elementConverter.open(null);
        TestItem item = new TestItem("1", "a", "data", "0");

        DynamoDbWriteRequest actual = elementConverter.apply(item, null);

        assertThat(actual.getType()).isEqualTo(PUT);
        assertThat(actual.getConditionExpression()).isEqualTo("attribute_not_exists(pk)");
        assertThat(actual.getExpressionAttributeNames()).containsEntry("#pk", "pk");
        assertThat(actual.getItem()).containsKeys("pk", "sk", "payload", "counter");
    }

    @Test
    void testDeleteExtractsOnlyKeyAttributes() {
        ElementConverter<TestItem, DynamoDbWriteRequest> elementConverter =
                DynamoDbBeanElementConverter.builder(TestItem.class)
                        .setType(DELETE)
                        .build();
        elementConverter.open(null);
        TestItem item = new TestItem("1", "a", "data", "0");

        DynamoDbWriteRequest actual = elementConverter.apply(item, null);

        assertThat(actual.getType()).isEqualTo(DELETE);
        assertThat(actual.getItem()).containsOnlyKeys("pk", "sk");
        assertThat(actual.getItem().get("pk").s()).isEqualTo("1");
        assertThat(actual.getItem().get("sk").s()).isEqualTo("a");
    }

    @Test
    void testUpdateExtractsKeyAndSetsUpdateExpression() {
        ElementConverter<TestItem, DynamoDbWriteRequest> elementConverter =
                DynamoDbBeanElementConverter.builder(TestItem.class)
                        .setType(UPDATE)
                        .setUpdateExpression("SET #c = :val")
                        .setExpressionAttributeNames(Collections.singletonMap("#c", "counter"))
                        .setExpressionAttributeValues(
                                Collections.singletonMap(
                                        ":val", AttributeValue.builder().s("42").build()))
                        .build();
        elementConverter.open(null);
        TestItem item = new TestItem("1", "a", "data", "0");

        DynamoDbWriteRequest actual = elementConverter.apply(item, null);

        assertThat(actual.getType()).isEqualTo(UPDATE);
        assertThat(actual.getUpdateExpression()).isEqualTo("SET #c = :val");
        assertThat(actual.getItem()).containsOnlyKeys("pk", "sk");
        assertThat(actual.getExpressionAttributeNames()).containsEntry("#c", "counter");
        assertThat(actual.getExpressionAttributeValues())
                .containsEntry(":val", AttributeValue.builder().s("42").build());
    }

    @Test
    void testUpdateWithoutUpdateExpressionThrows() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                DynamoDbBeanElementConverter.builder(TestItem.class)
                                        .setType(UPDATE)
                                        .build())
                .withMessageContaining("updateExpression is required for UPDATE type");
    }

    @Test
    void testPutWithUpdateExpressionThrows() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                DynamoDbBeanElementConverter.builder(TestItem.class)
                                        .setType(PUT)
                                        .setUpdateExpression("SET #a = :val")
                                        .build())
                .withMessageContaining("updateExpression is only allowed for UPDATE type");
    }
}
