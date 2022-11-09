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
import org.apache.flink.connector.dynamodb.util.Order;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType.PUT;
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
        Order order = new Order(null, 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);

        assertThat(actual.getItem()).containsOnlyKeys("quantity", "total");
    }

    @Test
    void testConvertOrderToDynamoDbWriteRequestWritesNull() {
        ElementConverter<Order, DynamoDbWriteRequest> elementConverter =
                new DynamoDbBeanElementConverter<>(Order.class, false);
        Order order = new Order(null, 1, 2.0);

        DynamoDbWriteRequest actual = elementConverter.apply(order, null);

        assertThat(actual.getItem()).containsOnlyKeys("orderId", "quantity", "total");
        assertThat(actual.getItem().get("orderId").nul()).isTrue();
    }
}
