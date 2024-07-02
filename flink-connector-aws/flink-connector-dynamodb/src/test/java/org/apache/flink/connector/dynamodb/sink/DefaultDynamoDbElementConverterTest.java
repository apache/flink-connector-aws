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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.connector.dynamodb.util.Order;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test for {@link DefaultDynamoDbElementConverter}. */
class DefaultDynamoDbElementConverterTest {

    @Test
    void defaultConverterFallsBackToInformedConverter() {
        DefaultDynamoDbElementConverter<Order> converter = new DefaultDynamoDbElementConverter<>();
        Order order = new Order("order-1", 1, 100.0);

        DynamoDbWriteRequest request = converter.apply(order, null);
        assertThat(converter).hasNoNullFieldsOrProperties();
        assertThat(request.getItem()).isNotNull();
        assertThat(request.getItem().get("orderId").s()).isEqualTo("order-1");
        assertThat(request.getItem().get("quantity").n()).isEqualTo("1");
        assertThat(request.getItem().get("total").n()).isEqualTo("100.0");
    }

    @Test
    void defaultConverterThrowsExceptionForNonCompositeType() {
        DefaultDynamoDbElementConverter<String> converter = new DefaultDynamoDbElementConverter<>();
        String str = "test";

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> converter.apply(str, null))
                .withMessage("The input type must be a CompositeType.");
    }
}
