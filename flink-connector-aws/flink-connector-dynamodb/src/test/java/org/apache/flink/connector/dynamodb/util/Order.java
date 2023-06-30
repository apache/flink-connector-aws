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

import org.apache.flink.connector.dynamodb.sink.DynamoDbBeanElementConverter;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

/** A test {@link DynamoDbBean} POJO for use with {@link DynamoDbBeanElementConverter}. */
@DynamoDbBean
public class Order {

    private String orderId;
    private int quantity;
    private double total;

    public Order() {}

    public Order(String orderId, int quantity, double total) {
        this.orderId = orderId;
        this.quantity = quantity;
        this.total = total;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }
}
