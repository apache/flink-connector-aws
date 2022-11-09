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
