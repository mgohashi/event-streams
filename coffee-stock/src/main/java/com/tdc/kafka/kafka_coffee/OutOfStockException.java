package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Order;

public class OutOfStockException extends Exception {

    private Order order;

    public OutOfStockException(String message, Order order) {
        this.order = order;
    }

    public Order getOrder() {
        return order;
    }
}
