package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Item;
import com.tdc.kafka.kafka_coffee.model.Order;

import java.math.BigDecimal;

public class BaseTest {
    protected Order createOrder() {
        Order order = new Order();
        Item item1 = new Item();
        item1.setProdId("1");
        item1.setValue(BigDecimal.TEN.setScale(2));
        item1.setAmount(1);
        order.addItem(item1);

        Item item2 = new Item();
        item2.setProdId("2");
        item2.setValue(BigDecimal.TEN.setScale(2));
        item2.setAmount(1);
        order.addItem(item2);
        order.setTotal(item1.getValue().add(item2.getValue()));
        return order;
    }
}
