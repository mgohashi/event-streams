package com.tdc.kafka.kafka_coffee.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.UUID;

@AllArgsConstructor
@Getter @Setter
public class Item {
    private String id;
    private String prodId;
    private Integer amount;
    private BigDecimal value;

    public Item() {
        id = UUID.randomUUID().toString();
    }

    public void setValue(BigDecimal value) {
        if (value != null)
            this.value = value.setScale(2);
        else
            this.value = null;
    }
}
