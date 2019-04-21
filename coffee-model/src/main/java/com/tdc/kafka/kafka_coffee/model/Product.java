package com.tdc.kafka.kafka_coffee.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter @Setter
public class Product {
    private String id;
    private String name;
    private Long amount;
    private String unit;
}
