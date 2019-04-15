package com.tdc.kafka.kafka_coffee.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
@Getter @Setter
public class Order {
    private String id;
    private Date placedDate;
    private Date confirmedDate;
    private Date deliveredDate;
    private Date cancelledDate;
    private OrderStatus status;
    private List<Item> items;

    public Order() {
        this.id = UUID.randomUUID().toString();
        this.items = new ArrayList<>();
    }

    public Order(String id, Date placedDate, Date confirmedDate, Date deliveredDate, Date cancelledDate) {
        this.id = id;
        this.placedDate = placedDate;
        this.confirmedDate = confirmedDate;
        this.deliveredDate = deliveredDate;
        this.cancelledDate = cancelledDate;
        this.items = new ArrayList<>();
        validateStatus();
    }

    public Order addItem(Item item) {
        items.add(item);
        return this;
    }

    public void validateStatus() {
        this.status = OrderStatus.PLACED;

        if (confirmedDate != null) {
            validateDates("Placed date should be set before confirmed date", placedDate);
            this.status = OrderStatus.CONFIRMED;
        } else if (deliveredDate != null) {
            validateDates("Placed and confirmed date should be set before confirmed date", placedDate, confirmedDate);
            this.status = OrderStatus.DELIVERED;
        } else if (cancelledDate != null) {
            validateDates("Placed date should be set before confirmed date", placedDate);
            this.status = OrderStatus.CANCELLED;
        }
    }

    private void validateDates(String msg, Date... dates) {
        for (Date date : dates) {
            if (date == null) {
                throw new IllegalStateException(msg);
            }
        }
    }

}
