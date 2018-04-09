package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LatestOrder {
    OrderNumberId orderNumber;
    DefaultId customerId;
    Order latest;

    public LatestOrder() {}

    public LatestOrder(
            @JsonProperty("orderNumber") OrderNumberId orderNumber,
            @JsonProperty("customerId") DefaultId customerId,
            @JsonProperty("latest") Order latest) {
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.latest = latest;
    }

    public void update(Order order, OrderNumberId orderNumber, DefaultId customerId) {
        if (EventType.DELETE == order.get_eventType()) {
            latest = null;
            return;
        }
        latest = order;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
    }

    public OrderNumberId getOrderNumber() {
        return orderNumber;
    }

    public DefaultId getCustomerId() {
        return customerId;
    }

    public Order getLatest() {
        return latest;
    }

    @Override
    public String toString() {
        return "LatestOrder{" +
               "orderNumber=" + orderNumber +
               ", customerId=" + customerId +
               ", latest=" + latest +
               '}';
    }
}
