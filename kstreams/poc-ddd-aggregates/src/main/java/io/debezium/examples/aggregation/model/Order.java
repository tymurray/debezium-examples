package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Order {
    private final EventType _eventType;

    private final Integer order_number;
    private final String order_date;
    private final Integer purchaser;
    private final Integer quantity;
    private final Integer product_id;

    @JsonCreator
    public Order(
            @JsonProperty("_eventType") EventType _eventType,
            @JsonProperty("order_number") Integer order_number,
            @JsonProperty("order_date") String order_date,
            @JsonProperty("purchaser") Integer purchaser,
            @JsonProperty("quantity") Integer quantity,
            @JsonProperty("product_id") Integer product_id) {
        this._eventType = _eventType == null ? EventType.UPSERT : _eventType;
        this.order_number = order_number;
        this.order_date = order_date;
        this.purchaser = purchaser;
        this.quantity = quantity;
        this.product_id = product_id;
    }

    public EventType get_eventType() {
        return _eventType;
    }

    public Integer getOrder_number() {
        return order_number;
    }

    public String getOrder_date() {
        return order_date;
    }

    public Integer getPurchaser() {
        return purchaser;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public Integer getProduct_id() {
        return product_id;
    }

    @Override
    public String toString() {
        return "Order{" +
               "_eventType=" + _eventType +
               ", order_number=" + order_number +
               ", order_date='" + order_date + '\'' +
               ", purchaser=" + purchaser +
               ", quantity=" + quantity +
               ", product_id=" + product_id +
               '}';
    }
}
