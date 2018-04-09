package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Orders {
    @JsonProperty
    @JsonSerialize(keyUsing = OrderNumberId.OrderNumberIdSerializer.class)
    @JsonDeserialize(keyUsing = OrderNumberId.OrderNumberIdDeserializer.class)
    private Map<OrderNumberId,Order> entries = new LinkedHashMap<>();

    public void update(LatestOrder order) {
        if (order.getLatest() != null) {
            entries.put(order.getOrderNumber(), order.getLatest());
        } else {
            entries.remove(order.getOrderNumber());
        }
    }

    @JsonIgnore
    public List<Order> getEntries() {
        return new ArrayList<>(entries.values());
    }

    @Override
    public String toString() {
        return "Orders{" +
               "entries=" + entries +
               '}';
    }
}
