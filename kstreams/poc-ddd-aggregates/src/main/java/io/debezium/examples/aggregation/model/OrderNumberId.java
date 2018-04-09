package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Objects;

public class OrderNumberId {
    @JsonIgnore
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static class OrderNumberIdDeserializer extends KeyDeserializer {

        @Override
        public OrderNumberId deserializeKey(String key, DeserializationContext ctx) throws IOException {
            return OBJECT_MAPPER.readValue(key, OrderNumberId.class);
        }
    }

    public static class OrderNumberIdSerializer extends JsonSerializer<OrderNumberId> {
        @Override
        public void serialize(OrderNumberId key, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
                gen.writeFieldName(OBJECT_MAPPER.writeValueAsString(key));
        }
    }

    private final Integer order_number;

    @JsonCreator
    public OrderNumberId(@JsonProperty("order_number") Integer order_number) {
        this.order_number = order_number;
    }

    public Integer getOrder_number() {
        return order_number;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrderNumberId that = (OrderNumberId)o;
        return Objects.equals(order_number, that.order_number);
    }

    @Override
    public int hashCode() {
        return Objects.hash(order_number);
    }

    @Override
    public String toString() {
        return "OrderNumberId{" +
               "order_number=" + order_number +
               '}';
    }
}
