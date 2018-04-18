package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.ArrayList;
import java.util.List;

public class CustomerAggregate {

    private final Customer customer;

    private List<Address> addresses = new ArrayList<>();

    private List<Order> orders = new ArrayList<>();

    @JsonCreator
    public CustomerAggregate(@JsonProperty("customer") Customer customer) {
        this.customer = customer;
    }

    public Customer getCustomer() {
        return customer;
    }

    public List<Address> getAddresses() {
        return addresses;
    }

    @JsonSetter("addresses")
    public void setAddresses(List<Address> addresses) {
        if (this.addresses.isEmpty()) {
            this.addresses.addAll(addresses);
        }
    }

    public List<Order> getOrders() {
        return orders;
    }

    @JsonSetter("orders")
    public void setOrders(List<Order> orders) {
        if (this.orders.isEmpty()) {
            this.orders.addAll(orders);
        }
    }

    @Override
    public String toString() {
        return "CustomerAggregate{" +
               "customer=" + customer +
               ", addresses=" + addresses +
               ", orders=" + orders +
               '}';
    }
}
