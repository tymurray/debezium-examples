package io.debezium.examples.aggregation;

import static java.util.Optional.ofNullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.debezium.examples.aggregation.model.CustomerAggregate;
import io.debezium.examples.aggregation.model.LatestOrder;
import io.debezium.examples.aggregation.model.Order;
import io.debezium.examples.aggregation.model.OrderNumberId;
import io.debezium.examples.aggregation.model.Orders;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

import io.debezium.examples.aggregation.model.Address;
import io.debezium.examples.aggregation.model.Addresses;
import io.debezium.examples.aggregation.model.Customer;
import io.debezium.examples.aggregation.model.DefaultId;
import io.debezium.examples.aggregation.model.EventType;
import io.debezium.examples.aggregation.model.LatestAddress;
import io.debezium.examples.aggregation.serdes.SerdeFactory;

public class StreamingAggregatesDDD {

    public static void main(String[] args) {

        if(args.length != 4) {
            System.err.println("usage: java -jar <package> "
                    + StreamingAggregatesDDD.class.getName() + " <parent_topic> <addresses_topic> <orders_topic> <bootstrap_servers>");
            System.exit(-1);
        }

        final String parentTopic = args[0];
        final String addressesTopic = args[1];
        final String ordersTopic = args[2];
        final String bootstrapServers = args[3];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-aggregates-ddd");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<DefaultId> defaultIdSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(DefaultId.class,true);
        final Serde<OrderNumberId> orderNumberIdSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(OrderNumberId.class,true);
        final Serde<Customer> customerSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Customer.class,false);
        final Serde<Address> addressSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Address.class,false);
        final Serde<Order> orderSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Order.class,false);
        final Serde<LatestAddress> latestAddressSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(LatestAddress.class,false);
        final Serde<LatestOrder> latestOrderSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(LatestOrder.class,false);
        final Serde<Addresses> addressesSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Addresses.class,false);
        final Serde<Orders> ordersSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Orders.class,false);
        final Serde<CustomerAggregate> aggregateSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(CustomerAggregate.class, false);

        StreamsBuilder builder = new StreamsBuilder();

        KTable<DefaultId, Customer> customerTable = builder.table(parentTopic, Consumed.with(defaultIdSerde,customerSerde));
        KStream<DefaultId, Address> addressStream = builder.stream(addressesTopic, Consumed.with(defaultIdSerde, addressSerde));
        KStream<OrderNumberId, Order> orderStream = builder.stream(ordersTopic, Consumed.with(orderNumberIdSerde, orderSerde));

        // Address Aggregate Tables
        KTable<DefaultId,LatestAddress> tempAddressTable = addressStream
            .groupByKey(Serialized.with(defaultIdSerde, addressSerde))
            .aggregate(
                LatestAddress::new,
                (DefaultId addressId, Address address, LatestAddress latest) -> {
                    latest.update(address,addressId,new DefaultId(address.getCustomer_id()));
                    return latest;
                },
                Materialized.<DefaultId,LatestAddress,KeyValueStore<Bytes, byte[]>>
                    as(addressesTopic+"_table_temp")
                    .withKeySerde(defaultIdSerde)
                    .withValueSerde(latestAddressSerde)
            );

        KTable<DefaultId, Addresses> addressTable = tempAddressTable.toStream()
            .map((addressId, latestAddress) -> new KeyValue<>(latestAddress.getCustomerId(),latestAddress))
            .groupByKey(Serialized.with(defaultIdSerde,latestAddressSerde))
            .aggregate(
                Addresses::new,
                (customerId, latestAddress, addresses) -> {
                    addresses.update(latestAddress);
                    return addresses;
                },
                Materialized.<DefaultId,Addresses,KeyValueStore<Bytes, byte[]>>
                    as(addressesTopic+"_table_aggregate")
                    .withKeySerde(defaultIdSerde)
                    .withValueSerde(addressesSerde)
            );

        // Order Aggregate Tables
        KTable<OrderNumberId, LatestOrder> tempOrderTable = orderStream
            .groupByKey(Serialized.with(orderNumberIdSerde, orderSerde))
            .aggregate(
                LatestOrder::new,
                (OrderNumberId orderId, Order order, LatestOrder latest) -> {
                    latest.update(order, orderId, new DefaultId(order.getPurchaser()));
                    return latest;
                },
                Materialized.<OrderNumberId, LatestOrder, KeyValueStore<Bytes, byte[]>>
                    as(ordersTopic + "_table_temp")
                    .withKeySerde(orderNumberIdSerde)
                    .withValueSerde(latestOrderSerde)
            );

        KTable<DefaultId, Orders> orderTable = tempOrderTable.toStream()
            .map((orderNumberId, latestOrder) -> new KeyValue<>(latestOrder.getCustomerId(), latestOrder))
            .groupByKey(Serialized.with(defaultIdSerde, latestOrderSerde))
            .aggregate(
                Orders::new,
                (customerId, latestOrder, orders) -> {
                    orders.update(latestOrder);
                    return orders;
                },
                Materialized.<DefaultId, Orders, KeyValueStore<Bytes, byte[]>>
                    as(ordersTopic + "_table_aggregate")
                    .withKeySerde(defaultIdSerde)
                    .withValueSerde(ordersSerde)
            );

        // Final Customer Aggregate
        KTable<DefaultId,CustomerAggregate> dddAggregate =
            customerTable.mapValues(customer -> {
                if (customer.get_eventType() == EventType.DELETE) {
                    return null;
                }
                return new CustomerAggregate(customer);
            }).leftJoin(addressTable, (customerAgg, addresses) -> {
                final List<Address> addressList = ofNullable(addresses).map(Addresses::getEntries).orElseGet(ArrayList::new);

                System.out.println("JOIN ADDRESSES ---- " + addressList);
                return ofNullable(customerAgg)
                    .map(customer -> {
                        customer.setAddresses(addressList);
                        return customer;
                    })
                    .orElse(null);
            })
            .leftJoin(orderTable, (customerAgg, orders) -> {
                final List<Order> orderList = ofNullable(orders).map(Orders::getEntries).orElseGet(ArrayList::new);

                System.out.println("JOIN ORDERS ---- " + orderList);
                return ofNullable(customerAgg)
                    .map(customer -> {
                        customer.setOrders(orderList);
                        return customer;
                    })
                    .orElse(null);
                }
            );

        dddAggregate.toStream().to("final_ddd_aggregates", Produced.with(defaultIdSerde, aggregateSerde));
        dddAggregate.toStream().print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
