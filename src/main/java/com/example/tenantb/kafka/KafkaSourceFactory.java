package com.example.tenantb.kafka;

import com.example.tenantb.config.FlinkJobConfig;
import com.example.tenantb.model.Order;
import com.example.tenantb.model.Product;
import com.example.tenantb.serialization.JsonDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Factory for Kafka sources used by the Tenant B Flink pipeline.
 *
 * <p>This class centralizes Kafka source construction so topic names, bootstrap servers, consumer group conventions,
 * startup offsets, and JSON deserialization are configured consistently. The job layer can therefore focus on stream
 * topology instead of connector setup.</p>
 */
public final class KafkaSourceFactory {
    private KafkaSourceFactory() {
    }

    /**
     * Creates the Kafka source that reads {@link Order} events.
     *
     * <p>The source starts at the earliest available offset, which is useful for reproducible prototype runs and local
     * demonstrations where topics may already contain sample data.</p>
     *
     * @param config runtime configuration containing Kafka connection settings and the orders topic
     * @return a configured Kafka source for Tenant B orders
     */
    public static KafkaSource<Order> createOrdersSource(FlinkJobConfig config) {
        return KafkaSource.<Order>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.ordersInputTopic())
                .setGroupId(config.consumerGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Order.class))
                .build();
    }

    /**
     * Creates the Kafka source that reads {@link Product} reference events.
     *
     * <p>The product stream uses a related consumer group suffix so it can maintain independent offsets from the order
     * stream while still being clearly associated with the same Tenant B job.</p>
     *
     * @param config runtime configuration containing Kafka connection settings and the products topic
     * @return a configured Kafka source for product reference data
     */
    public static KafkaSource<Product> createProductsSource(FlinkJobConfig config) {
        return KafkaSource.<Product>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.productsInputTopic())
                .setGroupId(config.consumerGroupId() + "-products")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Product.class))
                .build();
    }
}
