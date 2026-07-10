package com.example.tenantb.mapper;

import com.example.tenantb.model.EnrichedOrder;
import com.example.tenantb.model.Order;
import com.example.tenantb.model.Product;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stateful enrichment function that joins order events with product reference data.
 *
 * <p>The function receives a keyed order stream and a keyed product stream connected by {@code productId}. Product
 * records are stored in keyed state, and each incoming order is enriched with the product currently known for that key.
 * This keeps the prototype simple while demonstrating a second two-stream join pattern on the shared Flink platform.</p>
 */
public class ProductEnrichmentMapper extends KeyedCoProcessFunction<String, Order, Product, EnrichedOrder> {
    private final String tenant;
    private final String processedBy;
    private transient ValueState<Product> productState;
    private transient ListState<Order> pendingOrders;

    /**
     * Creates the mapper with the metadata fields added to every enriched output record.
     *
     * @param tenant tenant identifier written to the enriched order
     * @param processedBy job or consumer group identifier written to the enriched order
     */
    public ProductEnrichmentMapper(String tenant, String processedBy) {
        this.tenant = tenant;
        this.processedBy = processedBy;
    }

    /**
     * Initializes keyed product state for the currently executing Flink task.
     *
     * <p>The state is scoped by the key selected in {@code ProductEnrichmentJob}; in this job that key is
     * {@code productId}. Each key therefore stores the latest product record for one product.</p>
     *
     * @param openContext Flink lifecycle context for the operator instance
     */
    @Override
    public void open(OpenContext openContext) {
        productState = getRuntimeContext().getState(new ValueStateDescriptor<>("product-by-id", Product.class));
        pendingOrders = getRuntimeContext().getListState(
                new ListStateDescriptor<>("pending-orders-by-product-id", Order.class));
    }

    /**
     * Processes an order event and emits an enriched order.
     *
     * <p>If product state is already available for the order's {@code productId}, the enriched order is emitted
     * immediately. Otherwise, the order is kept in state until the product record arrives.</p>
     *
     * @param order order event from the orders Kafka topic
     * @param context Flink processing context for the order stream
     * @param collector output collector for enriched order events
     * @throws Exception if keyed state cannot be read
     */
    @Override
    public void processElement1(Order order, Context context, Collector<EnrichedOrder> collector) throws Exception {
        Product product = productState.value();
        if (product == null) {
            pendingOrders.add(order);
            return;
        }
        collector.collect(enrich(order, product, tenant, processedBy));
    }

    /**
     * Processes a product reference event and stores it in keyed state.
     *
     * <p>Future orders with the same {@code productId} key will use this product data during enrichment.</p>
     *
     * @param product product reference event from the products Kafka topic
     * @param context Flink processing context for the product stream
     * @param collector output collector; unused because product events update state only
     * @throws Exception if keyed state cannot be updated
     */
    @Override
    public void processElement2(Product product, Context context, Collector<EnrichedOrder> collector) throws Exception {
        productState.update(product);
        for (Order pendingOrder : pendingOrders.get()) {
            collector.collect(enrich(pendingOrder, product, tenant, processedBy));
        }
        pendingOrders.clear();
    }

    /**
     * Builds an enriched order from an order and optional product information.
     *
     * <p>This static helper contains the pure mapping logic used by the stateful Flink operator and unit tests. Keeping
     * the transformation separate from Flink state access makes the enrichment contract easy to verify.</p>
     *
     * @param order source order event
     * @param product product reference data for the same {@code productId}
     * @param tenant tenant identifier to include in the output event
     * @param processedBy job identifier to include in the output event
     * @return enriched order event ready for Kafka serialization
     */
    public static EnrichedOrder enrich(Order order, Product product, String tenant, String processedBy) {
        return new EnrichedOrder(
                order.getOrderId(),
                order.getProductId(),
                product.getName(),
                tenant,
                processedBy);
    }
}
