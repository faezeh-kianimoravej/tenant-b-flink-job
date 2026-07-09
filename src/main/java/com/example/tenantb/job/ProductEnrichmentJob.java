package com.example.tenantb.job;

import com.example.tenantb.config.FlinkJobConfig;
import com.example.tenantb.kafka.KafkaSinkFactory;
import com.example.tenantb.kafka.KafkaSourceFactory;
import com.example.tenantb.mapper.ProductEnrichmentMapper;
import com.example.tenantb.model.EnrichedOrder;
import com.example.tenantb.model.JobMetadata;
import com.example.tenantb.model.Order;
import com.example.tenantb.model.Product;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Builds and executes the Tenant B product enrichment Flink pipeline.
 *
 * <p>The job is the application layer that connects configuration, Kafka integration, and enrichment logic. It reads
 * order events and product reference events from separate Kafka topics, joins them by {@code productId}, and writes
 * enriched order events back to Kafka. The implementation intentionally uses the Flink DataStream API directly so the
 * data flow remains visible for reviewers and future operators of the shared platform.</p>
 */
public class ProductEnrichmentJob {
    private final FlinkJobConfig config;
    private final JobMetadata metadata;

    /**
     * Creates a product enrichment job from runtime configuration and platform metadata.
     *
     * @param config Kafka topics, bootstrap servers, tenant, and consumer group settings
     * @param metadata platform metadata used for the Flink execution name and startup context
     */
    public ProductEnrichmentJob(FlinkJobConfig config, JobMetadata metadata) {
        this.config = config;
        this.metadata = metadata;
    }

    /**
     * Creates the Flink execution environment, builds the streaming topology, and submits the job.
     *
     * <p>{@link StreamExecutionEnvironment} is Flink's entry point for constructing a DataStream pipeline. The call to
     * {@link StreamExecutionEnvironment#execute(String)} hands the completed topology to the configured Flink runtime.</p>
     *
     * @throws Exception if Flink cannot build, submit, or execute the job graph
     */
    public void execute() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(environment);
        environment.execute(metadata.getJobName());
    }

    /**
     * Wires the Kafka-backed product enrichment topology into the provided Flink environment.
     *
     * <p>The pipeline has five main steps:</p>
     * <ol>
     *     <li>Create Kafka sources for orders and products using the configured topics.</li>
     *     <li>Create the Kafka sink for enriched orders.</li>
     *     <li>Convert the sources into Flink {@link DataStream} instances without event-time watermarks because this
     *     prototype enriches records as they arrive.</li>
     *     <li>Key both streams by {@code productId}, connect them, and run the stateful enrichment function.</li>
     *     <li>Write enriched records to the configured output topic.</li>
     * </ol>
     *
     * @param environment Flink environment that owns the DataStream topology
     */
    void buildPipeline(StreamExecutionEnvironment environment) {
        // Kafka sources define the external input streams for Tenant B's two-stream join.
        KafkaSource<Order> ordersSource = KafkaSourceFactory.createOrdersSource(config);
        KafkaSource<Product> productsSource = KafkaSourceFactory.createProductsSource(config);

        // The sink serializes enriched orders as JSON for downstream consumers.
        KafkaSink<EnrichedOrder> sink = KafkaSinkFactory.createEnrichedOrderSink(config);

        DataStream<Order> orders = environment.fromSource(
                ordersSource,
                WatermarkStrategy.noWatermarks(),
                config.ordersInputTopic());

        DataStream<Product> products = environment.fromSource(
                productsSource,
                WatermarkStrategy.noWatermarks(),
                config.productsInputTopic());

        // Keyed connect keeps product state scoped by productId before enrichment emits to Kafka.
        orders.keyBy(ProductEnrichmentJob::orderProductKey)
                .connect(products.keyBy(ProductEnrichmentJob::productKey))
                .process(new ProductEnrichmentMapper(config.tenant(), config.consumerGroupId()))
                .name("tenant-b-product-enrichment")
                .sinkTo(sink)
                .name(config.outputTopic());
    }

    private static String orderProductKey(Order order) {
        return order == null || order.getProductId() == null ? "" : order.getProductId();
    }

    private static String productKey(Product product) {
        return product == null || product.getProductId() == null ? "" : product.getProductId();
    }
}
