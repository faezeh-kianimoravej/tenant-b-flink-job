package com.example.tenantb.config;

import com.example.tenantb.model.JobMetadata;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable runtime configuration for the Tenant B Flink job.
 *
 * <p>The record captures the infrastructure settings needed to connect the job to Kafka and the metadata used to
 * identify the workload on the shared Flink platform. It exists as the boundary between deployment-time arguments
 * and application code, allowing the rest of the pipeline to depend on typed values instead of raw strings.</p>
 *
 * @param tenant logical tenant that owns and runs the job
 * @param bootstrapServers Kafka bootstrap server list used by sources and sinks
 * @param ordersInputTopic Kafka topic containing order events
 * @param productsInputTopic Kafka topic containing product reference events
 * @param outputTopic Kafka topic that receives enriched order events
 * @param consumerGroupId Kafka consumer group used for the order stream; the product stream derives a related group
 * @param jobMetadata descriptive platform metadata for ownership, environment, and job naming
 */
public record FlinkJobConfig(
        String tenant,
        String bootstrapServers,
        String ordersInputTopic,
        String productsInputTopic,
        String outputTopic,
        String consumerGroupId,
        JobMetadata jobMetadata) {

    public static final String DEFAULT_TENANT = "tenant-b";
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";
    public static final String DEFAULT_ORDERS_INPUT_TOPIC = "tenant-b-orders";
    public static final String DEFAULT_PRODUCTS_INPUT_TOPIC = "tenant-b-products";
    public static final String DEFAULT_OUTPUT_TOPIC = "tenant-b-enriched-orders";
    public static final String DEFAULT_CONSUMER_GROUP_ID = "tenant-b-flink-job";
    public static final String DEFAULT_PLATFORM_APPLICATION_ID = "app-tenant-b-001";
    public static final String DEFAULT_APPLICATION_NAME = "tenant-b-product-enrichment";
    public static final String DEFAULT_OWNER_TEAM = "tenant-b";
    public static final String DEFAULT_ENVIRONMENT = "dev";
    public static final String DEFAULT_DATA_DOMAIN = "orders";
    public static final String DEFAULT_JOB_NAME = "ProductEnrichmentJob";

    /**
     * Creates a configuration using the default Tenant B local/platform values.
     *
     * <p>The defaults target the shared in-cluster Kafka service and the Tenant B topics:
     * {@code tenant-b-orders}, {@code tenant-b-products}, and {@code tenant-b-enriched-orders}.</p>
     *
     * @return a complete Tenant B configuration with default Kafka settings and metadata
     */
    public static FlinkJobConfig defaults() {
        JobMetadata metadata = new JobMetadata(
                DEFAULT_PLATFORM_APPLICATION_ID,
                DEFAULT_APPLICATION_NAME,
                DEFAULT_TENANT,
                DEFAULT_OWNER_TEAM,
                DEFAULT_ENVIRONMENT,
                DEFAULT_DATA_DOMAIN,
                DEFAULT_JOB_NAME);

        return new FlinkJobConfig(
                DEFAULT_TENANT,
                DEFAULT_BOOTSTRAP_SERVERS,
                DEFAULT_ORDERS_INPUT_TOPIC,
                DEFAULT_PRODUCTS_INPUT_TOPIC,
                DEFAULT_OUTPUT_TOPIC,
                DEFAULT_CONSUMER_GROUP_ID,
                metadata);
    }

    /**
     * Parses command-line arguments into a complete job configuration.
     *
     * <p>Supported arguments use the {@code --key=value} format. Unknown arguments are ignored by design so the job can
     * be launched by environments that add unrelated parameters. Missing values fall back to the documented Tenant B
     * defaults.</p>
     *
     * @param args command-line arguments, usually passed from {@code main}
     * @return a complete configuration with command-line overrides applied
     */
    public static FlinkJobConfig fromArgs(String[] args) {
        Map<String, String> values = parseArgs(args);
        String tenant = values.getOrDefault("tenant", DEFAULT_TENANT);

        JobMetadata metadata = new JobMetadata(
                values.getOrDefault("platform.application.id", DEFAULT_PLATFORM_APPLICATION_ID),
                values.getOrDefault("application.name", DEFAULT_APPLICATION_NAME),
                tenant,
                values.getOrDefault("owner.team", DEFAULT_OWNER_TEAM),
                values.getOrDefault("environment", DEFAULT_ENVIRONMENT),
                values.getOrDefault("data.domain", DEFAULT_DATA_DOMAIN),
                values.getOrDefault("job.name", DEFAULT_JOB_NAME));

        return new FlinkJobConfig(
                tenant,
                values.getOrDefault("kafka.bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS),
                values.getOrDefault("orders.input.topic", DEFAULT_ORDERS_INPUT_TOPIC),
                values.getOrDefault("products.input.topic", DEFAULT_PRODUCTS_INPUT_TOPIC),
                values.getOrDefault("output.topic", DEFAULT_OUTPUT_TOPIC),
                values.getOrDefault("consumer.group.id", DEFAULT_CONSUMER_GROUP_ID),
                metadata);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> values = new HashMap<>();
        if (args == null) {
            return values;
        }

        for (String arg : args) {
            if (arg == null || !arg.startsWith("--")) {
                continue;
            }

            int separatorIndex = arg.indexOf('=');
            if (separatorIndex <= 2 || separatorIndex == arg.length() - 1) {
                continue;
            }

            values.put(arg.substring(2, separatorIndex), arg.substring(separatorIndex + 1));
        }
        return values;
    }
}
