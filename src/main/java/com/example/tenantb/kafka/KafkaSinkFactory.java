package com.example.tenantb.kafka;

import com.example.tenantb.config.FlinkJobConfig;
import com.example.tenantb.model.EnrichedOrder;
import com.example.tenantb.serialization.JsonSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Factory for Kafka sinks used by the Tenant B Flink pipeline.
 *
 * <p>The sink factory keeps Kafka output configuration in one place and ensures the enriched order contract is written
 * as JSON. This mirrors the source factory pattern and keeps connector details outside the job topology class.</p>
 */
public final class KafkaSinkFactory {
    private KafkaSinkFactory() {
    }

    /**
     * Creates the Kafka sink that writes {@link EnrichedOrder} events to the configured output topic.
     *
     * @param config runtime configuration containing Kafka connection settings and the enriched output topic
     * @return a configured Kafka sink for enriched orders
     */
    public static KafkaSink<EnrichedOrder> createEnrichedOrderSink(FlinkJobConfig config) {
        return KafkaSink.<EnrichedOrder>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaClientProperties())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.outputTopic())
                        .setValueSerializationSchema(new JsonSerializationSchema<EnrichedOrder>())
                        .build())
                .build();
    }
}
