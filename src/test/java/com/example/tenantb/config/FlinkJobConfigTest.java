package com.example.tenantb.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.junit.jupiter.api.Test;

class FlinkJobConfigTest {
    @Test
    void defaultsMatchTenantBTopicsAndMetadata() {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(new String[0]);

        assertEquals("tenant-b", config.tenant());
        assertEquals("kafka:9092", config.bootstrapServers());
        assertEquals("tenant-b-orders", config.ordersInputTopic());
        assertEquals("tenant-b-products", config.productsInputTopic());
        assertEquals("tenant-b-enriched-orders", config.outputTopic());
        assertEquals("tenant-b-flink-job", config.consumerGroupId());
        assertEquals("tenant-b-flink-user", config.kafkaUser());
        assertEquals("", config.kafkaSecurityProtocol());
        assertEquals("", config.kafkaSaslMechanism());
        assertEquals("", config.kafkaUsername());
        assertEquals("", config.kafkaPassword());
        assertTrue(config.kafkaClientProperties().isEmpty());
        assertEquals("APP-TENANT-B", config.jobMetadata().getPlatformApplicationId());
        assertEquals("tenant-b-product-enrichment", config.jobMetadata().getApplicationName());
        assertEquals("tenant-b", config.jobMetadata().getTenant());
        assertEquals("tenant-b", config.jobMetadata().getOwnerTeam());
        assertEquals("dev", config.jobMetadata().getEnvironment());
        assertEquals("orders", config.jobMetadata().getDataDomain());
        assertEquals("ProductEnrichmentJob", config.jobMetadata().getJobName());
    }

    @Test
    void commandLineArgsOverrideDefaults() {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(new String[] {
                "--tenant=tenant-b-dev",
                "--kafka.bootstrap.servers=localhost:9092",
                "--orders.input.topic=orders-dev",
                "--products.input.topic=products-dev",
                "--output.topic=enriched-dev",
                "--consumer.group.id=consumer-dev",
                "--platform.application.id=app-dev",
                "--kafka.user=tenant-b-dev-user",
                "--kafka.security.protocol=SASL_PLAINTEXT",
                "--kafka.sasl.mechanism=SCRAM-SHA-512",
                "--kafka.username=tenant-b-dev-user",
                "--kafka.password=secret",
                "--application.name=tenant-b-dev-enrichment",
                "--owner.team=team-dev",
                "--environment=test",
                "--data.domain=retail",
                "--job.name=ProductEnrichmentJobDev"
        });

        assertEquals("tenant-b-dev", config.tenant());
        assertEquals("localhost:9092", config.bootstrapServers());
        assertEquals("orders-dev", config.ordersInputTopic());
        assertEquals("products-dev", config.productsInputTopic());
        assertEquals("enriched-dev", config.outputTopic());
        assertEquals("consumer-dev", config.consumerGroupId());
        assertEquals("tenant-b-dev-user", config.kafkaUser());
        assertEquals("SASL_PLAINTEXT", config.kafkaSecurityProtocol());
        assertEquals("SCRAM-SHA-512", config.kafkaSaslMechanism());
        assertEquals("app-dev", config.jobMetadata().getPlatformApplicationId());
        assertEquals("tenant-b-dev-enrichment", config.jobMetadata().getApplicationName());
        assertEquals("tenant-b-dev", config.jobMetadata().getTenant());
        assertEquals("team-dev", config.jobMetadata().getOwnerTeam());
        assertEquals("test", config.jobMetadata().getEnvironment());
        assertEquals("retail", config.jobMetadata().getDataDomain());
        assertEquals("ProductEnrichmentJobDev", config.jobMetadata().getJobName());

        Properties kafkaProperties = config.kafkaClientProperties();
        assertEquals("SASL_PLAINTEXT", kafkaProperties.getProperty("security.protocol"));
        assertEquals("SCRAM-SHA-512", kafkaProperties.getProperty("sasl.mechanism"));
        assertEquals(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"tenant-b-dev-user\" password=\"secret\";",
                kafkaProperties.getProperty("sasl.jaas.config"));
    }
}
