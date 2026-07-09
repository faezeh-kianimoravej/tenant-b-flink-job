package com.example.tenantb.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        assertEquals("app-tenant-b-001", config.jobMetadata().getPlatformApplicationId());
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
        assertEquals("app-dev", config.jobMetadata().getPlatformApplicationId());
        assertEquals("tenant-b-dev-enrichment", config.jobMetadata().getApplicationName());
        assertEquals("tenant-b-dev", config.jobMetadata().getTenant());
        assertEquals("team-dev", config.jobMetadata().getOwnerTeam());
        assertEquals("test", config.jobMetadata().getEnvironment());
        assertEquals("retail", config.jobMetadata().getDataDomain());
        assertEquals("ProductEnrichmentJobDev", config.jobMetadata().getJobName());
    }
}
