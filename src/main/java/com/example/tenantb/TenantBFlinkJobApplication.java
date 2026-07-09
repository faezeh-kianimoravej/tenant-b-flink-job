package com.example.tenantb;

import com.example.tenantb.config.FlinkJobConfig;
import com.example.tenantb.job.ProductEnrichmentJob;
import com.example.tenantb.model.JobMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line entry point for the Tenant B Flink application.
 *
 * <p>This class owns application bootstrap only: it parses runtime configuration, records the startup metadata
 * that identifies the tenant-owned job, creates the Flink pipeline object, and delegates execution. Keeping the
 * entry point thin makes the job logic easier to test and keeps platform concerns separate from Tenant B's
 * stream-processing use case.</p>
 */
public final class TenantBFlinkJobApplication {
    private static final Logger LOG = LoggerFactory.getLogger(TenantBFlinkJobApplication.class);

    private TenantBFlinkJobApplication() {
    }

    /**
     * Starts the Tenant B product enrichment job.
     *
     * <p>Arguments are parsed as {@code --key=value} pairs by {@link FlinkJobConfig#fromArgs(String[])}. Any omitted
     * values fall back to the Tenant B defaults, including Kafka topics, consumer group, and metadata fields.</p>
     *
     * @param args command-line arguments supplied by the Flink runtime or local launcher
     * @throws Exception if the Flink pipeline cannot be submitted or executed
     */
    public static void main(String[] args) throws Exception {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(args);
        JobMetadata metadata = config.jobMetadata();

        LOG.info("Starting Tenant B Flink job with metadata: {}", metadata);
        LOG.info(
                "Kafka topics configured: ordersInputTopic={}, productsInputTopic={}, outputTopic={}",
                config.ordersInputTopic(),
                config.productsInputTopic(),
                config.outputTopic());

        ProductEnrichmentJob job = new ProductEnrichmentJob(config, metadata);
        job.execute();
    }
}
