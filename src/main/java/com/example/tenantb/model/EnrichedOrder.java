package com.example.tenantb.model;

import java.util.Objects;

/**
 * Enriched order event produced by the Tenant B Flink job.
 *
 * <p>This POJO is the JSON output contract for {@code tenant-b-enriched-orders}. It combines fields from an
 * {@link Order}, product details from {@link Product}, and job metadata that identifies the tenant and processor that
 * emitted the event.</p>
 */
public class EnrichedOrder {
    private String orderId;
    private String productId;
    private String productName;
    private String tenant;
    private String processedBy;

    /**
     * Creates an empty enriched order for frameworks that require a no-argument constructor.
     */
    public EnrichedOrder() {
    }

    /**
     * Creates an enriched order event with all fields populated.
     *
     * @param orderId unique business identifier for the order
     * @param productId product identifier used as the join key
     * @param productName human-readable product name
     * @param tenant tenant identifier that owns the event
     * @param processedBy job identifier that produced the event
     */
    public EnrichedOrder(
            String orderId,
            String productId,
            String productName,
            String tenant,
            String processedBy) {
        this.orderId = orderId;
        this.productId = productId;
        this.productName = productName;
        this.tenant = tenant;
        this.processedBy = processedBy;
    }

    /**
     * Returns the order identifier.
     *
     * @return unique business identifier for the order
     */
    public String getOrderId() {
        return orderId;
    }

    /**
     * Sets the order identifier.
     *
     * @param orderId unique business identifier for the order
     */
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    /**
     * Returns the product identifier used by the join.
     *
     * @return product identifier used as the join key
     */
    public String getProductId() {
        return productId;
    }

    /**
     * Sets the product identifier used by the join.
     *
     * @param productId product identifier used as the join key
     */
    public void setProductId(String productId) {
        this.productId = productId;
    }

    /**
     * Returns the product name included in the enriched output.
     *
     * @return product name or fallback text
     */
    public String getProductName() {
        return productName;
    }

    /**
     * Sets the product name included in the enriched output.
     *
     * @param productName product name or fallback text
     */
    public void setProductName(String productName) {
        this.productName = productName;
    }

    /**
     * Returns the tenant that owns the output event.
     *
     * @return tenant identifier
     */
    public String getTenant() {
        return tenant;
    }

    /**
     * Sets the tenant that owns the output event.
     *
     * @param tenant tenant identifier
     */
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    /**
     * Returns the processor identifier that emitted the output event.
     *
     * @return job or processor identifier
     */
    public String getProcessedBy() {
        return processedBy;
    }

    /**
     * Sets the processor identifier that emitted the output event.
     *
     * @param processedBy job or processor identifier
     */
    public void setProcessedBy(String processedBy) {
        this.processedBy = processedBy;
    }

    /**
     * Compares enriched orders by their serialized output fields.
     *
     * @param o object to compare with this enriched order
     * @return {@code true} when both objects represent the same enriched output payload
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EnrichedOrder that)) {
            return false;
        }
        return Objects.equals(orderId, that.orderId)
                && Objects.equals(productId, that.productId)
                && Objects.equals(productName, that.productName)
                && Objects.equals(tenant, that.tenant)
                && Objects.equals(processedBy, that.processedBy);
    }

    /**
     * Computes a hash code from the enriched output fields.
     *
     * @return hash code for this enriched order
     */
    @Override
    public int hashCode() {
        return Objects.hash(orderId, productId, productName, tenant, processedBy);
    }

    /**
     * Returns a readable representation for logging and diagnostics.
     *
     * @return string representation of this enriched order
     */
    @Override
    public String toString() {
        return "EnrichedOrder{"
                + "orderId='" + orderId + '\''
                + ", productId='" + productId + '\''
                + ", productName='" + productName + '\''
                + ", tenant='" + tenant + '\''
                + ", processedBy='" + processedBy + '\''
                + '}';
    }
}
