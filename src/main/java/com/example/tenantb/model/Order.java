package com.example.tenantb.model;

import java.util.Objects;

/**
 * Order event consumed from the Tenant B orders Kafka topic.
 *
 * <p>This POJO is part of the external JSON contract for {@code tenant-b-orders}. It exists as the order side of the
 * product enrichment join and is intentionally implemented as a classic Java bean so Jackson and Flink can serialize,
 * deserialize, and inspect it reliably.</p>
 */
public class Order {
    private String orderId;
    private String productId;

    /**
     * Creates an empty order for frameworks that require a no-argument constructor.
     */
    public Order() {
    }

    /**
     * Creates an order event with all fields populated.
     *
     * @param orderId unique business identifier for the order
     * @param productId product identifier used as the join key
     */
    public Order(String orderId, String productId) {
        this.orderId = orderId;
        this.productId = productId;
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
     * Returns the product identifier used to join with product reference data.
     *
     * @return product identifier used as the join key
     */
    public String getProductId() {
        return productId;
    }

    /**
     * Sets the product identifier used to join with product reference data.
     *
     * @param productId product identifier used as the join key
     */
    public void setProductId(String productId) {
        this.productId = productId;
    }

    /**
     * Compares orders by their serialized business fields.
     *
     * @param o object to compare with this order
     * @return {@code true} when both objects represent the same order payload
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Order order)) {
            return false;
        }
        return Objects.equals(orderId, order.orderId)
                && Objects.equals(productId, order.productId);
    }

    /**
     * Computes a hash code from the order payload fields.
     *
     * @return hash code for this order
     */
    @Override
    public int hashCode() {
        return Objects.hash(orderId, productId);
    }

    /**
     * Returns a readable representation for logging and diagnostics.
     *
     * @return string representation of this order
     */
    @Override
    public String toString() {
        return "Order{"
                + "orderId='" + orderId + '\''
                + ", productId='" + productId + '\''
                + '}';
    }
}
