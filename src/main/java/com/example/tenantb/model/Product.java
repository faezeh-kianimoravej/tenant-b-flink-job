package com.example.tenantb.model;

import java.util.Objects;

/**
 * Product reference event consumed from the Tenant B products Kafka topic.
 */
public class Product {
    private String productId;
    private String name;

    /**
     * Creates an empty product for frameworks that require a no-argument constructor.
     */
    public Product() {
    }

    /**
     * Creates a product reference event.
     *
     * @param productId product identifier used as the join key
     * @param name product display name
     */
    public Product(String productId, String name) {
        this.productId = productId;
        this.name = name;
    }

    /**
     * Returns the product identifier.
     *
     * @return product identifier used as the join key
     */
    public String getProductId() {
        return productId;
    }

    /**
     * Sets the product identifier.
     *
     * @param productId product identifier used as the join key
     */
    public void setProductId(String productId) {
        this.productId = productId;
    }

    /**
     * Returns the human-readable product name.
     *
     * @return product name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the human-readable product name.
     *
     * @param name product name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Compares products by their serialized reference-data fields.
     *
     * @param o object to compare with this product
     * @return {@code true} when both objects represent the same product payload
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Product product)) {
            return false;
        }
        return Objects.equals(productId, product.productId)
                && Objects.equals(name, product.name);
    }

    /**
     * Computes a hash code from the product payload fields.
     *
     * @return hash code for this product
     */
    @Override
    public int hashCode() {
        return Objects.hash(productId, name);
    }

    /**
     * Returns a readable representation for logging and diagnostics.
     *
     * @return string representation of this product
     */
    @Override
    public String toString() {
        return "Product{"
                + "productId='" + productId + '\''
                + ", name='" + name + '\''
                + '}';
    }
}
