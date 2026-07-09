package com.example.tenantb.model;

import java.util.Objects;

/**
 * Product reference event consumed from the Tenant B products Kafka topic.
 *
 * <p>This POJO represents the product side of the enrichment join. Product events are keyed by {@code productId} and
 * stored in Flink keyed state so incoming orders can be decorated with the latest known product name, category, and
 * price.</p>
 */
public class Product {
    private String productId;
    private String productName;
    private String category;
    private Double price;

    /**
     * Creates an empty product for frameworks that require a no-argument constructor.
     */
    public Product() {
    }

    /**
     * Creates a product reference event with all fields populated.
     *
     * @param productId product identifier used as the join key
     * @param productName human-readable product name
     * @param category product category used for downstream grouping or reporting
     * @param price product unit price
     */
    public Product(String productId, String productName, String category, Double price) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.price = price;
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
    public String getProductName() {
        return productName;
    }

    /**
     * Sets the human-readable product name.
     *
     * @param productName product name
     */
    public void setProductName(String productName) {
        this.productName = productName;
    }

    /**
     * Returns the product category.
     *
     * @return product category
     */
    public String getCategory() {
        return category;
    }

    /**
     * Sets the product category.
     *
     * @param category product category
     */
    public void setCategory(String category) {
        this.category = category;
    }

    /**
     * Returns the product unit price.
     *
     * @return product unit price
     */
    public Double getPrice() {
        return price;
    }

    /**
     * Sets the product unit price.
     *
     * @param price product unit price
     */
    public void setPrice(Double price) {
        this.price = price;
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
                && Objects.equals(productName, product.productName)
                && Objects.equals(category, product.category)
                && Objects.equals(price, product.price);
    }

    /**
     * Computes a hash code from the product payload fields.
     *
     * @return hash code for this product
     */
    @Override
    public int hashCode() {
        return Objects.hash(productId, productName, category, price);
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
                + ", productName='" + productName + '\''
                + ", category='" + category + '\''
                + ", price=" + price
                + '}';
    }
}
