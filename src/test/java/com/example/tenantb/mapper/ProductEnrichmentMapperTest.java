package com.example.tenantb.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.example.tenantb.model.EnrichedOrder;
import com.example.tenantb.model.Order;
import com.example.tenantb.model.Product;
import org.junit.jupiter.api.Test;

class ProductEnrichmentMapperTest {
    @Test
    void enrichesOrderWithMatchingProduct() {
        Order order = new Order("order-1", "product-1", 2);
        Product product = new Product("product-1", "Coffee", "Beverages", 4.50);

        EnrichedOrder enrichedOrder = ProductEnrichmentMapper.enrich(
                order,
                product,
                "tenant-b",
                "tenant-b-flink-job");

        assertEquals("order-1", enrichedOrder.getOrderId());
        assertEquals("product-1", enrichedOrder.getProductId());
        assertEquals("Coffee", enrichedOrder.getProductName());
        assertEquals("Beverages", enrichedOrder.getCategory());
        assertEquals(4.50, enrichedOrder.getPrice());
        assertEquals(2, enrichedOrder.getQuantity());
        assertEquals("tenant-b", enrichedOrder.getTenant());
        assertEquals("tenant-b-flink-job", enrichedOrder.getProcessedBy());
    }

    @Test
    void fallsBackWhenProductHasNotArrivedYet() {
        Order order = new Order("order-2", "product-missing", 1);

        EnrichedOrder enrichedOrder = ProductEnrichmentMapper.enrich(
                order,
                null,
                "tenant-b",
                "tenant-b-flink-job");

        assertEquals("order-2", enrichedOrder.getOrderId());
        assertEquals("product-missing", enrichedOrder.getProductId());
        assertEquals("unknown product name", enrichedOrder.getProductName());
        assertEquals("unknown category", enrichedOrder.getCategory());
        assertNull(enrichedOrder.getPrice());
        assertEquals(1, enrichedOrder.getQuantity());
    }
}
