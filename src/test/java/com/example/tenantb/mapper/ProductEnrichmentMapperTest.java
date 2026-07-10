package com.example.tenantb.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.example.tenantb.model.EnrichedOrder;
import com.example.tenantb.model.Order;
import com.example.tenantb.model.Product;
import org.junit.jupiter.api.Test;

class ProductEnrichmentMapperTest {
    @Test
    void enrichesOrderWithMatchingProduct() {
        Order order = new Order("order-1", "product-1");
        Product product = new Product("product-1", "Coffee");

        EnrichedOrder enrichedOrder = ProductEnrichmentMapper.enrich(
                order,
                product,
                "tenant-b",
                "tenant-b-flink-job");

        assertEquals("order-1", enrichedOrder.getOrderId());
        assertEquals("product-1", enrichedOrder.getProductId());
        assertEquals("Coffee", enrichedOrder.getProductName());
        assertEquals("tenant-b", enrichedOrder.getTenant());
        assertEquals("tenant-b-flink-job", enrichedOrder.getProcessedBy());
    }
}
