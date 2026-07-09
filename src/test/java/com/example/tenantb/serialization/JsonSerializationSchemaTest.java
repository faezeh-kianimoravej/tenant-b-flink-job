package com.example.tenantb.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.example.tenantb.model.EnrichedOrder;
import com.example.tenantb.model.Order;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonSerializationSchemaTest {
    @Test
    void deserializesJsonIntoPojo() throws IOException {
        JsonDeserializationSchema<Order> schema = new JsonDeserializationSchema<>(Order.class);

        Order order = schema.deserialize("""
                {
                  "orderId": "order-1",
                  "productId": "product-1",
                  "quantity": 2
                }
                """.getBytes(StandardCharsets.UTF_8));

        assertEquals(new Order("order-1", "product-1", 2), order);
    }

    @Test
    void serializesPojoIntoJson() throws IOException {
        JsonSerializationSchema<EnrichedOrder> serializer = new JsonSerializationSchema<>();
        JsonDeserializationSchema<EnrichedOrder> deserializer = new JsonDeserializationSchema<>(EnrichedOrder.class);
        EnrichedOrder source = new EnrichedOrder(
                "order-1",
                "product-1",
                "Coffee",
                "Beverages",
                4.50,
                2,
                "tenant-b",
                "tenant-b-flink-job");

        byte[] json = serializer.serialize(source);
        EnrichedOrder parsed = deserializer.deserialize(json);

        assertEquals(source, parsed);
    }
}
