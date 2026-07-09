package com.example.tenantb.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Generic Jackson-backed JSON serialization schema for Flink Kafka sinks.
 *
 * <p>The schema converts Tenant B output POJOs into Kafka message values. It is intentionally generic so the same
 * infrastructure can be reused for future Tenant B output contracts while keeping the sink factory concise.</p>
 *
 * @param <T> source POJO type accepted by the schema
 */
public class JsonSerializationSchema<T> implements SerializationSchema<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Converts a model object into JSON bytes for Kafka.
     *
     * @param element model object emitted by a Flink operator
     * @return JSON representation of the element
     * @throws IllegalArgumentException if Jackson cannot serialize the element
     */
    @Override
    public byte[] serialize(T element) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize element to JSON", e);
        }
    }
}
