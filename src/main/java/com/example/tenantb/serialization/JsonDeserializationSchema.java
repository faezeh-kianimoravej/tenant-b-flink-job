package com.example.tenantb.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Generic Jackson-backed JSON deserialization schema for Flink Kafka sources.
 *
 * <p>The schema converts Kafka message values into Tenant B POJOs such as {@code Order} and {@code Product}. It exists
 * as reusable connector infrastructure so each Kafka source can declare only its target model type.</p>
 *
 * @param <T> target POJO type produced by the schema
 */
public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> targetType;

    /**
     * Creates a JSON deserializer for the given model class.
     *
     * @param targetType class that Jackson should instantiate for each Kafka message
     */
    public JsonDeserializationSchema(Class<T> targetType) {
        this.targetType = targetType;
    }

    /**
     * Converts a Kafka message value from JSON bytes into the configured target type.
     *
     * @param message raw Kafka message value
     * @return deserialized model object
     * @throws IOException if the JSON payload cannot be parsed into the target type
     */
    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || new String(message).trim().isEmpty()) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(message, targetType);
        } catch (IOException exception) {
            return null;
        }
    }

    /**
     * Indicates whether the current element should terminate the stream.
     *
     * <p>Kafka topics are unbounded streams in this application, so this always returns {@code false}.</p>
     *
     * @param nextElement deserialized element being inspected by Flink
     * @return {@code false} because Tenant B Kafka sources are continuous streams
     */
    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    /**
     * Provides Flink with the produced Java type for serialization and operator planning.
     *
     * @return Flink type information for the configured target class
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetType);
    }
}
