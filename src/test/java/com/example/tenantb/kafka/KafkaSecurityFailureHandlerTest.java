package com.example.tenantb.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.tenantb.config.FlinkJobConfig;

import java.util.Set;

import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;

/**
 * Verifies user-friendly Kafka security failure messages.
 */
class KafkaSecurityFailureHandlerTest {
    private static final String PASSWORD = "super-secret-password";

    /**
     * Confirms nested topic authorization failures are converted to a safe message.
     */
    @Test
    void wrapsNestedTopicAuthorizationFailure() {
        Exception original = nested(new TopicAuthorizationException(Set.of("tenant-b-enriched-orders")));

        Exception wrapped = KafkaSecurityFailureHandler.wrapIfKafkaSecurityFailure(original, config());

        assertSame(original, wrapped.getCause());
        assertTrue(wrapped instanceof IllegalStateException);
        assertTrue(wrapped.getMessage().contains("Kafka topic authorization failed"));
        assertTrue(wrapped.getMessage().contains("tenant-b-flink-user"));
        assertTrue(wrapped.getMessage().contains("tenant-b-orders"));
        assertTrue(wrapped.getMessage().contains("tenant-b-products"));
        assertTrue(wrapped.getMessage().contains("tenant-b-enriched-orders"));
        assertSafeMessage(wrapped.getMessage());
    }

    /**
     * Confirms nested group authorization failures are converted to a safe message.
     */
    @Test
    void wrapsNestedGroupAuthorizationFailure() {
        Exception original = nested(new GroupAuthorizationException("tenant-b-flink-job"));

        Exception wrapped = KafkaSecurityFailureHandler.wrapIfKafkaSecurityFailure(original, config());

        assertSame(original, wrapped.getCause());
        assertTrue(wrapped instanceof IllegalStateException);
        assertTrue(wrapped.getMessage().contains("Kafka consumer group authorization failed"));
        assertTrue(wrapped.getMessage().contains("tenant-b-flink-user"));
        assertTrue(wrapped.getMessage().contains("tenant-b-flink-job"));
        assertTrue(wrapped.getMessage().contains("tenant-b-flink-job-products"));
        assertSafeMessage(wrapped.getMessage());
    }

    /**
     * Confirms nested SASL authentication failures are converted to a safe message.
     */
    @Test
    void wrapsNestedSaslAuthenticationFailure() {
        Exception original = nested(new SaslAuthenticationException("Invalid username or password"));

        Exception wrapped = KafkaSecurityFailureHandler.wrapIfKafkaSecurityFailure(original, config());

        assertSame(original, wrapped.getCause());
        assertTrue(wrapped instanceof IllegalStateException);
        assertTrue(wrapped.getMessage().contains("Kafka SASL authentication failed"));
        assertTrue(wrapped.getMessage().contains("tenant-b-flink-user"));
        assertTrue(wrapped.getMessage().contains("Kubernetes Secret"));
        assertSafeMessage(wrapped.getMessage());
    }

    /**
     * Confirms unknown failures are not replaced.
     */
    @Test
    void returnsUnknownExceptionsUnchanged() {
        Exception original = new Exception("ordinary failure");

        Exception wrapped = KafkaSecurityFailureHandler.wrapIfKafkaSecurityFailure(original, config());

        assertSame(original, wrapped);
        assertEquals("ordinary failure", wrapped.getMessage());
    }

    private static Exception nested(RuntimeException kafkaException) {
        return new Exception("Flink job execution failed",
                new RuntimeException("Async Kafka failure", kafkaException));
    }

    private static FlinkJobConfig config() {
        return FlinkJobConfig.fromArgs(new String[] {
                "--kafka.user=tenant-b-flink-user",
                "--orders.input.topic=tenant-b-orders",
                "--products.input.topic=tenant-b-products",
                "--output.topic=tenant-b-enriched-orders",
                "--consumer.group.id=tenant-b-flink-job",
                "--kafka.password=" + PASSWORD
        });
    }

    private static void assertSafeMessage(String message) {
        assertFalse(message.contains(PASSWORD));
        assertFalse(message.contains("sasl.jaas.config"));
        assertFalse(message.contains("password"));
    }
}
