package com.example.tenantb.kafka;

import com.example.tenantb.config.FlinkJobConfig;

import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;

/**
 * Converts Kafka security failures into user-friendly job startup failures.
 */
public final class KafkaSecurityFailureHandler {
    private KafkaSecurityFailureHandler() {
    }

    public static Exception wrapIfKafkaSecurityFailure(Exception exception, FlinkJobConfig config) {
        if (containsCause(exception, TopicAuthorizationException.class)) {
            return new IllegalStateException(topicAuthorizationMessage(config), exception);
        }
        if (containsCause(exception, GroupAuthorizationException.class)) {
            return new IllegalStateException(groupAuthorizationMessage(config), exception);
        }
        if (containsCause(exception, SaslAuthenticationException.class)) {
            return new IllegalStateException(saslAuthenticationMessage(config), exception);
        }
        return exception;
    }

    private static boolean containsCause(Throwable throwable, Class<? extends Throwable> expectedType) {
        Throwable current = throwable;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String topicAuthorizationMessage(FlinkJobConfig config) {
        return "Kafka topic authorization failed for kafka.user '" + config.kafkaUser()
                + "'. The user authenticated successfully but is not authorized for one of the configured Kafka topics. "
                + "Check topic ACLs for input topics '" + config.ordersInputTopic() + ", "
                + config.productsInputTopic() + "' and output topic '" + config.outputTopic() + "'.";
    }

    private static String groupAuthorizationMessage(FlinkJobConfig config) {
        return "Kafka consumer group authorization failed for kafka.user '" + config.kafkaUser()
                + "'. The user authenticated successfully but is not authorized for the configured consumer group '"
                + config.consumerGroupId() + "' or the derived products group '"
                + config.consumerGroupId() + "-products'.";
    }

    private static String saslAuthenticationMessage(FlinkJobConfig config) {
        return "Kafka SASL authentication failed for kafka.user '" + config.kafkaUser()
                + "'. Check that the selected Kubernetes Secret matches the configured KafkaUser "
                + "and contains valid SCRAM credentials.";
    }
}
