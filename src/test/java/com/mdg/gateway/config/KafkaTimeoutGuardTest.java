package com.mdg.gateway.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTimeoutGuardTest {

    @Test
    @DisplayName("accepts a send timeout longer than Kafka's delivery timeout")
    void acceptsCorrectOrder() {
        assertThatCode(() -> KafkaConfig.requireSendTimeoutAboveDeliveryTimeout(4000, Duration.ofSeconds(5)))
                .doesNotThrowAnyException();
        // Property values arrive as strings when they come from YAML.
        assertThatCode(() -> KafkaConfig.requireSendTimeoutAboveDeliveryTimeout("1500", Duration.ofSeconds(2)))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("refuses to start when the gateway would give up before Kafka does")
    void rejectsGivingUpFirst() {
        // The configuration that put duplicate trades on the topic: 5s wait, 10s delivery.
        assertThatThrownBy(() -> KafkaConfig.requireSendTimeoutAboveDeliveryTimeout(10_000, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate");
        assertThatThrownBy(() -> KafkaConfig.requireSendTimeoutAboveDeliveryTimeout(5000, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("an unset delivery timeout means Kafka's 2-minute default, which is also refused")
    void unsetMeansKafkaDefault() {
        assertThatThrownBy(() -> KafkaConfig.requireSendTimeoutAboveDeliveryTimeout(null, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalStateException.class);
    }
}
