package com.mdg.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka topology and the two producer paths.
 *
 * <p>Two templates rather than one: the primary path serializes
 * {@link CanonicalTradeEvent} as JSON, while the DLQ path must publish the raw frame
 * <em>exactly as it arrived</em> as a String. Sharing one template would force the DLQ to
 * re-wrap a payload that is, by definition, the thing we failed to parse.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    // ------------------------------------------------------------------
    // Topics
    // ------------------------------------------------------------------

    @Bean
    public NewTopic normalizedMarketDataTopic(GatewayProperties properties) {
        return TopicBuilder.name(properties.topics().normalized())
                .partitions(properties.topics().partitions())
                .replicas(properties.topics().replicationFactor())
                // Audit window, not storage - see GatewayProperties.Topics.normalizedRetention
                // for the disk arithmetic behind the default.
                .config("retention.ms",
                        Long.toString(properties.topics().normalizedRetention().toMillis()))
                .config("compression.type", "producer")
                .build();
    }

    @Bean
    public NewTopic marketDataDlqTopic(GatewayProperties properties) {
        return TopicBuilder.name(properties.topics().deadLetter())
                // Single partition: replay ordering is easier to reason about, and the DLQ
                // should never carry enough volume to need parallelism. If it does, that is
                // the alert, not a capacity problem.
                .partitions(1)
                .replicas(properties.topics().replicationFactor())
                .config("retention.ms",
                        Long.toString(properties.topics().deadLetterRetention().toMillis()))
                .build();
    }

    // ------------------------------------------------------------------
    // Primary producer: CanonicalTradeEvent as JSON
    // ------------------------------------------------------------------

    @Bean
    @Primary
    public ProducerFactory<String, CanonicalTradeEvent> canonicalProducerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> config = baseProducerConfig(kafkaProperties);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, CanonicalTradeEvent> factory =
                new DefaultKafkaProducerFactory<>(config);
        // Hand the serializer the Spring-configured mapper so Instant is written as
        // ISO-8601 rather than an epoch array, matching what consumers are told to expect.
        JsonSerializer<CanonicalTradeEvent> valueSerializer = new JsonSerializer<>(objectMapper);
        valueSerializer.setAddTypeInfo(false); // no __TypeId__ header: consumers are polyglot
        factory.setValueSerializer(valueSerializer);
        return factory;
    }

    @Bean
    @Primary
    public KafkaTemplate<String, CanonicalTradeEvent> canonicalKafkaTemplate(
            ProducerFactory<String, CanonicalTradeEvent> factory, GatewayProperties properties) {

        KafkaTemplate<String, CanonicalTradeEvent> template = new KafkaTemplate<>(factory);
        template.setDefaultTopic(properties.topics().normalized());
        template.setObservationEnabled(true);
        return template;
    }

    // ------------------------------------------------------------------
    // DLQ producer: raw frame as String
    // ------------------------------------------------------------------

    @Bean
    public ProducerFactory<String, String> dlqProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> config = baseProducerConfig(kafkaProperties);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> dlqKafkaTemplate(
            ProducerFactory<String, String> dlqProducerFactory, GatewayProperties properties) {

        KafkaTemplate<String, String> template = new KafkaTemplate<>(dlqProducerFactory);
        template.setDefaultTopic(properties.topics().deadLetter());
        return template;
    }

    // ------------------------------------------------------------------
    // Replay consumer properties
    // ------------------------------------------------------------------

    /**
     * Raw consumer properties for the on-demand DLQ replay consumer.
     *
     * <p>Not a {@code ConsumerFactory}/listener container, because replay is a bounded
     * batch job driven by an HTTP call: it assigns partitions manually, seeks, drains, and
     * closes. A long-lived listener container would either sit idle consuming a group slot
     * or race with the replay for the same offsets.
     */
    @Bean
    public Properties dlqReplayConsumerProperties(KafkaProperties kafkaProperties) {
        Properties props = new Properties();
        props.putAll(kafkaProperties.buildConsumerProperties(null));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return props;
    }

    // ------------------------------------------------------------------

    private Map<String, Object> baseProducerConfig(KafkaProperties kafkaProperties) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // acks=all + idempotence: a duplicate tick is harmless, a silently dropped one is
        // not. Note these two must agree — acks=1 with idempotence on is a ConfigException.
        config.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        config.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Small linger buys real batching on a tick firehose for ~5ms of latency.
        config.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Bound the in-memory buffer explicitly: the default 32MB is a large slice of a
        // 256MB heap, and we would rather fail fast into the circuit breaker than swell.
        config.putIfAbsent(ProducerConfig.BUFFER_MEMORY_CONFIG, 16L * 1024 * 1024);
        config.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3_000);
        return config;
    }
}
