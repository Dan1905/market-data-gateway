package com.mdg.gateway.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One Redpanda broker and one Spring context, shared by every integration test.
 *
 * <p>The container is a manually started singleton rather than a {@code @Container} field:
 * JUnit's Testcontainers extension starts and stops a {@code @Container} per test
 * <em>class</em>, so each IT class would pay a fresh broker boot. Starting it once in a
 * static initializer and letting Ryuk reap it at JVM exit keeps the whole suite to a single
 * startup.
 *
 * <p>Redpanda rather than Apache Kafka: the same broker as {@code docker-compose.yml},
 * boots in a couple of seconds against Kafka's ~15, and needs a few hundred MB less RAM —
 * which matters both in CI and on the t3.micro this project targets.
 *
 * <p>Because every subclass shares these annotations and the same dynamic properties,
 * Spring's context cache serves them all one application context.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractKafkaIT {

    protected static final String NORMALIZED_TOPIC = "normalized-market-data";
    protected static final String DLQ_TOPIC = "market-data-dlq";

    private static final RedpandaContainer REDPANDA =
            new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v24.2.18"));

    static {
        REDPANDA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", REDPANDA::getBootstrapServers);
    }

    protected static String bootstrapServers() {
        return REDPANDA.getBootstrapServers();
    }

    protected static KafkaTestConsumer tail(String topic) {
        return new KafkaTestConsumer(REDPANDA.getBootstrapServers(), topic);
    }
}
