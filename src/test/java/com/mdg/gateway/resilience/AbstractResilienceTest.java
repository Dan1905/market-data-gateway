package com.mdg.gateway.resilience;

import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.MarketDataProducer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Base for tests that need Resilience4j's <em>annotations</em> to actually fire.
 *
 * <p>{@code MarketDataProducerTest} constructs the producer directly, so its annotations
 * are inert there. These subclasses autowire the proxied bean from a real context, which is
 * the only way to prove the Retry/CircuitBreaker wiring — aspect order, the
 * {@code ignore-exceptions} entry for {@code CallNotPermittedException}, and the fallback
 * resolution — is correct rather than merely plausible.
 *
 * <p>No broker is involved: both {@link KafkaTemplate}s are mocked by bean name (which
 * preserves their generic types, so injection by {@code KafkaTemplate<String,
 * CanonicalTradeEvent>} still resolves), and {@link KafkaAdmin} is mocked so the context
 * does not stall trying to create topics against an absent broker.
 */
@SpringBootTest
@ActiveProfiles("test")
abstract class AbstractResilienceTest {

    @MockitoBean(name = "canonicalKafkaTemplate")
    protected KafkaTemplate<String, CanonicalTradeEvent> canonicalKafkaTemplate;

    @MockitoBean(name = "dlqKafkaTemplate")
    protected KafkaTemplate<String, String> dlqKafkaTemplate;

    /** Mocked so no AdminClient is created against a broker that is not running. */
    @MockitoBean
    protected KafkaAdmin kafkaAdmin;

    @MockitoBean
    protected DeadLetterPublisher deadLetterPublisher;

    @Autowired
    protected MarketDataProducer producer;

    @Autowired
    protected CircuitBreakerRegistry circuitBreakerRegistry;

    @Autowired
    protected RetryRegistry retryRegistry;

    protected CircuitBreaker breaker() {
        return circuitBreakerRegistry.circuitBreaker(MarketDataProducer.RESILIENCE_INSTANCE);
    }

    /**
     * Circuit breaker state is per-registry and the context is shared across test classes,
     * so every test starts from a known-closed breaker.
     */
    @BeforeEach
    void resetBreaker() {
        breaker().reset();
    }
}
