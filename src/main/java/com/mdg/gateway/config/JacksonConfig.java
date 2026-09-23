package com.mdg.gateway.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Jackson tuning that the correctness of this gateway actually depends on.
 *
 * <p>The load-bearing setting is {@code USE_BIG_DECIMAL_FOR_FLOATS}. Kraken v2 sends price
 * and quantity as JSON <em>numbers</em>; without this flag Jackson parses them through
 * {@code double} first, so {@code 0.23374249} can arrive as {@code 0.2337424899999...}
 * and a price like {@code 4136.4} loses its exact decimal representation before it ever
 * reaches {@link java.math.BigDecimal}. Binance sidesteps this by quoting its numerics —
 * Kraken does not.
 */
@Configuration(proxyBeanMethods = false)
public class JacksonConfig {

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer marketDataJacksonCustomizer() {
        return builder -> builder
                .featuresToEnable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .featuresToDisable(
                        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                        // Venues add fields without notice; an unknown field is not a defect.
                        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Standalone mapper mirroring the Spring-managed configuration, for use outside the
     * web/Kafka converters (and to keep unit tests honest about the BigDecimal setting).
     */
    public static ObjectMapper marketDataObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}
