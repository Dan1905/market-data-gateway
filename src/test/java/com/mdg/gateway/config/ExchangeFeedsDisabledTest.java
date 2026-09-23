package com.mdg.gateway.config;

import com.mdg.gateway.model.Exchange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the single most important property of the test profile: <b>no test in this suite
 * opens a socket to a real exchange.</b>
 *
 * <p>This exists because the obvious way to express "no venues in tests" — leaving
 * {@code gateway.exchanges} out of {@code application-test.yml} — does not work. Spring
 * profiles are additive: {@code application.yml} is always loaded and the profile file is
 * overlaid on it, so an omitted key keeps its original value and all three venues stay
 * enabled.
 *
 * <p>When that happens the suite does not fail cleanly. It starts receiving live BTC-USD
 * ticks on the normalized topic alongside its own fixtures, so every count assertion
 * becomes a race against market activity — and it only reproduces when the exchanges
 * happen to be reachable from wherever the build is running.
 *
 * <p>This test turns that into one unambiguous failure.
 */
@SpringBootTest
@ActiveProfiles("test")
class ExchangeFeedsDisabledTest {

    @MockitoBean
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private ExchangeProperties exchangeProperties;

    @Test
    @DisplayName("every venue feed is disabled under the test profile")
    void noVenueIsEnabledInTests() {
        Map<Exchange, ExchangeProperties.Connection> venues = exchangeProperties.exchanges();

        assertThat(venues)
                .as("gateway.exchanges should still be bound — we disable venues, not delete them")
                .isNotEmpty();

        assertThat(venues)
                .as("A venue left enabled here means the test suite dials a public exchange. "
                        + "Set gateway.exchanges.<venue>.enabled=false in application-test.yml.")
                .allSatisfy((exchange, connection) ->
                        assertThat(connection.enabled())
                                .as("%s feed enabled", exchange)
                                .isFalse());
    }
}
