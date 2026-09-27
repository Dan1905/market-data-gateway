package com.mdg.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class JacksonConfigTest {

    private static final BigDecimal DUST = new BigDecimal("0.00000005");

    private static CanonicalTradeEvent dustTrade() {
        return Fixtures.canonicalEvent().toBuilder().quantity(DUST).build();
    }

    @Test
    @DisplayName("the Spring-managed mapper (used by the Kafka serializer) writes dust sizes as plain decimals")
    void springMapperWritesPlainDecimals() throws Exception {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        new JacksonConfig().marketDataJacksonCustomizer().customize(builder);
        ObjectMapper mapper = builder.build();

        // Found live: a real Coinbase trade of 0.00000005 BTC was published as 5E-8.
        assertThat(mapper.writeValueAsString(dustTrade()))
                .contains("\"quantity\":0.00000005")
                .doesNotContain("E-");
    }

    @Test
    void standaloneMapperWritesPlainDecimals() throws Exception {
        assertThat(JacksonConfig.marketDataObjectMapper().writeValueAsString(dustTrade()))
                .contains("\"quantity\":0.00000005")
                .doesNotContain("E-");
    }

    @Test
    @DisplayName("round-trips exactly - no double on the way in or out")
    void roundTripsExactly() throws Exception {
        ObjectMapper mapper = JacksonConfig.marketDataObjectMapper();
        CanonicalTradeEvent back = mapper.readValue(mapper.writeValueAsString(dustTrade()), CanonicalTradeEvent.class);
        assertThat(back.quantity()).isEqualTo(DUST);
    }
}
