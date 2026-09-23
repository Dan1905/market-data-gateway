package com.mdg.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DlqHeaders;
import com.mdg.gateway.service.MarketDataIngestionService;
import com.mdg.gateway.support.AbstractKafkaIT;
import com.mdg.gateway.support.Fixtures;
import com.mdg.gateway.support.KafkaTestConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end pipeline against a real broker: raw venue frame in, canonical event or dead
 * letter out.
 *
 * <p>Frames are injected into {@link MarketDataIngestionService} rather than delivered over
 * a real WebSocket. The socket layer is the one part that cannot be tested hermetically,
 * and pointing CI at Binance would make the suite fail on their rate limits and
 * geo-blocking rather than on our defects. Everything downstream of the frame —
 * normalization, validation, resilience, serialization, partitioning, DLQ headers — runs
 * for real here, against a real broker.
 */
class MarketDataPipelineIT extends AbstractKafkaIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private MarketDataIngestionService ingestionService;

    @Test
    @DisplayName("a valid Binance trade lands on the normalized topic, keyed by symbol")
    void validFrameReachesNormalizedTopic() throws Exception {
        try (KafkaTestConsumer consumer = tail(NORMALIZED_TOPIC)) {

            ingestionService.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

            List<ConsumerRecord<String, String>> records = consumer.awaitAtLeast(1, TIMEOUT);
            assertThat(records).hasSize(1);

            ConsumerRecord<String, String> record = records.getFirst();
            assertThat(record.key()).isEqualTo("BTC-USD");

            JsonNode event = objectMapper.readTree(record.value());
            assertThat(event.get("exchange").asText()).isEqualTo("BINANCE");
            assertThat(event.get("symbol").asText()).isEqualTo("BTC-USD");
            assertThat(event.get("price").decimalValue()).isEqualByComparingTo("16580.01");
            assertThat(event.get("quantity").decimalValue()).isEqualByComparingTo("0.004");
            assertThat(event.get("eventId").asText()).isNotBlank();

            // ISO-8601, not an epoch array. Consumers are polyglot, and re-enabling
            // WRITE_DATES_AS_TIMESTAMPS would silently break all of them.
            assertThat(event.get("timestamp").asText()).isEqualTo("2022-12-31T19:43:02.136Z");

            // The audit trail must be the exact bytes the venue sent.
            assertThat(event.get("rawPayload").asText()).isEqualTo(Fixtures.BINANCE_TRADE);
        }
    }

    @Test
    @DisplayName("all three venue notations converge onto one canonical symbol and partition")
    void everyVenueNormalizesToTheSameSymbol() {
        try (KafkaTestConsumer consumer = tail(NORMALIZED_TOPIC)) {

            ingestionService.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);    // BTCUSDT
            ingestionService.ingest(Exchange.COINBASE, Fixtures.COINBASE_TICKER); // BTC-USD
            ingestionService.ingest(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE);      // BTC/USD

            List<ConsumerRecord<String, String>> records = consumer.awaitAtLeast(3, TIMEOUT);
            assertThat(records).hasSize(3);

            assertThat(records).allSatisfy(record -> assertThat(record.key()).isEqualTo("BTC-USD"));
            assertThat(records).extracting(record -> readField(record.value(), "exchange"))
                    .containsExactlyInAnyOrder("BINANCE", "COINBASE", "KRAKEN");

            // This is the whole point of the gateway: one instrument, one partition, so a
            // cross-venue consumer sees a single ordered stream.
            assertThat(records).extracting(ConsumerRecord::partition).containsOnly(records.getFirst().partition());
        }
    }

    @Test
    @DisplayName("Kraken numerics survive the pipeline without double rounding")
    void preservesDecimalPrecisionEndToEnd() {
        try (KafkaTestConsumer consumer = tail(NORMALIZED_TOPIC)) {

            ingestionService.ingest(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE);

            List<ConsumerRecord<String, String>> records = consumer.awaitAtLeast(1, TIMEOUT);
            assertThat(records).hasSize(1);

            // Kraken sends these as JSON numbers. Without USE_BIG_DECIMAL_FOR_FLOATS they
            // would arrive as 0.23374248999999999 — a silent corruption of trade data.
            assertThat(records.getFirst().value())
                    .contains("\"quantity\":0.23374249")
                    .contains("\"price\":4136.4");
        }
    }

    @Test
    @DisplayName("a corrupt frame lands on the DLQ with full error context")
    void malformedFrameReachesDlqWithHeaders() {
        try (KafkaTestConsumer dlq = tail(DLQ_TOPIC)) {

            ingestionService.ingest(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE_NO_TIMESTAMP);

            List<ConsumerRecord<String, String>> records = dlq.awaitAtLeast(1, TIMEOUT);
            assertThat(records).hasSize(1);

            ConsumerRecord<String, String> record = records.getFirst();
            // Body is the untouched original, so a replay re-runs the genuine path.
            assertThat(record.value()).isEqualTo(Fixtures.KRAKEN_TRADE_NO_TIMESTAMP);
            assertThat(record.key()).isEqualTo("KRAKEN");

            assertThat(header(record, DlqHeaders.SOURCE_EXCHANGE)).isEqualTo("KRAKEN");
            assertThat(header(record, DlqHeaders.FAILURE_STAGE)).isEqualTo("TRANSFORM");
            assertThat(header(record, DlqHeaders.EXCEPTION_MESSAGE)).contains("timestamp is required");
            assertThat(header(record, DlqHeaders.EXCEPTION_CLASS)).isNotBlank();
            assertThat(header(record, DlqHeaders.ORIGINAL_TOPIC)).isEqualTo(NORMALIZED_TOPIC);
            assertThat(header(record, DlqHeaders.TIMESTAMP)).isNotBlank();
        }
    }

    @Test
    @DisplayName("malformed JSON is dead-lettered, not dropped")
    void malformedJsonReachesDlq() {
        try (KafkaTestConsumer dlq = tail(DLQ_TOPIC)) {

            ingestionService.ingest(Exchange.BINANCE, Fixtures.MALFORMED_JSON);

            List<ConsumerRecord<String, String>> records = dlq.awaitAtLeast(1, TIMEOUT);
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().value()).isEqualTo(Fixtures.MALFORMED_JSON);
            assertThat(header(records.getFirst(), DlqHeaders.EXCEPTION_MESSAGE)).isNotBlank();
        }
    }

    @Test
    @DisplayName("venue heartbeats and acks reach neither topic")
    void controlFramesAreSilentlySkipped() {
        try (KafkaTestConsumer normalized = tail(NORMALIZED_TOPIC);
             KafkaTestConsumer dlq = tail(DLQ_TOPIC)) {

            ingestionService.ingest(Exchange.KRAKEN, Fixtures.KRAKEN_HEARTBEAT);
            ingestionService.ingest(Exchange.KRAKEN, Fixtures.KRAKEN_SUBSCRIBE_ACK);
            ingestionService.ingest(Exchange.KRAKEN, Fixtures.KRAKEN_ERROR);
            ingestionService.ingest(Exchange.BINANCE, Fixtures.BINANCE_SUBSCRIBE_ACK);
            ingestionService.ingest(Exchange.COINBASE, Fixtures.COINBASE_SUBSCRIPTIONS);

            // Kraken alone heartbeats about once a second. Dead-lettering these would bury
            // the one genuinely corrupt frame under thousands of non-events within an hour.
            assertThat(normalized.drainFor(Duration.ofSeconds(3))).isEmpty();
            assertThat(dlq.drainFor(Duration.ofSeconds(3))).isEmpty();
        }
    }

    // ------------------------------------------------------------------

    private String readField(String json, String field) {
        try {
            return objectMapper.readTree(json).get(field).asText();
        } catch (Exception ex) {
            throw new AssertionError("Could not read '" + field + "' from " + json, ex);
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).as("header %s", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
