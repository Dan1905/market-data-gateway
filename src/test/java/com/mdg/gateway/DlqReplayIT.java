package com.mdg.gateway;

import com.mdg.gateway.dto.DlqReplayRequest;
import com.mdg.gateway.dto.DlqReplayResponse;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.support.AbstractKafkaIT;
import com.mdg.gateway.support.Fixtures;
import com.mdg.gateway.support.KafkaTestConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code POST /api/v1/dlq/replay} against a real broker.
 *
 * <p>The scenario each test sets up is the realistic one: a frame that was dead-lettered
 * because of a <em>transient</em> problem (broker unavailable, or a mapping bug since
 * fixed) and is perfectly valid now. Replay should turn it back into a canonical event.
 */
class DlqReplayIT extends AbstractKafkaIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private DeadLetterPublisher deadLetterPublisher;

    @Test
    @DisplayName("replays a recoverable dead letter back onto the normalized topic")
    void replayRepublishesRecoverableRecords() {
        // A frame that failed at the publish stage: valid payload, broker was down.
        seedDlq(Exchange.BINANCE, Fixtures.BINANCE_TRADE, FailureStage.PUBLISH);

        try (KafkaTestConsumer normalized = tail(NORMALIZED_TOPIC)) {

            DlqReplayResponse response = replay(new DlqReplayRequest(
                    500, 500, 1, Exchange.BINANCE.name(), false));

            assertThat(response.consumed()).isPositive();
            assertThat(response.republished()).isPositive();

            List<ConsumerRecord<String, String>> records = normalized.awaitAtLeast(1, TIMEOUT);
            assertThat(records).isNotEmpty();
            assertThat(records.getFirst().key()).isEqualTo("BTC-USD");
            assertThat(records.getFirst().value()).contains("\"exchange\":\"BINANCE\"");
        }
    }

    @Test
    @DisplayName("dryRun reports what would happen without publishing anything")
    void dryRunPublishesNothing() {
        seedDlq(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE, FailureStage.PUBLISH);

        try (KafkaTestConsumer normalized = tail(NORMALIZED_TOPIC)) {

            DlqReplayResponse response = replay(new DlqReplayRequest(
                    500, 500, 1, Exchange.KRAKEN.name(), true));

            assertThat(response.dryRun()).isTrue();
            assertThat(response.republished()).isPositive();

            // Sizing a replay must never be the thing that duplicates production data.
            assertThat(normalized.drainFor(Duration.ofSeconds(3))).isEmpty();
        }
    }

    @Test
    @DisplayName("a record that is still broken is counted, not re-queued into the DLQ")
    void permanentlyBrokenRecordsAreReportedNotLooped() {
        seedDlq(Exchange.BINANCE, Fixtures.BINANCE_TRADE_NO_PRICE, FailureStage.TRANSFORM);

        try (KafkaTestConsumer dlq = tail(DLQ_TOPIC)) {

            DlqReplayResponse response = replay(new DlqReplayRequest(
                    500, 500, 1, Exchange.BINANCE.name(), false));

            assertThat(response.stillFailing()).isPositive();
            assertThat(response.failureSamples()).isNotEmpty();
            assertThat(response.failureSamples().getFirst()).contains("price is required");

            // The critical property: replaying a poison record must not append it to the
            // DLQ again, or every run would grow the queue it is supposed to drain.
            assertThat(dlq.drainFor(Duration.ofSeconds(3))).isEmpty();
        }
    }

    @Test
    @DisplayName("offsets are never committed, so a replay can be re-run after a fix")
    void replayIsRepeatable() {
        seedDlq(Exchange.COINBASE, Fixtures.COINBASE_TICKER, FailureStage.PUBLISH);

        DlqReplayResponse first = replay(new DlqReplayRequest(500, 500, 1, null, true));
        DlqReplayResponse second = replay(new DlqReplayRequest(500, 500, 1, null, true));

        // A replay is a read of the DLQ, not a consumption of it. An operator must be able
        // to fix, replay, inspect and replay again.
        assertThat(second.consumed()).isEqualTo(first.consumed());
    }

    @Test
    @DisplayName("exchangeFilter narrows the run and reports what it skipped")
    void exchangeFilterSkipsOtherVenues() {
        seedDlq(Exchange.BINANCE, Fixtures.BINANCE_TRADE, FailureStage.PUBLISH);
        seedDlq(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE, FailureStage.PUBLISH);

        DlqReplayResponse response = replay(new DlqReplayRequest(500, 500, 1, "KRAKEN", true));

        assertThat(response.filtered()).isPositive();
        assertThat(response.consumed()).isGreaterThan(response.republished());
    }

    @Test
    @DisplayName("an unknown exchangeFilter is a 400, not a silent no-op")
    void unknownExchangeFilterIsRejected() {
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/dlq/replay",
                new DlqReplayRequest(null, null, null, "NASDAQ", true),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("NASDAQ");
    }

    @Test
    @DisplayName("out-of-range parameters are rejected by validation")
    void invalidParametersAreRejected() {
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/dlq/replay",
                new DlqReplayRequest(0, 10, null, null, true),  // maxRecords < 1, pollTimeout < 100
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("an empty body replays with defaults")
    void emptyBodyUsesDefaults() {
        ResponseEntity<DlqReplayResponse> response = restTemplate.postForEntity(
                "/api/v1/dlq/replay", null, DlqReplayResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().dryRun()).isFalse();
    }

    // ------------------------------------------------------------------

    /**
     * Writes a record to the DLQ and waits for the broker to acknowledge it.
     *
     * <p>{@code DeadLetterPublisher} is deliberately fire-and-forget — it must never block
     * or throw on the ingest path — so a test that seeded and immediately replayed would
     * race the producer and fail intermittently.
     */
    private void seedDlq(Exchange exchange, String payload, FailureStage stage) {
        try (KafkaTestConsumer dlq = tail(DLQ_TOPIC)) {
            deadLetterPublisher.publish(exchange, payload, new RuntimeException("seeded by test"), stage);
            assertThat(dlq.awaitAtLeast(1, TIMEOUT))
                    .as("seeded DLQ record for %s", exchange)
                    .isNotEmpty();
        }
    }

    private DlqReplayResponse replay(DlqReplayRequest request) {
        ResponseEntity<DlqReplayResponse> response =
                restTemplate.postForEntity("/api/v1/dlq/replay", request, DlqReplayResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody();
    }
}
