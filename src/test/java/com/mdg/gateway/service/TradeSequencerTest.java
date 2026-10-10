package com.mdg.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.config.JacksonConfig;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.service.TradeSequencer.Verdict;
import com.mdg.gateway.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class TradeSequencerTest {

    @TempDir
    Path dir;

    private final ObjectMapper mapper = JacksonConfig.marketDataObjectMapper();

    private TradeSequencer sequencer(boolean enabled) {
        GatewayProperties base = Fixtures.gatewayProperties();
        return new TradeSequencer(new GatewayProperties(base.topics(), base.symbol(), base.producer(), base.dlq(),
                base.dedup(), base.lanes(), base.spill(),
                new GatewayProperties.Backfill(enabled, dir.resolve("state/positions.json"), 20_000,
                        Duration.ofSeconds(2), 2)), mapper);
    }

    private static CanonicalTradeEvent trade(String exchange, String venueSymbol, String id) {
        return Fixtures.canonicalEvent().toBuilder()
                .exchange(exchange).venueSymbol(venueSymbol).tradeId(id)
                .eventId(CanonicalTradeEvent.deterministicEventId(exchange, venueSymbol, id))
                .build();
    }

    private static CanonicalTradeEvent btc(long id) {
        return trade("BINANCE", "BTCUSDT", Long.toString(id));
    }

    @Test
    @DisplayName("the first trade ever seen for a symbol starts its sequence")
    void firstTradeIsNext() {
        assertThat(sequencer(true).check(btc(5_000)).verdict()).isEqualTo(Verdict.NEXT);
    }

    @Test
    @DisplayName("consecutive ids are NEXT; a jump is a GAP naming exactly what is missing")
    void detectsGap() {
        TradeSequencer sequencer = sequencer(true);
        sequencer.advance(btc(100));

        assertThat(sequencer.check(btc(101)).verdict()).isEqualTo(Verdict.NEXT);

        TradeSequencer.Check gap = sequencer.check(btc(105));
        assertThat(gap.verdict()).isEqualTo(Verdict.GAP);
        assertThat(gap.firstMissing()).isEqualTo(101);
        assertThat(gap.lastMissing()).isEqualTo(104);
        assertThat(gap.missing()).isEqualTo(4);
        // Kraken pages by time, so the gap carries when the last delivered trade happened.
        assertThat(gap.lastSeenAt()).isEqualTo(btc(100).timestamp());
    }

    @Test
    @DisplayName("an id at or below the last delivered one is BEHIND: already delivered")
    void detectsReplays() {
        TradeSequencer sequencer = sequencer(true);
        sequencer.advance(btc(100));

        assertThat(sequencer.check(btc(100)).verdict()).isEqualTo(Verdict.BEHIND);
        assertThat(sequencer.check(btc(42)).verdict()).isEqualTo(Verdict.BEHIND);
    }

    @Test
    @DisplayName("each venue symbol has its own sequence")
    void sequencesAreIndependent() {
        TradeSequencer sequencer = sequencer(true);
        sequencer.advance(btc(100));

        assertThat(sequencer.check(trade("BINANCE", "ETHUSDT", "7")).verdict()).isEqualTo(Verdict.NEXT);
        assertThat(sequencer.check(trade("KRAKEN", "BTCUSDT", "7")).verdict()).isEqualTo(Verdict.NEXT);
    }

    @Test
    @DisplayName("positions never move backwards")
    void advanceIsMonotonic() {
        TradeSequencer sequencer = sequencer(true);
        sequencer.advance(btc(100));
        sequencer.advance(btc(90));

        assertThat(sequencer.positions().get("BINANCE:BTCUSDT").tradeId()).isEqualTo(100);
    }

    @Test
    @DisplayName("a restart resumes from the saved position and sees its own downtime as a gap")
    void survivesRestart() {
        TradeSequencer before = sequencer(true);
        before.start();
        before.advance(btc(100));
        before.stop();   // a clean shutdown saves the exact final position

        TradeSequencer after = sequencer(true);

        TradeSequencer.Check gap = after.check(btc(250));
        assertThat(gap.verdict()).isEqualTo(Verdict.GAP);
        assertThat(gap.firstMissing()).isEqualTo(101);
        assertThat(gap.lastMissing()).isEqualTo(249);
        assertThat(gap.lastSeenAt()).isEqualTo(btc(100).timestamp());
        // And the reconnect snapshot's old trades are recognised although nothing is in memory.
        assertThat(after.check(btc(99)).verdict()).isEqualTo(Verdict.BEHIND);
    }

    @Test
    @DisplayName("an unreadable state file means 'no history', not a failed start")
    void corruptStateFile() throws Exception {
        Files.createDirectories(dir.resolve("state"));
        Files.writeString(dir.resolve("state/positions.json"), "{ not json");

        TradeSequencer sequencer = sequencer(true);

        assertThat(sequencer.positions()).isEmpty();
        assertThat(sequencer.check(btc(7)).verdict()).isEqualTo(Verdict.NEXT);
    }

    @Test
    @DisplayName("symbols with non-numeric trade ids are passed through untracked")
    void nonNumericIds() {
        TradeSequencer sequencer = sequencer(true);
        CanonicalTradeEvent odd = trade("COINBASE", "BTC-USD", "a1b2");

        assertThat(sequencer.check(odd).verdict()).isEqualTo(Verdict.UNTRACKED);
        sequencer.advance(odd);
        assertThat(sequencer.positions()).isEmpty();
    }

    @Test
    @DisplayName("when disabled it tracks nothing and writes nothing")
    void disabled() {
        TradeSequencer sequencer = sequencer(false);
        sequencer.start();
        sequencer.advance(btc(100));
        sequencer.stop();

        assertThat(sequencer.check(btc(500)).verdict()).isEqualTo(Verdict.UNTRACKED);
        assertThat(dir.resolve("state/positions.json")).doesNotExist();
    }

    @Test
    @DisplayName("the saved file is plain, readable JSON keyed by venue symbol")
    void stateFileFormat() throws Exception {
        TradeSequencer sequencer = sequencer(true);
        sequencer.advance(btc(100));
        sequencer.flush();

        var json = mapper.readTree(dir.resolve("state/positions.json").toFile());
        assertThat(json.get("BINANCE:BTCUSDT").get("tradeId").asLong()).isEqualTo(100);
        assertThat(Instant.parse(json.get("BINANCE:BTCUSDT").get("timestamp").asText()))
                .isEqualTo(btc(100).timestamp());
    }
}
