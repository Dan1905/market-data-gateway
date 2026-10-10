package com.mdg.gateway.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.model.CanonicalTradeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Knows, for every venue symbol, the last trade that was delivered — and therefore whether
 * the next one is the next one.
 *
 * <p>All three venues number a symbol's trades consecutively. That turns "did we miss
 * anything?" from a guess into arithmetic: after trade 100 the only acceptable id is 101.
 * <ul>
 *   <li>{@code 101} — {@link Verdict#NEXT}: carry on.</li>
 *   <li>{@code 105} — {@link Verdict#GAP}: 101 to 104 were missed (the socket was down, or
 *       this process was). The caller recovers them before delivering 105.</li>
 *   <li>{@code 98} — {@link Verdict#BEHIND}: already delivered. Venues re-send recent trades
 *       in the snapshot that follows a reconnect; this is how they are recognised even after
 *       a restart has emptied the in-memory deduplication window.</li>
 *   <li>{@code 3} — {@link Verdict#RESTARTED}: far too old to be a re-send. The venue has
 *       started numbering again (a relisted symbol, a test network reset). The sequence
 *       starts over from here. Without this, every later trade would be "behind" the stale
 *       position and the symbol would be dropped silently, for good.</li>
 * </ul>
 *
 * <p>Positions are written to disk about once a second and on shutdown, so a restart or a
 * redeploy resumes from where it stopped and sees its own downtime as a gap. After a hard
 * crash the file can be a second stale, which re-delivers that second (same {@code eventId})
 * rather than losing it.
 *
 * <p>Each symbol is only ever touched from its own lane, one event at a time, so there is no
 * check-then-act race between {@link #check} and {@link #advance}.
 */
@Component
public class TradeSequencer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TradeSequencer.class);

    public enum Verdict { NEXT, GAP, BEHIND, RESTARTED, UNTRACKED }

    /**
     * @param firstMissing first missing trade id (only for {@link Verdict#GAP})
     * @param lastMissing  last missing trade id (only for {@link Verdict#GAP})
     * @param lastSeenAt   venue timestamp of the last delivered trade; some REST APIs page by time
     */
    public record Check(Verdict verdict, long firstMissing, long lastMissing, Instant lastSeenAt) {

        static final Check NEXT = new Check(Verdict.NEXT, 0, 0, null);
        static final Check BEHIND = new Check(Verdict.BEHIND, 0, 0, null);
        static final Check RESTARTED = new Check(Verdict.RESTARTED, 0, 0, null);
        static final Check UNTRACKED = new Check(Verdict.UNTRACKED, 0, 0, null);

        public long missing() {
            return verdict == Verdict.GAP ? lastMissing - firstMissing + 1 : 0;
        }
    }

    /** What is persisted per venue symbol. */
    public record Position(long tradeId, Instant timestamp) {
    }

    private final boolean enabled;
    /** How far behind the last delivered id an event can be and still be a plausible re-send. */
    private final long replayWindow;
    private final Path stateFile;
    private final ObjectMapper objectMapper;
    private final Map<String, Position> positions = new ConcurrentHashMap<>();

    private volatile boolean dirty;
    private volatile boolean running;
    private Thread flusher;

    public TradeSequencer(GatewayProperties properties, ObjectMapper objectMapper) {
        this.enabled = properties.backfill().enabled();
        this.replayWindow = properties.backfill().maxGap();
        this.stateFile = properties.backfill().stateFile();
        this.objectMapper = objectMapper;
        if (enabled) {
            load();
        }
    }

    /** Classifies an event against the last one delivered for its venue symbol. */
    public Check check(CanonicalTradeEvent event) {
        long id = numericId(event);
        if (!enabled || id < 0) {
            return Check.UNTRACKED;
        }
        Position last = positions.get(key(event));
        if (last == null || id == last.tradeId() + 1) {
            // The very first trade ever seen for a symbol has nothing to be compared with.
            return Check.NEXT;
        }
        if (id <= last.tradeId()) {
            if (last.tradeId() - id <= replayWindow) {
                return Check.BEHIND;
            }
            // A re-send is recent: a reconnect snapshot, or an overlap with a backfill, both
            // bounded by the largest gap we ever recover. Something further back than that is
            // not this sequence any more.
            log.warn("{} trade id went from {} back to {}: treating it as the venue restarting its "
                    + "numbering, not as a re-send", key(event), last.tradeId(), id);
            positions.put(key(event), new Position(id - 1, event.timestamp()));
            dirty = true;
            return Check.RESTARTED;
        }
        return new Check(Verdict.GAP, last.tradeId() + 1, id - 1, last.timestamp());
    }

    /** Records that an event has been delivered (or durably handed off). Never moves backwards. */
    public void advance(CanonicalTradeEvent event) {
        long id = numericId(event);
        if (!enabled || id < 0) {
            return;
        }
        positions.merge(key(event), new Position(id, event.timestamp()),
                (current, candidate) -> candidate.tradeId() > current.tradeId() ? candidate : current);
        dirty = true;
    }

    public Map<String, Position> positions() {
        return Map.copyOf(positions);
    }

    private static String key(CanonicalTradeEvent event) {
        return event.exchange() + ':' + event.venueSymbol();
    }

    /** -1 for an id that is not a plain non-negative number: such a symbol is not tracked. */
    private static long numericId(CanonicalTradeEvent event) {
        String id = event.tradeId();
        if (id == null || id.isEmpty() || id.length() > 18) {
            return -1;
        }
        for (int i = 0; i < id.length(); i++) {
            if (id.charAt(i) < '0' || id.charAt(i) > '9') {
                return -1;
            }
        }
        return Long.parseLong(id);
    }

    // ------------------------------------------------------------------
    // Persistence
    // ------------------------------------------------------------------

    private void load() {
        if (!Files.exists(stateFile)) {
            return;
        }
        try {
            positions.putAll(objectMapper.readValue(stateFile.toFile(), new TypeReference<Map<String, Position>>() { }));
            log.info("Resuming from saved trade positions: {}", new TreeMap<>(positions));
        } catch (IOException | RuntimeException ex) {
            // Unreadable state means "no history": the next trade of each symbol starts a new
            // sequence. Nothing is delivered wrongly; a gap spanning the restart goes unnoticed.
            log.error("Could not read trade positions from {}; starting without history", stateFile, ex);
        }
    }

    /** Writes positions if they changed. Write-then-rename, so the file is never half-written. */
    void flush() {
        if (!enabled || !dirty) {
            return;
        }
        dirty = false;
        try {
            if (stateFile.toAbsolutePath().getParent() != null) {
                Files.createDirectories(stateFile.toAbsolutePath().getParent());
            }
            Path temp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            objectMapper.writeValue(temp.toFile(), new TreeMap<>(positions));
            Files.move(temp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException ex) {
            dirty = true;
            log.warn("Could not save trade positions to {}: {}", stateFile, ex.toString());
        }
    }

    @Override
    public void start() {
        running = true;
        if (!enabled) {
            return;
        }
        flusher = Thread.ofVirtual().name("mdg-positions-flush").start(() -> {
            while (running) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
                flush();
            }
        });
    }

    @Override
    public void stop() {
        running = false;
        if (flusher != null) {
            flusher.interrupt();
        }
        // The exact final positions: a clean shutdown must not re-deliver anything.
        flush();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Stops after the lanes (MAX_VALUE - 2000) have drained, so the last deliveries are recorded. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 3000;
    }
}
