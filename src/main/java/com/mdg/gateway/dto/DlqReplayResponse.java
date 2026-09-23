package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.List;

/** Outcome summary of a DLQ replay run. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DlqReplayResponse(

        Instant startedAt,

        long durationMs,

        boolean dryRun,

        /** Records drained from {@code market-data-dlq}. */
        int consumed,

        /** Records skipped because they did not match {@code exchangeFilter}. */
        int filtered,

        /** Canonical events successfully republished to the primary topic. */
        int republished,

        /**
         * Records that failed again. These are <em>not</em> re-queued to the DLQ — doing so
         * would create an infinite loop — so their offsets stay uncommitted and they remain
         * in place for the next replay.
         */
        int stillFailing,

        /** Bounded sample of failure reasons, for operator triage without tailing logs. */
        List<String> failureSamples) {

    public DlqReplayResponse {
        failureSamples = failureSamples == null ? List.of() : List.copyOf(failureSamples);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder — eight fields, most of them {@code int}, so positional args are a trap. */
    public static final class Builder {

        private Instant startedAt;
        private long durationMs;
        private boolean dryRun;
        private int consumed;
        private int filtered;
        private int republished;
        private int stillFailing;
        private List<String> failureSamples = List.of();

        private Builder() {
        }

        public Builder startedAt(Instant startedAt) {
            this.startedAt = startedAt;
            return this;
        }

        public Builder durationMs(long durationMs) {
            this.durationMs = durationMs;
            return this;
        }

        public Builder dryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        public Builder consumed(int consumed) {
            this.consumed = consumed;
            return this;
        }

        public Builder filtered(int filtered) {
            this.filtered = filtered;
            return this;
        }

        public Builder republished(int republished) {
            this.republished = republished;
            return this;
        }

        public Builder stillFailing(int stillFailing) {
            this.stillFailing = stillFailing;
            return this;
        }

        public Builder failureSamples(List<String> failureSamples) {
            this.failureSamples = failureSamples;
            return this;
        }

        public DlqReplayResponse build() {
            return new DlqReplayResponse(startedAt, durationMs, dryRun, consumed, filtered,
                    republished, stillFailing, failureSamples);
        }
    }
}
