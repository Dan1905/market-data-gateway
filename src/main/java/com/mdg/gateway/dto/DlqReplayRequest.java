package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * Body of {@code POST /api/v1/dlq/replay}. Every field is optional; the static
 * {@link #defaults()} instance is used when the caller posts an empty body.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DlqReplayRequest(

        /** Upper bound on records drained from the DLQ in this run. */
        @Min(1) @Max(50_000) Integer maxRecords,

        /** Per-poll broker wait. */
        @Min(100) @Max(30_000) Integer pollTimeoutMs,

        /** Consecutive empty polls tolerated before concluding the DLQ is drained. */
        @Min(1) @Max(10) Integer emptyPollsBeforeStop,

        /** Replay only records whose {@code X-Source-Exchange} header matches, e.g. "KRAKEN". */
        String exchangeFilter,

        /** Parse and validate without republishing. Use this to size a real run first. */
        Boolean dryRun) {

    private static final int DEFAULT_MAX_RECORDS = 1_000;
    private static final int DEFAULT_POLL_TIMEOUT_MS = 2_000;
    private static final int DEFAULT_EMPTY_POLLS = 2;

    public static DlqReplayRequest defaults() {
        return new DlqReplayRequest(null, null, null, null, null);
    }

    public int maxRecordsOrDefault() {
        return maxRecords == null ? DEFAULT_MAX_RECORDS : maxRecords;
    }

    public int pollTimeoutMsOrDefault() {
        return pollTimeoutMs == null ? DEFAULT_POLL_TIMEOUT_MS : pollTimeoutMs;
    }

    public int emptyPollsBeforeStopOrDefault() {
        return emptyPollsBeforeStop == null ? DEFAULT_EMPTY_POLLS : emptyPollsBeforeStop;
    }

    public boolean dryRunOrDefault() {
        return Boolean.TRUE.equals(dryRun);
    }
}
