package com.mdg.gateway.producer;

/**
 * Header names carried on every {@code market-data-dlq} record.
 *
 * <p>These are the contract between the failure path and the replay path: replay reads
 * {@link #SOURCE_EXCHANGE} to decide which venue parser to re-run, and an operator reads
 * the rest to triage without deserializing the body.
 */
public final class DlqHeaders {

    /** Failure message from the terminal exception. */
    public static final String EXCEPTION_MESSAGE = "X-Exception-Message";

    /** Fully-qualified exception type, for grouping in log/metric backends. */
    public static final String EXCEPTION_CLASS = "X-Exception-Class";

    /** Venue name — the routing key for replay. */
    public static final String SOURCE_EXCHANGE = "X-Source-Exchange";

    /** ISO-8601 instant at which the gateway gave up on the record. */
    public static final String TIMESTAMP = "X-Timestamp";

    /** Topic the record was destined for before it failed. */
    public static final String ORIGINAL_TOPIC = "X-Original-Topic";

    /** Which pipeline stage failed: {@code TRANSFORM} or {@code PUBLISH}. */
    public static final String FAILURE_STAGE = "X-Failure-Stage";

    /** How many times this record has been through a replay, to bound retry loops. */
    public static final String REPLAY_ATTEMPTS = "X-Replay-Attempts";

    private DlqHeaders() {
    }
}
