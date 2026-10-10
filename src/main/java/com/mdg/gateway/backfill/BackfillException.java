package com.mdg.gateway.backfill;

import java.time.Duration;

/** A REST backfill call failed. {@link #retryAfter()} is set when the venue rate limited us. */
class BackfillException extends RuntimeException {

    private final boolean rateLimited;
    private final Duration retryAfter;

    BackfillException(String message, Throwable cause) {
        super(message, cause);
        this.rateLimited = false;
        this.retryAfter = null;
    }

    private BackfillException(String message, Duration retryAfter) {
        super(message);
        this.rateLimited = true;
        this.retryAfter = retryAfter;
    }

    static BackfillException rateLimited(String message, Duration retryAfter) {
        return new BackfillException(message, retryAfter);
    }

    boolean isRateLimited() {
        return rateLimited;
    }

    /** The venue's {@code Retry-After}, or {@code null} if it sent none. */
    Duration retryAfter() {
        return retryAfter;
    }
}
