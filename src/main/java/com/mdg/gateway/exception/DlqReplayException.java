package com.mdg.gateway.exception;

/** Raised when a DLQ replay cannot be started or completed. */
public class DlqReplayException extends RuntimeException {

    public DlqReplayException(String message) {
        super(message);
    }

    public DlqReplayException(String message, Throwable cause) {
        super(message, cause);
    }
}
