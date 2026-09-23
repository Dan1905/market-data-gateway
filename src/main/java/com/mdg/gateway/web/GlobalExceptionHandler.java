package com.mdg.gateway.web;

import com.mdg.gateway.exception.DlqReplayException;
import com.mdg.gateway.exception.ReplayAlreadyRunningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Maps gateway failures onto RFC 7807 {@code application/problem+json} responses.
 *
 * <p>Uses {@link ProblemDetail} rather than a bespoke error record so the shape matches
 * what Spring already emits for framework-level errors — an operator's tooling then only
 * has to understand one error format.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private static final String PROBLEM_BASE = "https://market-data-gateway/problems/";

    /**
     * 409: a replay is already draining the DLQ. Retrying later is the correct response,
     * which is what {@code CONFLICT} communicates.
     */
    @ExceptionHandler(ReplayAlreadyRunningException.class)
    public ProblemDetail handleReplayInProgress(ReplayAlreadyRunningException ex) {
        return problem(HttpStatus.CONFLICT, "Replay already in progress", ex.getMessage(), "replay-in-progress");
    }

    /** 400: the caller asked for something impossible, e.g. an unknown exchange filter. */
    @ExceptionHandler(DlqReplayException.class)
    public ProblemDetail handleReplayFailure(DlqReplayException ex) {
        log.error("DLQ replay failed", ex);
        boolean callerError = ex.getCause() == null;
        HttpStatus status = callerError ? HttpStatus.BAD_REQUEST : HttpStatus.INTERNAL_SERVER_ERROR;
        return problem(status, "DLQ replay failed", ex.getMessage(), "replay-failed");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + " " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", detail, "invalid-request");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled exception", ex);
        // Deliberately does not echo ex.getMessage(): internal messages can leak broker
        // addresses and topic names to whoever can reach this endpoint.
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error",
                "An unexpected error occurred. Check gateway logs for the correlating stack trace.",
                "internal-error");
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail, String type) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create(PROBLEM_BASE + type));
        problem.setProperty("timestamp", Instant.now().toString());
        return problem;
    }
}
