package com.mdg.gateway.web;

import com.mdg.gateway.dto.DlqReplayRequest;
import com.mdg.gateway.dto.DlqReplayResponse;
import com.mdg.gateway.service.DlqReplayService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator endpoint for re-processing dead-lettered frames.
 *
 * <p>Synchronous on purpose. A replay is a deliberate, supervised action taken after a fix
 * has shipped, and the operator needs the outcome — how many came back, how many are still
 * broken, and why — in the response rather than having to go hunting through logs. The
 * service enforces a hard duration budget so the request cannot hang indefinitely.
 *
 * <p>This endpoint republishes to a production topic and should not be publicly reachable.
 * It is unauthenticated because the service has no security layer; put it behind the
 * authenticating reverse proxy in {@code deploy/} before exposing it.
 */
@RestController
@RequestMapping("/api/v1/dlq")
@Tag(name = "Dead Letter Queue",
        description = "Re-process frames that failed normalization or publication")
public class DlqReplayController {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayController.class);

    private final DlqReplayService replayService;

    public DlqReplayController(DlqReplayService replayService) {
        this.replayService = replayService;
    }

    /**
     * Drains the DLQ and re-runs each record through the normalization pipeline.
     *
     * <p>The body is optional — {@code POST} with no body replays with defaults. Start with
     * {@code {"dryRun": true}} to see what would happen without republishing anything.
     */
    @Operation(
            summary = "Replay dead-lettered frames",
            description = """
                    Reads `market-data-dlq` from the beginning, re-runs each record through the \
                    same normalization pipeline that originally rejected it, and republishes the \
                    successes to `normalized-market-data`.

                    Two properties are worth understanding before calling this:

                    * **Offsets are never committed.** A replay is a *read* of the DLQ, not a \
                    consumption of it. Records stay in place until retention expires, so a \
                    fix-and-replay cycle can be run repeatedly and an operator can always see \
                    what is still broken. The cost is that every run re-reads from the \
                    beginning, which is what `maxRecords` and the duration budget are for.

                    * **Records that fail again are counted, not re-queued.** Re-dead-lettering \
                    them would make every run grow the queue it is supposed to drain.

                    Replays are single-flight; a concurrent request is rejected with `409`.

                    Send `{"dryRun": true}` first to size a real run.""")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Replay completed",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = DlqReplayResponse.class),
                            examples = @ExampleObject(name = "Partial recovery", value = """
                                    {
                                      "startedAt": "2026-09-22T19:02:23.103Z",
                                      "durationMs": 812,
                                      "dryRun": false,
                                      "consumed": 143,
                                      "filtered": 0,
                                      "republished": 141,
                                      "stillFailing": 2,
                                      "failureSamples": ["offset 87 (KRAKEN): timestamp is required"]
                                    }"""))),
            @ApiResponse(responseCode = "400",
                    description = "Unknown exchangeFilter, or a parameter outside its allowed range",
                    content = @Content(mediaType = MediaType.APPLICATION_PROBLEM_JSON_VALUE,
                            schema = @Schema(implementation = ProblemDetail.class))),
            @ApiResponse(responseCode = "409",
                    description = "Another replay is already draining the DLQ — retry once it finishes",
                    content = @Content(mediaType = MediaType.APPLICATION_PROBLEM_JSON_VALUE,
                            schema = @Schema(implementation = ProblemDetail.class))),
            @ApiResponse(responseCode = "500",
                    description = "The broker could not be reached, or the replay failed partway",
                    content = @Content(mediaType = MediaType.APPLICATION_PROBLEM_JSON_VALUE,
                            schema = @Schema(implementation = ProblemDetail.class)))
    })
    @PostMapping(value = "/replay", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DlqReplayResponse> replay(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    required = false,
                    description = "Optional — omit the body entirely to replay everything with defaults.",
                    content = @Content(examples = {
                            @ExampleObject(name = "Dry run (start here)", value = "{\"dryRun\": true}"),
                            @ExampleObject(name = "One venue, bounded",
                                    value = "{\"exchangeFilter\": \"KRAKEN\", \"maxRecords\": 500}"),
                            @ExampleObject(name = "Everything, defaults", value = "{}")
                    }))
            @Valid @RequestBody(required = false) DlqReplayRequest request) {

        log.info("DLQ replay requested: {}", request);
        return ResponseEntity.ok(replayService.replay(request));
    }
}
