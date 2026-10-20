# market-data-gateway

Integration **middleware**: ingests live trade feeds (one or many symbols per venue) from
Binance, Coinbase and Kraken over WebSockets, normalizes three incompatible JSON schemas into one
`CanonicalTradeEvent`, publishes to Kafka (Redpanda). Invalid frames go to a DLQ with error
headers; `POST /api/v1/dlq/replay` re-runs them.

Java 21 (virtual threads) · Spring Boot 3.5.16 · MapStruct 1.6.3 · Resilience4j 2.4.0 ·
spring-kafka · springdoc 2.9.1 · Testcontainers 1.21.4 (Redpanda) · no Lombok.

**Keep it middleware.** Its product is the Kafka topic contract, not a UI. Do not add
data-plane HTTP endpoints (SSE/WebSocket trade streams) or dashboards to this service; a UI
belongs in a *separate consumer service* reading `normalized-market-data`.

## Commands

```bash
make toolchain        # ONE-TIME per machine: registers JDK 21 in ~/.m2/toolchains.xml
mvn clean verify      # unit + Testcontainers ITs + JaCoCo gate (85%). Needs Docker running.
mvn clean test        # unit only, no Docker
make help             # other shortcuts: up, down, logs, ps, topics, dlq, replay

docker compose up -d --build     # local stack: gateway :8080, console :8090, kafka :19092
```

Local URLs: Console http://localhost:8090 · contract http://localhost:8080/asyncapi.html ·
control API http://localhost:8080/swagger-ui.html · venues `/actuator/health/exchanges`.

Baseline: 252 tests (229 unit + 23 IT), ~89% line coverage. Keep it green.

## Layout — `src/main/java/com/mdg/gateway/`

| Package | Holds |
|---|---|
| `backfill/` | `GapBackfillService` + one REST client per venue (`Binance`/`Coinbase`/`KrakenBackfillClient`): fetches the trades inside a gap as canonical events flagged `backfilled`. Bounded, rate limited, never throws into the feed. |
| `client/` | JDK `java.net.http.WebSocket` clients. `AbstractExchangeWebSocketClient` does connect, frame reassembly, ordered virtual-thread dispatch, idle watchdog, backoff reconnect. `VenueConnectionGuard` decides whether a dial is allowed (per-venue circuit breaker + rate-limit cooldown). `ExchangeConnectionManager` (SmartLifecycle) owns them. |
| `config/` | Typed `@ConfigurationProperties` records (`GatewayProperties`, `ExchangeProperties`), `KafkaConfig` (topics + two templates), `JacksonConfig` (`USE_BIG_DECIMAL_FOR_FLOATS`), virtual-thread executors, `OpenApiConfig` |
| `dto/` | Raw venue payload records + envelopes; replay request/response |
| `mapper/` | `ExchangePayloadMapper` (MapStruct interface), **`ExchangePayloadMapperImpl` (generated, committed)**, `SymbolNormalizer` |
| `model/` | `CanonicalTradeEvent` (record, invariants in compact ctor, hand-written builder, `deterministicEventId`), `Exchange` enum |
| `producer/` | `MarketDataProducer` (Retry + CircuitBreaker, blocks on ack; its fallback only returns `false`), `SpillBuffer` + `SpillJournal` (bounded on-disk FIFO for events the broker refused, drained in order), `DeadLetterPublisher` (never throws), `DlqHeaders` |
| `service/` | `PayloadTransformationService` (validates a frame and routes it to its `Venue`; no per-venue code), `MarketDataIngestionService` (RateLimiter entry point: admit, parse, hand each trade to its lane), `TradeDeliveryPipeline` (per trade, inside its lane: sequence → backfill → dedup → publish or spill → DLQ as last resort), `SymbolLaneDispatcher` (per-symbol ordered lanes + in-flight budget), `TradeDeduplicator` (Caffeine claim/release by deterministic `eventId`), `TradeSequencer` (last delivered trade id per venue symbol, persisted; classifies NEXT / GAP / BEHIND), `DlqReplayService` |
| `venue/` | `Venue` interface + `BinanceVenue` / `CoinbaseVenue` / `KrakenVenue`: each owns its frame parsing (`parse`), builds its socket client and its REST backfill client. `FrameReader` holds the shared JSON/error/ordering helpers. |
| `web/` | `DlqReplayController`, RFC 7807 `GlobalExceptionHandler`, `ExchangeHealthIndicator` |

`src/main/resources/static/asyncapi.yaml` is the **event contract** (AsyncAPI 3.0.0),
rendered at `/asyncapi.html`. OpenAPI/Swagger documents only the small HTTP control plane.

## Gotchas — each of these has already cost real time

1. **Never put `mapstruct-processor` back on the build's annotation processor path.**
   Running it in-build corrupted ~50% of compilations on every JDK (8/16 on JDK 21, 6/6 in
   a Linux container): the generated class loses its `implements` clause and descriptors
   lose package qualifiers (`LSymbolNormalizer;`), yet Maven prints BUILD SUCCESS. Symptom
   is later `NoSuchMethodError` / JUnit "failed to discover tests". The generated
   `ExchangePayloadMapperImpl.java` is committed instead. After changing the interface:
   `mvn -Pregenerate-mappers clean generate-sources`, copy the file from
   `target/generated-sources/annotations/`, restore its header. It was never Lombok or the
   JDK — earlier diagnoses blaming those were wrong.
2. **Tests run on JDK 21 via a Maven toolchain.** `--release 21` only fixes bytecode; the
   test JVM is whatever launches Maven (JDK 24/25 on this machine). The toolchain forces
   compile + surefire + failsafe onto 21. Missing `~/.m2/toolchains.xml` → build fails with
   "Cannot find matching toolchain definitions" → run `make toolchain`. CI and the
   Dockerfile register their own.
3. **Test profile must disable venues explicitly.** Spring profiles are additive; omitting
   `gateway.exchanges` from `application-test.yml` does NOT disable them and tests start
   ingesting live BTC ticks. `ExchangeFeedsDisabledTest` guards this.
4. **Verify with `mvn clean verify`, not `compile`.** Changing a record's components breaks
   `src/test/.../support/Fixtures.java`, which only test-compile catches.
5. **Measure; don't estimate.** Redpanda grows into its allocator (110 MB at 1 min → 292 MB
   at 16 h). Resource numbers in docs come from long-running measurement.
6. **zsh:** never name a shell variable `path` — it is bound to `PATH`.
7. **`docker compose up --build` failing then plain `up -d`** silently runs the stale image.
   Check `docker images market-data-gateway --format '{{.CreatedSince}}'`.
8. **Caddy directive order is fixed, not source order.** `basic_auth` runs before `handle`,
   so public paths need a matcher on `basic_auth` (`@protected not path ...`).
9. Resilience4j fallbacks resolve reflectively; fallback methods are package-private and
   must mirror the signature plus a trailing `Throwable`. Retry wraps CircuitBreaker;
   `CallNotPermittedException` is in retry's `ignore-exceptions` on purpose.
10. **No blocking I/O inside `synchronized` on a virtual thread.** On Java 21 it pins the
    carrier thread. `SpillJournal` uses a `ReentrantLock` for that reason; its `pending()`
    is lock-free because it is read for every event.
11. **The load test must not share state with the real stack.** The fake venue restarts
    trade ids at 1; against saved positions they all read as re-sends and are dropped
    (published = 0). `docker-compose.loadtest.yml` points state at a throwaway directory.

12. **A catch-all `@ExceptionHandler(Exception.class)` also catches Spring MVC's own errors.**
    Unknown paths (`NoResourceFoundException`), wrong methods and bad bodies were all
    reported as 500 with a stack trace. `GlobalExceptionHandler` now keeps the status of
    anything that is an `ErrorResponse`. `/` redirects to `/asyncapi.html` (relative, so it
    stays on https behind Caddy).

13. **Integration tests share one broker, so they share the DLQ.** A replay reads the whole
    topic, including other test classes' records. Never assume your record is first or alone.
    Failsafe orders classes by filesystem listing, which differs between macOS and CI's Linux
    (a test passed locally and failed on CI for exactly this). Check with
    `mvn verify -Dfailsafe.runOrder=reversealphabetical`.

## Design rules that are load-bearing

- Resilience (Retry + CircuitBreaker) guards the **Kafka publish**, not parsing. Parse
  failures are deterministic → straight to DLQ, no retry.
- Venue heartbeats/acks/status frames return an **empty list** (`mdg.frames.skipped`), never
  the DLQ.
- `onText` returns a `CompletionStage` so frames stay **ordered** per connection and TCP
  backpressure reaches the venue. Don't switch to fire-and-forget.
- After parsing, events go to per-venue-symbol **lanes** (`SymbolLaneDispatcher`): ordered
  within a symbol, parallel across symbols. `submit` blocks at `gateway.lanes.max-in-flight`,
  which is what keeps backpressure intact — never make it non-blocking or unbounded.
- Batches are emitted in ascending trade-id order (Coinbase sends newest-first).
- The ingest rate limiter **waits** (5s) rather than dropping; dropping is a data-loss path.
- `make loadtest` benchmarks against a fake Binance (`loadtest/`) and verifies every event
  on the topic for gaps/duplicates/reorders. Never pass `--build` with `IMAGE=<old tag>`.
- Two kinds of breaker: `kafka-producer` (publish) and `venue-<name>` (connection). A venue
  connection counts as a success only after `limits.stable-after`; never reset backoff or
  close the breaker on the handshake alone, or a throttled venue causes a reconnect loop.
- HTTP 429/418 and close codes 1008/1013 start a cooldown (`Retry-After`, capped). All
  outbound frames go through the per-venue rate limiter (Binance: 4/s, limit is 5).
- A failed publish is **spilled to disk**, not dead-lettered (the DLQ is on the same broker
  that just failed). While `SpillBuffer.isActive()`, new events must be offered to it, never
  published directly, or they overtake older events of the same symbol. The DLQ `PUBLISH`
  stage is only the last resort when the buffer is full or unavailable.
- `delivery.timeout.ms` must stay **below** `gateway.producer.send-timeout`; `KafkaConfig`
  fails startup otherwise (the reverse caused duplicate trades during a broker stall).
- Gap recovery runs **inside the symbol's lane**, before the live trade is published; that
  is the only reason order holds. `TradeSequencer` is advanced only after an event is
  delivered or durably handed off. It is off in the test profile (fixtures reuse trade ids);
  `GapBackfillIT` turns it on against `FakeVenueRestServer`. No test may call a real venue.
- Venue REST paging differs and was verified live: Binance `fromId` ascending, Coinbase
  Exchange API `after=N` descending below N (needs a User-Agent), Kraken `since=<ns>`
  ascending with a `last` cursor. Normalise REST's zero-padded decimals to match the socket.
- **No `switch` on `Exchange` in shared code.** Per-exchange behaviour belongs in that
  exchange's `Venue`. A new exchange = new `Venue` bean + DTO + mapper method + enum constant
  + `gateway.exchanges.<name>` config (incl. `rest-url` for gap recovery).
- `rawPayload` is the venue's JSON for **that trade**: the whole frame for Binance (one trade
  per frame), the trade's own object cut from a batch for Coinbase/Kraken (`FrameReader.slice`),
  the REST row for a backfilled trade. The DLQ still gets the whole frame, which is what a
  replay needs. The tree is read with exact decimals; the default strips trailing zeros.
- CloudWatch export (`CloudWatchMetricsConfig`, off unless `CLOUDWATCH_ENABLED=true`) sends an
  **allowlist of 5 metrics** with the `exchange` tag folded away, to stay in the free tier's 10
  custom metrics. Adding a name to `EXPORTED` costs money past 10. Names arrive with `.count` /
  `.value` suffixes. Missing region or credentials must never stop startup: it logs one line and
  exports nothing. Credentials come only from the instance role (metadata hop limit 2).
- Events are keyed by **symbol**, not exchange (one instrument → one partition).
- Numerics are `BigDecimal` end to end; never route through `double`.
- DLQ replay never commits offsets and never re-dead-letters (`publishOrThrow`).
- All containers bind `127.0.0.1`; only Caddy is public.

## Style

Comments explain *why*, not what — match the existing density. Records for immutable data;
explicit constructors and SLF4J loggers (Lombok was removed). Unit tests `*Test.java`
(surefire), Testcontainers tests `*IT.java` (failsafe). Validate `asyncapi.yaml` after edits:
`docker run --rm -v "$PWD/src/main/resources/static:/spec" asyncapi/cli validate /spec/asyncapi.yaml`
(pinned to 3.0.0 because the bundled renderer predates 3.1).

## tools/ec2ctl

A separate Maven project (not a module, not in CI, not in the image): a laptop CLI on the AWS
SDK v2 that starts/stops the instance and updates DuckDNS to the new IP. Build with
`mvn -f tools/ec2ctl/pom.xml package` (JDK 21 toolchain; Mockito fails on JDK 25). Config is
env vars only; credentials from the SDK chain. `stop` requires `--yes`.

## README diagrams

The README shows **images**, not live Mermaid blocks: GitHub's Mermaid embedding failed for
the user ("Unable to render rich display: svg element not in render tree") and clipped
diagrams in another browser. Sources are `docs/diagrams/*.mmd`; `docs/diagrams/render.sh`
renders each to `-light.png` and `-dark.png` (transparent), and the README picks one with
`<picture>` and `prefers-color-scheme`. After editing a `.mmd`, run the script and commit
the PNGs. Keep diagrams under ~800 px natural width (GitHub's column is ~830 px): prefer
`flowchart TD` and few, short nodes; a long linear pipeline is better as a table.

## Deployment

- Target: AWS EC2 t2/t3.micro (1 GB), Ubuntu 24.04. Step-by-step: `deploy/EC2-QUICKSTART.md`;
  reference: `deploy/README.md`.
- **Never build on the instance** (Maven OOMs in 1 GB). CD builds on a runner → GHCR → the
  instance pulls. On the instance, `.env` drives compose: `COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml`,
  `COMPOSE_PROFILES=public` (Caddy), `GATEWAY_IMAGE=ghcr.io/dan1905/market-data-gateway:<tag>`.
  So every command there is plain `docker compose up -d`.
- CI (`ci.yml`): test job + image job in parallel; image build uses `SKIP_TESTS=true`.
- CD (`cd.yml`): push main → publish `sha-xxxxxxx`; push tag `v*` → verify, publish,
  deploy with health check + automatic rollback; manual dispatch → redeploy a tag.
  The deploy logic is `deploy/deploy.sh` (image + compose files move together; rollback restores
  both). It runs over **SSM** when the secret `AWS_DEPLOY_ROLE_ARN` (+ vars `AWS_REGION`,
  `EC2_INSTANCE_ID`) is set: OIDC, no AWS keys, no inbound port 22. Otherwise over SSH
  (`EC2_SSH_KEY`). `EC2_HOST`/`EC2_USER` are needed either way. Never pass a token through SSM:
  command history keeps it, which is why the GHCR image must stay public.
- Memory budget (limits/observed 2026-10-10): redpanda 320m/~270m (`--memory 200M`), gateway 400m/~260m
  (302m peak under load test), console 128m/~95m, caddy 32m/~15m. Whole stack ~6.4% of one core at ~19 events/s. Topic retention 24h (~1 GB/day at ~25 events/s).
  Disk also holds the `gateway-state` volume: saved trade positions plus the spill buffer (empty normally, capped at 256 MB during a broker outage).
