# market-data-gateway

Integration **middleware**: ingests live BTC trade/ticker feeds from Binance, Coinbase and
Kraken over WebSockets, normalizes three incompatible JSON schemas into one
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

Baseline: 138 tests (124 unit + 14 IT), ~87% line coverage. Keep it green.

## Layout — `src/main/java/com/mdg/gateway/`

| Package | Holds |
|---|---|
| `client/` | JDK `java.net.http.WebSocket` clients. `AbstractExchangeWebSocketClient` does connect, frame reassembly, ordered virtual-thread dispatch, idle watchdog, backoff reconnect. `ExchangeConnectionManager` (SmartLifecycle) owns them. |
| `config/` | Typed `@ConfigurationProperties` records (`GatewayProperties`, `ExchangeProperties`), `KafkaConfig` (topics + two templates), `JacksonConfig` (`USE_BIG_DECIMAL_FOR_FLOATS`), virtual-thread executors, `OpenApiConfig` |
| `dto/` | Raw venue payload records + envelopes; replay request/response |
| `mapper/` | `ExchangePayloadMapper` (MapStruct interface), **`ExchangePayloadMapperImpl` (generated, committed)**, `SymbolNormalizer` |
| `model/` | `CanonicalTradeEvent` (record, invariants in compact ctor, hand-written builder), `Exchange` enum |
| `producer/` | `MarketDataProducer` (Retry + CircuitBreaker, blocks on ack), `DeadLetterPublisher` (never throws), `DlqHeaders` |
| `service/` | `PayloadTransformationService` (parse + skip control frames), `MarketDataIngestionService` (RateLimiter entry point), `DlqReplayService` |
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

## Design rules that are load-bearing

- Resilience (Retry + CircuitBreaker) guards the **Kafka publish**, not parsing. Parse
  failures are deterministic → straight to DLQ, no retry.
- Venue heartbeats/acks/status frames return an **empty list** (`mdg.frames.skipped`), never
  the DLQ.
- `onText` returns a `CompletionStage` so frames stay **ordered** per connection and TCP
  backpressure reaches the venue. Don't switch to fire-and-forget.
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

## Deployment

- Target: AWS EC2 t2/t3.micro (1 GB), Ubuntu 24.04. Step-by-step: `deploy/EC2-QUICKSTART.md`;
  reference: `deploy/README.md`.
- **Never build on the instance** (Maven OOMs in 1 GB). CD builds on a runner → GHCR → the
  instance pulls. On the instance, `.env` drives compose: `COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml`,
  `COMPOSE_PROFILES=public` (Caddy), `GATEWAY_IMAGE=ghcr.io/dan1905/market-data-gateway:<tag>`.
  So every command there is plain `docker compose up -d`.
- CI (`ci.yml`): test job + image job in parallel; image build uses `SKIP_TESTS=true`.
- CD (`cd.yml`): push main → publish `sha-xxxxxxx`; push tag `v*` → verify, publish,
  deploy over SSH with health check + automatic rollback; manual dispatch → redeploy a tag.
  Secrets: `EC2_HOST`, `EC2_USER`, `EC2_SSH_KEY`.
- Memory budget (limits/observed): redpanda 320m/~236m (`--memory 200M`), gateway 400m/~307m,
  console 128m/~76m, caddy 32m/~15m. Topic retention 24h (~1 GB/day at ~25 events/s).
