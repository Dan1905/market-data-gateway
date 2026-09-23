# market-data-gateway

An event-driven integration gateway that ingests live trade and ticker feeds from three
crypto exchanges over WebSockets, normalizes their incompatible JSON schemas into a single
canonical model, and publishes the result to Kafka.

Java 21 (virtual threads) · Spring Boot 3.5 · MapStruct · Resilience4j · Redpanda ·
Testcontainers.

```
 Binance   wss://stream.binance.com/ws/btcusdt@trade   BTCUSDT   strings, epoch ms
 Coinbase  wss://advanced-trade-ws.coinbase.com        BTC-USD   nested envelope, ISO-8601
 Kraken    wss://ws.kraken.com/v2                      BTC/USD   JSON numbers, ISO-8601
     |
     |  JDK WebSocket client, one virtual thread per frame, ordered
     v
 PayloadTransformationService ──▶ ExchangePayloadMapper (MapStruct, compile-time)
     |                                    |
     |  control frames                    v
     |  (heartbeats, acks) ──▶ skipped   CanonicalTradeEvent  { eventId, exchange,
     |                                                          symbol, price, quantity,
     |  invalid payload                                         timestamp, rawPayload }
     v                                    |
 market-data-dlq  ◀── DLQ ────────────────┤  Retry + CircuitBreaker
   + error headers                        v
        |                          normalized-market-data
        └──▶ POST /api/v1/dlq/replay ──────┘
```

---

## Quick start

```bash
docker compose up --build
```

Brings up Redpanda, the gateway and a Kafka console. Within about 10 seconds live BTC
ticks are flowing:

| URL | What it is |
|---|---|
| **<http://localhost:8090>** | **Redpanda Console** — watch normalized events and dead letters arrive live |
| <http://localhost:8080/asyncapi.html> | **The event contract** (AsyncAPI) — topics, canonical schema, DLQ headers |
| <http://localhost:8080/swagger-ui.html> | HTTP control plane (OpenAPI) — DLQ replay, try-it-out |
| <http://localhost:8080/actuator/health/exchanges> | Per-venue connection state |

Or straight from the CLI:

```bash
docker exec redpanda rpk topic consume normalized-market-data --num 5
```

Anything that failed to normalize, with the reason attached as headers:

```bash
docker exec redpanda rpk topic consume market-data-dlq --num 5 --print-headers
```

### Build and test locally

One-time setup — register a JDK 21 toolchain:

```bash
make toolchain
```

Then:

```bash
mvn clean verify
```

Runs the unit suite, then the Testcontainers integration tests against a real Redpanda
broker, then the JaCoCo coverage gate. `make help` lists the other shortcuts.

### Why the toolchain step

Maven can *run* on any JDK, but two different things are at stake:

| | Controlled by | Result |
|---|---|---|
| Bytecode emitted | `maven.compiler.release=21` | Always Java 21 (major 65), whatever JDK compiles |
| **JVM the tests run on** | **whichever JVM launched Maven** | Whatever happens to be on your PATH |

`--release 21` guarantees the *artifact* is correct. It does **not** control the JVM
surefire forks to execute the tests. On a developer laptop that is frequently neither 21
nor the version in the container — on the machine this was built on, `mvn` defaulted to
JDK 25, so tests were passing on a runtime three versions ahead of production.

That gap matters for this codebase in particular: it leans on virtual threads, whose
behaviour changed meaningfully after 21 (JEP 491 removed carrier pinning on `synchronized`
in JDK 24). A test that passes locally on 25 is weaker evidence than it looks.

The toolchain closes it — laptop, CI and the Docker build stage all compile *and* test on
JDK 21. CI and the Dockerfile register their own toolchain automatically; only a developer
machine needs the one-time `make toolchain`.

If you skip it, the build fails immediately with
`Cannot find matching toolchain definitions` rather than quietly testing on the wrong JVM.

---

## Design decisions worth knowing

### Two specifications, because there are two interfaces

This is middleware. Its product is a stream of events on a Kafka topic, not an HTTP API —
so the document most people reach for first, OpenAPI, describes the *less* important half.

| | Describes | Audience | Where |
|---|---|---|---|
| **AsyncAPI** | `normalized-market-data`, `market-data-dlq`, the `CanonicalTradeEvent` schema, the DLQ header envelope | **Consumers** integrating with the gateway | `/asyncapi.html` |
| OpenAPI | `POST /api/v1/dlq/replay`, actuator | **Operators** running it | `/swagger-ui.html` |

OpenAPI is a specification for synchronous request/response APIs; it structurally cannot
express "this service publishes messages of this shape to this topic with these headers."
AsyncAPI exists for exactly that, and for a normalization gateway it *is* the deliverable —
it is the document that says what three incompatible exchange schemas collapse into.

The spec lives at `src/main/resources/static/asyncapi.yaml`, so the contract ships with the
service that implements it rather than drifting in a wiki. Validate after any edit:

```bash
docker run --rm -v "$PWD/src/main/resources/static:/spec" \
  asyncapi/cli:latest validate /spec/asyncapi.yaml
```

### There is deliberately no dashboard

A live trade UI would need a data-plane HTTP endpoint (SSE or WebSocket) on the gateway,
which would turn an integration component into a product and duplicate what Redpanda
Console already gives for free. If you do want one, build it as a **separate consumer
service** that reads `normalized-market-data` — that keeps the gateway pure and proves the
output contract is consumable, which is a stronger demonstration anyway.

### Resilience guards the broker, not the parser

The brief put Retry and the CircuitBreaker around payload parsing. They are on the Kafka
publish instead, because retrying is only rational against a *transient* fault:

| Failure | Nature | Handling |
|---|---|---|
| Malformed JSON, missing price, bad timestamp | Deterministic — will fail identically every time | Straight to the DLQ, no retry |
| Broker unreachable, leader election, timeout | Transient — usually resolves in seconds | Retry ×3 with jitter, then circuit breaker, then DLQ |

Retrying corrupt JSON three times burns CPU and delays the DLQ write while holding the
ingest path open. `MarketDataProducer` carries the full reasoning.

### Control frames are skipped, never dead-lettered

All three venues multiplex heartbeats, subscription acknowledgements and status messages
onto the same socket as market data. Kraken alone heartbeats roughly once a second. A
pipeline that dead-letters them buries the one genuinely corrupt frame under thousands of
non-events within the hour, so `PayloadTransformationService` filters them by channel and
counts them as `mdg.frames.skipped`.

### Frames stay ordered

`Listener.onText` returns a `CompletionStage`, and the JDK will not deliver the next frame
until it completes. The client returns a stage that runs the whole normalize-and-publish
pipeline on a virtual thread, then requests the next frame. That buys three things at once:

- **No OS thread blocks.** The pipeline parks on the broker ack; a virtual thread unmounts
  from its carrier while parked, so the `HttpClient` selector threads stay free.
- **Ordering.** One frame in flight per connection, so trades reach the topic in venue
  order. Fire-and-forget dispatch would be faster and would reorder them — the wrong trade
  for market data.
- **Real backpressure.** A slow broker means we stop requesting frames and TCP
  backpressure reaches the venue, instead of the heap absorbing the burst.

The ceiling is one frame per round-trip per venue (~200–1000 frames/s). Shard across
sockets before relaxing the ordering.

### Events are keyed by symbol, not exchange

All venues for one instrument land on the same partition, so a consumer doing cross-venue
comparison sees a single ordered stream.

### Stablecoin collapsing is a real modelling decision

Binance quotes `BTCUSDT`; the canonical form in the brief is `BTC-USD`. Collapsing USDT
onto USD conflates two genuinely different assets — USDT is an issuer-credit token that has
historically depegged. The default matches the brief, and one line turns it off:

```yaml
gateway:
  symbol:
    collapse-stablecoins: false   # BTCUSDT -> BTC-USDT
```

### Coinbase quantity is a 24h aggregate

The Advanced Trade `ticker` channel carries no per-trade size, so `volume_24_h` maps onto
the canonical `quantity`. Summing quantity across venues therefore adds Binance trade sizes
to a Coinbase daily total. Switch that venue to the `market_trades` channel if you need
true per-trade size.

### Decimals never touch `double`

Kraken sends price and quantity as JSON *numbers*. Without
`USE_BIG_DECIMAL_FOR_FLOATS`, Jackson routes them through `double` and `0.23374249`
arrives as `0.23374248999999999`. `JacksonConfig` enables it; an integration test asserts
the exact decimal survives to the topic.

---

## DLQ replay

```bash
# See what would happen, without republishing anything
curl -X POST localhost:8080/api/v1/dlq/replay \
     -H 'Content-Type: application/json' \
     -d '{"dryRun": true}'

# Replay only Kraken records
curl -X POST localhost:8080/api/v1/dlq/replay \
     -H 'Content-Type: application/json' \
     -d '{"exchangeFilter": "KRAKEN", "maxRecords": 500}'
```

```json
{
  "startedAt": "2026-09-22T18:55:10.408Z",
  "durationMs": 812,
  "dryRun": false,
  "consumed": 143,
  "filtered": 0,
  "republished": 141,
  "stillFailing": 2,
  "failureSamples": ["offset 87 (KRAKEN): timestamp is required"]
}
```

Two properties matter:

- **Offsets are never committed.** A replay is a *read* of the DLQ, not a consumption of
  it. Fix, replay, inspect, replay again — the records stay put until retention expires.
- **Still-broken records are counted, not re-queued.** Re-dead-lettering them would make
  every run grow the queue it is supposed to drain.

Replays are single-flight; a concurrent request gets `409 Conflict`.

⚠️ The endpoint is **unauthenticated** and republishes to a production topic. The
`public` compose profile puts Caddy in front with TLS and basic auth — see
[`deploy/README.md`](deploy/README.md). Never expose this service directly.

---

## Operations

| Endpoint | Purpose |
|---|---|
| `/asyncapi.html` · `/asyncapi.yaml` | The event contract — what consumers integrate with |
| `/swagger-ui.html` · `/v3/api-docs` | HTTP control plane |
| `/actuator/health` | Overall health |
| `/actuator/health/exchanges` | Per-venue connection state and consecutive failure count |
| `/actuator/circuitbreakers` | Resilience4j breaker states |
| `/actuator/prometheus` | Metrics scrape endpoint |

Metrics worth alerting on:

| Metric | Meaning |
|---|---|
| `mdg.dlq.published` | Frames that failed normalization |
| `mdg.dlq.publish.failed` | **Data loss** — the DLQ write itself failed |
| `mdg.frames.throttled` | Ingest rate limiter shedding load; raise the limit |
| `mdg.events.publish.fallback` | Retries exhausted or breaker open |
| `mdg.ws.disconnected` | Venue connection churn |

Venue health deliberately does **not** feed readiness. A venue outage is not a reason to
pull the instance from a load balancer or have an orchestrator restart it — restarting does
not bring Binance back, the reconnect loop is already the right response, and cycling the
fleet would take down the DLQ replay endpoint exactly when it is needed.

---

## Memory budget (t2.micro / t3.micro, 1 GB)

| Container | Limit | Observed | Notes |
|---|---|---|---|
| `redpanda` | 320 MB | ~110 MB | `--memory 256M --overprovisioned --smp 1` |
| `market-data-gateway` | 400 MB | ~240 MB | ~280 MB heap at `MaxRAMPercentage=70` |
| `console` | 96 MB | ~45 MB | Redpanda Console, stateless |
| `caddy` | 32 MB | ~15 MB | TLS + basic auth (`--profile public`) |
| **Total** | **848 MB** | **~410 MB** | limits stay under 1 GB so a spike cannot OOM-kill the broker |

Measured with all three feeds connected: **~410 MB actual across all four containers**.
Gateway image: **381 MB**.

Full EC2 walkthrough — swap, TLS, basic auth, systemd, disk sizing — is in
[`deploy/README.md`](deploy/README.md).

`MaxRAMPercentage` rather than `-Xmx` so the heap tracks the cgroup limit instead of a
number that silently becomes wrong. SerialGC because G1's region metadata and concurrent
threads cost more than they return on a 2-vCPU burstable instance at this heap size;
revisit past ~512 MB.

---

## Testing

```
138 tests — 124 unit + 14 integration, 87% line coverage (gate: 85%)
```

| Suite | What it covers |
|---|---|
| `ExchangePayloadMapperTest` | All three venue schemas, plus missing/negative/unexpected fields |
| `SymbolNormalizerTest` | Longest-suffix quote splitting, XBT aliasing, stablecoin toggle |
| `PayloadTransformationServiceTest` | Control-frame filtering, fan-out, decimal precision |
| `CircuitBreakerFallbackTest` | Breaker OPEN / HALF_OPEN / CLOSED transitions and DLQ fallback, through real Spring proxies |
| `KafkaPublishRetryTest` | Retry budget, recovery mid-retry, no-retry-on-success |
| `ExchangeWebSocketClientTest` | Real sockets: handshake, subscriptions, fragmented frames, ordering, reconnect, idle watchdog, oversized-frame rejection |
| `MarketDataPipelineIT` | Frame → Redpanda, end to end, both topics |
| `DlqReplayIT` | Replay, dryRun, filters, repeatability, poison-record handling |

Integration tests run against a real Redpanda container via Testcontainers. **No test
contacts a public exchange** — `ExchangeFeedsDisabledTest` enforces that, because Spring
profiles are additive and simply omitting `gateway.exchanges` from the test profile does
*not* disable the venues.

---

## Why the MapStruct implementation is committed, not generated

`ExchangePayloadMapperImpl.java` lives in `src/main/java` and `mapstruct-processor` is
**not** on the build's annotation processor path. That is deliberate.

Running the processor during the build corrupted roughly **half of all compilations**.
MapStruct emits the mapper source in one annotation-processing round and javac attributes
it in the next; about one build in two, the class file came out with unresolved symbols —
the `implements ExchangePayloadMapper` clause dropped, and method descriptors stripped of
their package qualifiers (`LSymbolNormalizer;` instead of
`Lcom/mdg/gateway/mapper/SymbolNormalizer;`).

Nothing reported an error. The build printed `BUILD SUCCESS`, and the damage surfaced later
as `NoSuchMethodError`, or JUnit refusing to discover tests with a `NoClassDefFoundError`
naming a class that plainly exists.

| | With processor in the build | Implementation committed |
|---|---|---|
| JDK 21 | 8 / 16 corrupt | **0 / 16** |
| JDK 24 | corrupt | **0 / 16** |
| JDK 25 | corrupt | **0 / 10** |
| Linux container, JDK 21 | 6 / 6 corrupt | **0 / 6** |

The generated *source* was always correct — only its compilation raced. Compiling it as an
ordinary source file, with no processor round involved, is deterministic on every JDK.

MapStruct is still the authoring tool: the `@Mapper` interface is unchanged and the
implementation is its real output. Regenerate after changing the interface:

```bash
mvn -Pregenerate-mappers clean generate-sources
cp target/generated-sources/annotations/com/mdg/gateway/mapper/ExchangePayloadMapperImpl.java \
   src/main/java/com/mdg/gateway/mapper/
```

Then restore the explanatory header on that file and run `mvn clean verify`. Drift between
the interface and the committed class is caught by `ExchangePayloadMapperTest`, which
asserts every canonical field for all three venues.

> Earlier revisions of this file blamed javac 24/25 and pinned the build to JDK 21. That
> diagnosis was wrong — the JDK was never the variable. Any JDK 21+ builds this correctly.


---

## Layout

```
src/main/java/com/mdg/gateway/
├── client/      WebSocket ingestion, backoff, reconnect, lifecycle
├── config/      Typed properties, Kafka topology, virtual threads, Jackson
├── dto/         Raw venue payloads (Binance, Coinbase, Kraken) + replay API types
├── exception/   Terminal vs transient failure types
├── mapper/      MapStruct mapper + symbol normalization
├── model/       CanonicalTradeEvent, Exchange
├── producer/    Kafka publishing, DLQ, resilience
├── service/     Transformation, ingestion, DLQ replay
└── web/         Replay endpoint, RFC 7807 errors, venue health
```
