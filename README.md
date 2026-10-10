# market-data-gateway

An event-driven integration gateway that ingests live trade feeds from three
crypto exchanges over WebSockets, normalizes their incompatible JSON schemas into a single
canonical model, and publishes the result to Kafka.

Java 21 (virtual threads) · Spring Boot 3.5 · MapStruct · Resilience4j · Redpanda ·
Testcontainers.

```
 Binance   wss://stream.binance.com:9443/ws  SUBSCRIBE  BTCUSDT   strings, epoch ms
 Coinbase  wss://advanced-trade-ws.coinbase.com        BTC-USD   nested envelope, ISO-8601
 Kraken    wss://ws.kraken.com/v2                      BTC/USD   JSON numbers, ISO-8601
     |
     |  JDK WebSocket client: frames read in order, parsed on a virtual thread
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

### Concurrency: ordered per symbol, parallel across symbols

Venue symbols are configured by env (`BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,...`, likewise for
Coinbase and Kraken), and one connection per venue carries all of them.

Trade order only matters *within* a venue symbol: trade 1002 on BTCUSDT must follow 1001
on BTCUSDT, but it has no relationship with trade 55 on ETHUSDT. So after a frame is parsed,
each event goes to its symbol's **lane** (`SymbolLaneDispatcher`), a FIFO chain of tasks on
virtual threads. A lane runs one task at a time, and different lanes run in parallel. The
Kafka key is the canonical symbol, so each symbol's events also share one partition, which
keeps them in order on the topic too.

Backpressure survives the concurrency. There is a global budget of in-flight events
(`gateway.lanes.max-in-flight`, 1000 by default). When it is spent, the socket reader blocks,
no further frames are read, and a slow broker pushes back through TCP to the venue instead
of growing the heap. The ingest rate limiter works the same way: over its limit the reader
*waits*, and a frame is dropped only if it is still refused after the full wait.

Measured with `make loadtest`. A fake Binance venue sends 50 symbols as fast as the gateway
will read them. The gateway runs in its normal 400 MB container, pinned to 2 CPUs like a
t3.micro:

| | Before (one frame in flight per connection) | After (per-symbol lanes) |
|---|---|---|
| Sustained throughput | 121 events/s | **6,236 events/s** (≈51×) |
| Gateway CPU per event | 1,947 µs | 104 µs |
| Redpanda CPU per event | 632 µs | 13 µs |
| Gateway memory peak | 241 MB | 302 MB |
| Order check on the topic | – | 936,190 events, 0 gaps / 0 duplicates / 0 reorders |

Before, every trade waited for its own broker ack before the next frame was read. That
capped throughput at 1 / ack-latency, and the producer never had more than one record to
batch. With lanes, many symbols' records are in flight at once, so the producer batches
them, and the per-event cost of both the gateway and the broker falls by 20–50×.

### Adding an exchange is additive

An exchange differs from the others in three ways: how its socket is opened and subscribed,
what its frames look like, and how its REST API pages old trades. Those three live together
in one `Venue` implementation per exchange (`BinanceVenue`, `CoinbaseVenue`, `KrakenVenue`).
Everything else (connection management, ordering, deduplication, publishing, gap recovery)
is written once against the `Venue` interface and contains no `switch` on the exchange.

A fourth exchange is therefore: a new `Venue` (Spring finds it), its payload record and a
mapper method, an `Exchange` constant, and a `gateway.exchanges.<name>` block in
`application.yml`. No existing class gains a `case`.

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
| Broker unreachable, leader election, timeout | Transient — usually resolves in seconds | Retry ×3 with jitter, then circuit breaker, then **spilled to disk** and published in order when the broker is back |

Retrying corrupt JSON three times burns CPU and delays the DLQ write while holding the
ingest path open. `MarketDataProducer` carries the full reasoning.

### A broker outage loses nothing

The dead letter queue is the wrong place for an event the broker refused: it is a topic on
the same broker, so during an outage a "dead-lettered" event was really dropped. Instead,
an event that could not be published goes to a **spill buffer** on local disk
(`SpillBuffer`, on the `gateway-state` volume):

- **Order survives.** While anything is waiting in the buffer, every new event queues
  behind it instead of going to the broker. One drainer publishes from the front, in
  batches, and direct publishing resumes only when the buffer is empty.
- **It survives a restart.** A record is removed only after the broker acknowledged it, and
  a checkpoint records how far draining got. A crash in between re-sends, never loses.
- **It is bounded.** 256 MB by default (about five hours of this feed). Only past that does
  an event fall back to the DLQ, counted in `mdg.events.undeliverable`.

Measured with `OUTAGE=40 RATE=500 loadtest/run.sh 20 90 30`, which freezes the broker
(`docker pause`) for 40 s under a steady 500 trades/s:

| | Result |
|---|---|
| Trades sent / on the topic | 73,200 / 73,200 |
| Gaps, duplicates, reorders | 0 / 0 / 0 |
| Spilled to disk, then drained | 26,060 / 26,060 |
| Dead-lettered | 0 |

The guarantee is at-least-once. If the broker stalls after it has received a record but
before it acknowledges, that record can appear twice; both copies carry the same `eventId`.

One configuration rule makes this safe: Kafka's `delivery.timeout.ms` (4 s) must be shorter
than the gateway's own `send-timeout` (5 s). The other way round, the gateway gives up on a
record Kafka is still delivering and sends it again, and both land. That produced real
duplicate trades during a broker stall, so `KafkaConfig` now refuses to start with the two
in the wrong order.

Watch it with `mdg.spill.pending`, `mdg.spill.bytes`, `mdg.spill.appended` and
`mdg.spill.drained`.

### A disconnect or a restart loses nothing either

Reconnecting quickly is not the same as not losing data: every trade that happened while
the socket was down is simply never sent. All three venues number a symbol's trades
consecutively, which turns "did we miss anything?" into arithmetic.

- **`TradeSequencer`** remembers the last trade id delivered per venue symbol and saves it
  to disk about once a second (on the `gateway-state` volume), so a restart or a redeploy
  sees its own downtime as a gap.
- **Next id is not the next number** (after 100 comes 105): **`GapBackfillService`** fetches
  101 to 104 from the venue's REST API, as the same canonical events with the same
  deterministic `eventId`, flagged `backfilled`. They are published *before* 105, inside
  that symbol's lane, so the topic stays in order.
- **Next id is at or below the last one**: a re-send (Coinbase replays about 100 recent
  trades on every reconnect). It is dropped using the saved position, which works even
  after a restart has emptied the in-memory deduplication window.

Each venue pages differently, which is most of the work: Binance forwards from an id,
Coinbase backwards from an id (its Exchange REST API shares the socket's id space), Kraken
forwards from a timestamp with a cursor.

It is built not to make an incident worse: at most 20,000 trades per gap (the newest), 2
REST calls a second per venue, a cooldown on HTTP 429/418 honouring `Retry-After`, and any
failure returns what was fetched so far and lets the live trade through.

Measured live against the real exchanges: freeze the gateway for 50 s (`docker pause`),
resume it, then restart it.

| | Before | After |
|---|---|---|
| Binance trades missing | 520 | **0** |
| Coinbase trades missing | 115 | **0** |
| Kraken trades missing | 0 | **0** |
| Duplicate `eventId`s after a restart | 87 | **0** |
| Out-of-order trades | – | **0** |

2,877 trades checked with `loadtest/gapcheck.py --since <start>`; 100 were recovered over
REST and 99 re-sent trades were dropped.

It has also been through an unplanned one. During a later soak the host's network dropped
for about four minutes (DNS failures, then TLS errors, then connect timeouts on all three
venues). The connection breakers held off dialling, and on reconnect 2,687 trades were
recovered over REST. Across that whole run: 20,809 trades, 0 missing, 0 duplicates, 0
reorders. `mdg.backfill.trades.missing` is the counter to
alert on: it is the number of trades the gateway knows it could not recover.

### Control frames are skipped, never dead-lettered

All three venues multiplex heartbeats, subscription acknowledgements and status messages
onto the same socket as market data. Kraken alone heartbeats roughly once a second. A
pipeline that dead-letters them buries the one genuinely corrupt frame under thousands of
non-events within the hour, so `PayloadTransformationService` filters them by channel and
counts them as `mdg.frames.skipped`.

### Frames stay ordered

`Listener.onText` returns a `CompletionStage`, and the JDK will not deliver the next frame
until it completes. The client returns a stage that parses the frame on a virtual thread
and hands each event to its symbol's lane (see *Concurrency* above), then requests the next
frame. That buys three things at once:

- **No OS thread blocks.** Anything that waits (the rate limiter, the lanes' budget, a
  broker ack) parks a virtual thread, which unmounts from its carrier, so the `HttpClient`
  selector threads stay free.
- **Ordering.** One frame is parsed at a time per connection, so each symbol's events enter
  its lane in venue order.
- **Real backpressure.** When the lanes are full the hand-off blocks, the next frame is not
  requested, and TCP backpressure reaches the venue instead of the heap absorbing the burst.

### Staying inside the exchanges' limits

Reconnecting is where a market-data client gets itself banned. Binance allows 5 incoming
messages a second per connection and 300 connections per 5 minutes per IP, and states that
IPs which are repeatedly disconnected may be banned. Plain exponential backoff does not
cover the common case: a throttled venue *accepts* the handshake and then hangs up, each
accepted handshake looks like a success, the backoff resets, and the client reconnects once
a second forever.

`VenueConnectionGuard` closes that gap:

| Mechanism | What it does |
|---|---|
| **Circuit breaker per venue connection** (Resilience4j, `venue-binance` etc.) | A connection only counts as a success once the socket has stayed up for `stable-after` (30 s). Five failed or short-lived connections in a row open the breaker; while it is open the gateway **does not dial at all** for 2 minutes, then lets one probe through. |
| **Rate-limit signals are obeyed** | HTTP 429 or 418 on the handshake, or close codes 1008 / 1013, start a cooldown. A `Retry-After` header is honoured, capped at 15 minutes so a bad header cannot park a feed; without one the cooldown is 2 minutes. The breaker opens too, so it shows in metrics. |
| **Outbound message pacing** (Resilience4j rate limiter) | Everything the gateway sends on a socket waits for a permit. Binance is set to 4 messages/s, one under its limit, leaving room for a pong. |

So there are two kinds of circuit breaker in this service, guarding the two things that can
be unhealthy: `kafka-producer` on the way out, and one per venue on the way in.

Watch them with `/actuator/health/exchanges` (each venue shows `connectionCircuit`),
`/actuator/circuitbreakers`, and the counters `mdg.ws.rate.limited` and
`mdg.ws.connect.blocked`.

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

### Every trade has a deterministic identity, so duplicates are dropped

`eventId` is derived from the trade itself — a name-based UUID of
`exchange:venueSymbol:tradeId` — so a trade keeps the same id no matter how often it
arrives. Duplicates do arrive: reconnect overlap, the snapshot Coinbase replays on every
subscribe, DLQ replays of events whose broker ack was lost. `TradeDeduplicator` claims each
id atomically before publishing and **releases the claim if delivery fails**, so a copy of a
trade that never reached the broker is never discarded as a "duplicate". The window is
bounded (10 min / 100k ids, ~15 MB) and every drop is counted in `mdg.events.deduplicated`.
Past the window, consumers can deduplicate on `eventId` themselves.

The id is keyed on the **venue** symbol on purpose: `BTCUSDT` and `BTCUSDC` both normalize to
`BTC-USD`, but each has its own trade-id sequence, so the normalized symbol would make
distinct trades collide.

This is at-least-once delivery with duplicates removed wherever they can be recognised — not
exactly-once.

Coinbase is read from the public `market_trades` channel, which carries trade ids and real
per-trade sizes. (The `ticker` channel used earlier had neither, so its `quantity` was a 24h
rolling volume.)

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
| `/actuator/health/exchanges` | Per-venue connection state, consecutive failure count and connection circuit-breaker state |
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
| `redpanda` | 320 MB | ~270 MB | `--memory 200M --overprovisioned --smp 1`; grows into its allocator over the first hours, then plateaus under 300 MB |
| `market-data-gateway` | 400 MB | ~260 MB | ~280 MB heap ceiling at `MaxRAMPercentage=70`; peaked at 302 MB under a 6,000 events/s load test |
| `console` | 128 MB | ~95 MB | Redpanda Console, stateless |
| `caddy` | 32 MB | ~15 MB | TLS + basic auth (`--profile public`) |
| **Total** | **880 MB** | **~640 MB** | limits stay under 1 GB so a spike cannot OOM-kill the broker |

Measured after a warm-up with all three feeds connected, at ~19 events/s: the whole stack
uses **6.4% of one core** (gateway 4.4%, Redpanda 2.0%) and the topic grows by 483 bytes per
event, about 0.8 GB/day before the 24h retention trims it. Deduplication, per-symbol lanes,
the rate-limit guard, the disk spill buffer and gap recovery added no measurable steady-state
memory or CPU over the first release: they are idle unless something goes wrong.

Full EC2 walkthrough — swap, TLS, basic auth, systemd, disk sizing — is in
[`deploy/README.md`](deploy/README.md).

`MaxRAMPercentage` rather than `-Xmx` so the heap tracks the cgroup limit instead of a
number that silently becomes wrong. SerialGC because G1's region metadata and concurrent
threads cost more than they return on a 2-vCPU burstable instance at this heap size;
revisit past ~512 MB.

---

## Testing

```
238 tests — 219 unit + 19 integration, 89% line coverage (gate: 85%)
```

| Suite | What it covers |
|---|---|
| `ExchangePayloadMapperTest` | All three venue schemas, plus missing/negative/unexpected fields |
| `SymbolNormalizerTest` | Longest-suffix quote splitting, XBT aliasing, stablecoin toggle |
| `PayloadTransformationServiceTest` | Control-frame filtering, fan-out, decimal precision, batches emitted in trade-id order |
| `SymbolLaneDispatcherTest` | Per-lane order under concurrency, lane mutual exclusion, failure isolation, backpressure blocking, permit return on rejection, bounded drain |
| `MarketDataIngestionServiceTest` | Frame admission: parse failures to the DLQ, control frames skipped, trades handed to their lane in frame order |
| `TradeDeliveryPipelineTest` | One trade's path with a real deduplicator and sequencer: publish, spill when the broker refuses, queue behind a backlog, gap recovery before the live trade, claims kept or released correctly |
| `TradeDeduplicatorTest` | Claim/release, TTL, size bound, atomic claims under contention |
| `TradeSequencerTest`, `GapBackfillServiceTest` | Gap / replay / next classification, restart from saved positions, each venue's REST paging, the per-gap limit, 429 cooldown, partial failure |
| `SpillJournalTest`, `SpillBufferTest` | FIFO, two-phase read, segment roll and reclaim, restart recovery, torn-write truncation, size bound, in-order drain after an outage |
| `CircuitBreakerFallbackTest` | Breaker OPEN / HALF_OPEN / CLOSED transitions and DLQ fallback, through real Spring proxies |
| `KafkaPublishRetryTest` | Retry budget, recovery mid-retry, no-retry-on-success |
| `ExchangeWebSocketClientTest` | Real sockets: handshake, subscriptions, fragmented frames, ordering, reconnect, idle watchdog, oversized-frame rejection, HTTP 429 + Retry-After, policy-violation closes, breaker opening on short-lived connections, subscribe pacing |
| `VenueConnectionGuardTest` | Retry-After parsing and capping, close-code classification, one outcome per attempt, breaker transitions |
| `MarketDataPipelineIT` | Frame → Redpanda, end to end, both topics |
| `DlqReplayIT` | Replay, dryRun, filters, repeatability, poison-record handling |
| `BrokerOutageIT` | Freezes the real broker mid-stream: every trade still reaches the topic, in order, with nothing dead-lettered |
| `GapBackfillIT` | A hole in the incoming trade ids is filled from a fake venue REST API: the topic ends up complete and in order, and re-sent trades add nothing |
| `make loadtest` | Saturation run against a fake Binance venue, then an end-to-end gap / duplicate / reorder check of every event on the topic |
| `loadtest/measure.sh`, `loadtest/gapcheck.py` | Live-stack measurement (throughput, CPU µs/event, topic bytes/event, memory) and lost-trade / duplicate detection on the real topic |

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
├── backfill/    REST recovery of trades missed during a disconnect or restart
├── client/      WebSocket ingestion, backoff, reconnect, rate-limit guard, lifecycle
├── config/      Typed properties, Kafka topology, virtual threads, Jackson
├── dto/         Raw venue payloads (Binance, Coinbase, Kraken) + replay API types
├── exception/   Terminal vs transient failure types
├── mapper/      MapStruct mapper + symbol normalization
├── model/       CanonicalTradeEvent, Exchange
├── producer/    Kafka publishing, disk spill buffer, DLQ, resilience
├── service/     Ingestion, delivery pipeline, lanes, dedup, trade sequencing, DLQ replay
├── venue/       One Venue per exchange: frame parsing, socket client, REST client
└── web/         Replay endpoint, RFC 7807 errors, venue health
```
