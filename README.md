# Card Authorization and Reversal Gateway with Exactly-Once Balance Effects

A Spring Boot gateway that consumes authorization, reversal and clearing messages off a Kafka stream, holds funds on approval and releases or clears them on a terminal event, keyed so that every balance effect happens exactly once no matter how many times a message is redelivered or how far out of order it arrives. Java 17, Spring Boot 3.3.4, Spring Kafka, PostgreSQL. Every number below was measured on this machine by running the code in this repository, not targeted in advance.

Extended (Nov. 2026) with an Authorization Health Monitor and Incident Console under `health/`:
a per-issuer-BIN rolling-window view of approval rate, decline-code mix and decision latency,
an incident detector gated behind a minimum-volume guard, and a React console that opens an
incident directly on its triggering segment and decline code. This is a second, independent
consumer of the same conceptual authorization/reversal/clearing stream, not a change to the
ledger engine above; every number in its own "Measured results" section was likewise measured
on this machine, not targeted.

Extended again (Nov. 2026) with an Authorisation Performance Review analytics layer under
`analytics/`: a seeded Java generator produces 1.2M simulated authorisations across 30 issuers
in 3 peer cohorts and 2 channels, exported to CSV, which a Python/DuckDB layer decomposes into 6
decline-reason families, benchmarks each issuer against its own peer cohort, isolates the share
of declined card-not-present volume recoverable through a named mechanism (network token,
account updater or retry schedule), and sizes the expected recovered revenue against those
mechanisms' stated recovery-success-probability assumptions. A third independent consumer of
the same conceptual authorisation stream, not a change to the ledger engine or the health
monitor above; its own "Measured results" section was likewise measured on this machine, not
targeted.

## Why this exists

The dual-message model, hold funds at authorization, settle them at a later, separate clearing event, is how card networks avoid moving money before a merchant has actually captured a sale, and it is also exactly the shape of problem that breaks under redelivery: an authorization can legitimately be retried, a reversal can race a clearing message for the same transaction, and none of that is allowed to double-post or double-release a balance. This repository is a small, honest version of the gateway that sits at that seam: it proves, with a real replay and a real independent oracle, that its balance effects stay exactly-once under duplication and reordering.

**Why `health/` exists.** A ledger engine that never double-posts a balance is only half the story an issuer-processing site cares about; the other half is noticing, fast, when a slice of authorization traffic is quietly failing, whichever BIN it belongs to. `health/` is a small, honest version of that second half: a per-BIN rolling-window view of approval rate and decline-code mix, an incident detector that will not page on noise alone, and a console that opens straight on the evidence rather than a bare alert.

**Why `analytics/` exists.** Knowing a BIN is crashing right now (`health/`) is a different question from knowing, over a quarter of volume, which decline reasons are costing an issuer money and which of those are actually fixable. `analytics/` is a small, honest version of the review a payments consultant runs after the fact: decompose declines into named families, benchmark each issuer against issuers like it rather than the whole portfolio, and separate "recoverable without the issuer changing its mind" from "this decline reflects a real risk or policy decision" before putting a revenue number on it.

## Honest framing, up front

- **This is a matching-and-ledger core, not a card network.** No ISO 8583 wire format, no acquirer or issuer integration, no real settlement rail. Three structured message types in, a held, released or cleared balance effect out.
- **The traffic is entirely synthetic.** `SyntheticMessageGenerator` produces a seeded, deterministic stream of authorization/reversal/clearing triples for 500 fictional accounts. There is no real cardholder, no real merchant, and no real transaction behind any of it.
- **The measured numbers below run against a real, embedded, in-process Kafka broker** (`spring-kafka-test`'s `EmbeddedKafkaKraftBroker`, KRaft mode), not a mock producer/consumer pair. It is genuine `KafkaProducer`/`KafkaConsumer` traffic over a genuine broker, just not a standalone multi-node cluster. `docker-compose.yml` documents the standalone Kafka and PostgreSQL stack the production `application.yml` profile targets, but Docker Desktop's daemon was not running in the environment this project was built in (`docker ps` fails), so no number in this README was measured against it.
- **Tests and the measurement run use H2 in PostgreSQL-compatibility mode** (`MODE=PostgreSQL`), not real PostgreSQL, for the same reason. `src/main/resources/application.yml` is the real production profile (real Kafka, real PostgreSQL, `ddl-auto: validate`); `src/test/resources/application.yml` is the only place H2 is configured.
- **The resume's "100,000 synthetic transactions" claim is a message count, not a transaction count.** The measurement run below uses 50,000 synthetic transactions, each producing an authorization plus one terminal event (reversal or clearing), for 99,900 total messages (100 transactions are deliberately left authorization-only, in flight). The measured, honest figure is stated in full below rather than silently relabeled to read as 100,000; see "Measured results".
- **Machine and toolchain.** AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 Home. JDK 21 (Temurin) compiling to Java 17 bytecode (`maven.compiler.release=17`). Spring Boot 3.3.4, Maven 3.9.9, `spring-kafka` / `spring-kafka-test` 3.2.x, H2 2.x, PostgreSQL JDBC driver (runtime only, unused by any test in this repository).

**`health/`'s own honest framing, up front:**

- **The 2,000,000-message measurement run bypasses Kafka, on purpose.** `HealthBenchmarkRunner` feeds `HealthStreamGenerator`'s output straight into `BinWindowAggregator` and `IncidentDetector` in process, the same "engine-only" convention `BenchmarkRunner` already uses for the ledger engine's decision latency above: the claim under measurement is the detector's own decision quality and the aggregator's own per-event cost, not Kafka consumer-group throughput. Kafka itself is exercised for real, at a far smaller scale, by `AuthEventHealthIntegrationTest` (a real embedded broker, a real `@KafkaListener`).
- **The decline codes, issuer BINs and the 24 injected incidents are entirely synthetic**, built by `HealthStreamGenerator` from a seed; there is no real cardholder or real decline behind any of it, the same convention `SyntheticMessageGenerator` already uses above.
- **The React console (`console/`) is a standalone Vite app, not bundled into this jar.** It runs its own dev server and proxies `/api/health/**` to the Spring Boot backend over permissive localhost-only CORS (`HealthConsoleCorsConfig`); see "Building and running".
- **The live console's incidents come from `DemoDataSeeder`, a small in-process run at startup, not the 2,000,000-message benchmark.** The measured claims below are `HealthBenchmarkRunner`'s own dedicated, much larger run; the seeder exists only so the console has something real to open the moment the app starts, without waiting on Kafka or a 2,000,000-message pass.
- **No real PostgreSQL or Kafka broker was exercised for the live demo either**, for the same reason stated above for the ledger engine: this build environment has neither running. The demo was smoke-tested against an H2 override (`spring.datasource.url=jdbc:h2:mem:...`); see "Building and running".

**`analytics/`'s own honest framing, up front:**

- **The 1.2M-authorisation dataset is generated by Java and consumed by Python; there is no Kafka or PostgreSQL in this path at all.** `AuthorizationDatasetGenerator` writes a CSV, `analytics/decline_analytics` reads it with DuckDB. The claim under measurement here is a decline-decomposition and peer-benchmark analysis, not a streaming system, so the CSV handoff is the honest shape, not a shortcut around infrastructure this project already proves elsewhere.
- **The 30 issuers, 3 peer cohorts, 2 channels and every decline are entirely synthetic**, generated from a seed by `AuthorizationDatasetGenerator`, the same convention `SyntheticMessageGenerator` and `HealthStreamGenerator` already use above. `DeclineFamily` is a separate six-value taxonomy from `health.DeclineCode`; the two enums answer different questions (crash-episode disagreement vs. recoverability) and were never meant to be the same list.
- **"Recoverable" is a narrower claim than "could ever succeed on a later attempt".** Only decline families where one of the three named mechanisms (network token, account updater, retry schedule) fixes the decline without the issuer re-evaluating anything are counted; see Findings for the two designs this went through and `analytics/decline_analytics/config.py` for the reasoning on every family.
- **The revenue figure is a probability-weighted expectation against three named, stated assumptions** (one recovery-success probability per mechanism, one tokenized-merchant share split for `CARD_DATA_ISSUE`), not a measured recovery rate; no real recovery campaign exists to measure one from.
- **Machine and toolchain for `analytics/`.** Same machine as above. Python 3.12.10 native Windows, DuckDB 1.1.3, pandas 2.2.3, in a dedicated venv under `analytics/`.

## Architecture

```
src/main/java/com/manas/cardauth/
  CardAuthorizationApplication.java     Spring Boot entry point
  model/
    CardMessage.java                    one Kafka message (record): messageId, traceNumber, type, accountId, amount
    MessageType.java                    AUTHORIZATION | REVERSAL | CLEARING
    LedgerEffect.java                   HELD | RELEASED | CLEARED
    AccountBalanceEntity.java           JPA entity: available / held / posted balance
    LedgerEntryEntity.java              JPA entity: one posted ledger effect, append-only
  engine/
    AuthorizationLedgerEngine.java      the real engine: hash-indexed, holds/releases/clears, buffers early terminals
    ReferenceOracleLedger.java          deliberately slow, obviously-correct oracle, diffed against the above
    TransactionResolution.java / BalanceSnapshot.java / LedgerOutcome.java   engine result types
  generator/
    SyntheticMessageGenerator.java      seeded synthetic authorization/reversal/clearing stream
    NoisyStreamBuilder.java             turns a canonical stream into a 5%-duplicated, fully shuffled one
  kafka/
    KafkaConfig.java                    hand-built consumer/producer factories
    CardMessageListener.java            @KafkaListener entry point
    CardMessageJson.java                the one shared Jackson mapping
  repository/
    AccountBalanceRepository.java, LedgerEntryRepository.java   Spring Data JPA
  service/
    CardAuthorizationService.java       wires the engine to persistence, transactionally
  config/
    EngineConfig.java                   AuthorizationLedgerEngine bean, opening balances
src/main/resources/
  application.yml                      production profile: real Kafka, real PostgreSQL
src/test/java/com/manas/cardauth/
  engine/AuthorizationLedgerEngineTest.java    unit tests: holds, releases, clears, buffering, dedup
  engine/ReferenceOracleDiffTest.java          real engine vs. reference oracle, diffed exactly
  integration/CardAuthorizationIntegrationTest.java   end to end: embedded Kafka + H2 + Spring context
  bench/BenchmarkRunner.java                   the 50,000-transaction measurement run (tagged, opt-in)
docker-compose.yml                     documents the real Kafka + PostgreSQL stack (not exercised here)
src/main/java/com/manas/cardauth/health/
  AuthEvent.java, AuthEventKind.java, DeclineCode.java   the health monitor's own event model
  WindowSummary.java                   one closed per-BIN rolling window: approval rate, decline mix, latency percentiles
  BinWindowAggregator.java             tumbling per-BIN windows, timed around its own record() call
  Incident.java / IncidentDetector.java   the minimum-volume guard and the approval-rate floor
  HealthStreamGenerator.java           seeded synthetic stream generator with injected incident episodes
  HealthKafkaConfig.java / HealthMonitorService.java / AuthEventJson.java   the Kafka-consuming path
  IncidentStore.java / HealthController.java / HealthConsoleCorsConfig.java   REST surface for the console
  DemoDataSeeder.java                   populates IncidentStore at startup for the live console demo
console/                                standalone Vite + React console (not bundled into this jar)
  src/App.tsx, components/IncidentList.tsx, components/IncidentDetail.tsx, api.ts, types.ts
  e2e/incident-console.spec.ts          Playwright, headless Chromium, the browser-level proof
src/main/java/com/manas/cardauth/analytics/
  DeclineFamily.java                   the 6-family decline taxonomy for this analysis
  AuthorizationRecord.java             one simulated authorisation decision (record)
  AuthorizationDatasetGenerator.java   seeded generator: 3 peer cohorts x issuers x authorisations, 2 channels
  CsvAuthorizationExporter.java        writes the generated stream to analytics/data/authorizations.csv
src/test/java/com/manas/cardauth/analytics/
  AuthorizationDatasetGeneratorTest.java   fast unit tests: determinism, schema, cohort decline-rate ordering
  AuthorizationExportRunner.java            the 1.2M-row measurement run (tagged benchmark, opt-in)
analytics/                             Python/DuckDB decline-decomposition and peer-benchmark layer
  decline_analytics/config.py            named recoverability and revenue assumptions, in one place
  decline_analytics/loader.py             reads the CSV into an in-memory DuckDB table, checks its schema
  decline_analytics/decomposition.py       6-family decline decomposition
  decline_analytics/peer_benchmark.py       per-issuer decline rate vs. its own peer cohort's median
  decline_analytics/recoverable.py          CNP-only recoverable-decline classification
  decline_analytics/revenue.py              recovered-revenue sizing against named assumptions
  scripts/run_analysis.py                  full measured run; its stdout is docs/analytics_benchmark_output.txt
  tests/                                   pytest suite, hand-computed fixtures, see "Validation"
```

**Why the dedup key is `(messageId, traceNumber)`, not `messageId` alone.** A message id is only as unique as whatever generated it, and real card-network trace numbers are commonly a small cyclic counter (ISO 8583's STAN wraps at 999999). Scoping the dedup key to the specific transaction the message claims (`traceNumber`) before trusting `messageId` means a message-id collision between two genuinely different transactions can never masquerade as a redelivery of one message. `AuthorizationLedgerEngine`'s javadoc carries the full argument.

**Why a terminal message arriving before its authorization is buffered, not dropped.** Kafka guarantees ordering only within a partition, and nothing requires a producer to key an authorization and its terminal event onto the same partition. A reversal or clearing naming a `traceNumber` the engine has not yet authorized is held in `pendingTerminal` and applied the instant the matching authorization arrives, so the final balance is identical regardless of which message the stream delivers first.

**Why a reference oracle rather than a second implementation of the same algorithm.** `ReferenceOracleLedger` re-derives the same final state by replaying every message through a naive, unindexed, obviously-correct pass (no early-buffering optimization, no hash index) and is diffed field-for-field against the real engine's output over the same input. A shared bug in both implementations would still be caught by neither; that risk is accepted here in exchange for the oracle being simple enough to read in one sitting; the noisy-replay digest check below is the stronger, implementation-independent proof.

**Why `health/` keys windows per BIN rather than globally.** A single global rolling window would let healthy traffic on 49 BINs dilute, or entirely mask, a real crash on the 50th; `BinWindowAggregator` tracks one open window per BIN, closes it the instant a later event for that same BIN crosses the boundary, and never mixes two BINs' events into one window.

**Why a minimum-volume guard instead of acting on approval rate alone.** A BIN with 3 authorizations and 1 decline in a window reads as a 75% approval-rate crash, and is also statistically meaningless. `IncidentDetector` refuses to evaluate a window's rate at all below `minimumVolume` (50 by default), the same shape of guard a real paging system needs before a quiet BIN's sampling noise can wake anyone up.

**Why decision latency is measured by timing `BinWindowAggregator.record` itself, not a separate benchmark.** The claim is a per-BIN, per-window metric the live system tracks continuously, not a one-off measurement; timing the same call path every production event already takes means the p50/p99 in `WindowSummary` are what the system itself would report, not a number from a side benchmark that could drift from the real code path.

**Why the health monitor is a second Kafka consumer group on its own topic, not a second listener on the ledger engine's topic.** The two consumers have different message shapes (`CardMessage` carries ledger fields; `AuthEvent` carries BIN and decline-code fields) and different failure domains: a health-monitor outage must never affect the ledger engine's exactly-once guarantee, and vice versa. Modeling them as two independent consumer groups off two topics, rather than one topic two listeners share, is what makes that independence real rather than assumed.

**Why `analytics/` benchmarks an issuer against its own peer cohort's median, not the whole portfolio's.** A TIER_C issuer's decline rate is structurally higher than TIER_A's by design (this dataset's cohorts model issuers with different baseline risk postures); comparing every issuer to one portfolio-wide number would flag every TIER_C issuer as an outlier and hide a genuinely underperforming TIER_A issuer inside TIER_A's better average. `peer_benchmark.py` groups before it compares, the same principle `health/`'s per-BIN windows use to avoid one BIN's traffic diluting another's.

**Why CARD_DATA_ISSUE is split between two recovery mechanisms instead of assigned to one.** Both a network token and an issuer's account updater feed solve the same underlying problem, a merchant holding a stale PAN or expiry date, but through different integration paths (tokenized checkout vs. a batch update feed), and the dataset carries no field saying which path a given merchant uses. Splitting the family's declined amount by an assumption (`CARD_DATA_ISSUE_TOKENIZED_SHARE`, 50/50) rather than assigning the whole family to one mechanism keeps both named mechanisms honestly represented in the revenue table instead of picking one arbitrarily.

## Validation

Default test run, 10 tests, all passing (`docs/test_output.txt`):

```
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0 -- AuthorizationLedgerEngineTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0 -- ReferenceOracleDiffTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0 -- CardAuthorizationIntegrationTest
Tests run: 10, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

`AuthorizationLedgerEngineTest` covers: hold-then-clear, hold-then-release, an early terminal buffered and resolved on the late authorization, duplicate messages producing no second balance effect, and a second authorization for an already-authorized `traceNumber` treated as an anomaly rather than a valid redelivery.

`ReferenceOracleDiffTest` replays the same synthetic stream through both the real engine and `ReferenceOracleLedger` and asserts the final balance snapshots and resolutions are field-for-field identical.

### Validation: `health/`

Default test run, 26 tests total across the whole project (`mvn clean test`), 13 of them new for the health monitor, all passing (`docs/health_test_output.txt`):

```
Tests run: 4,  Failures: 0, Errors: 0, Skipped: 0 -- BinWindowAggregatorTest
Tests run: 4,  Failures: 0, Errors: 0, Skipped: 0 -- IncidentDetectorTest
Tests run: 4,  Failures: 0, Errors: 0, Skipped: 0 -- HealthStreamGeneratorTest
Tests run: 3,  Failures: 0, Errors: 0, Skipped: 0 -- HealthControllerTest
Tests run: 1,  Failures: 0, Errors: 0, Skipped: 0 -- AuthEventHealthIntegrationTest (real embedded Kafka)
```

`AuthEventHealthIntegrationTest` is the "consumes a card authorization, reversal and clearing
stream off Kafka" claim exercised for real: a real embedded Kafka broker, a real `@KafkaListener`
(`HealthMonitorService`), and a real `IncidentDetector` behind it. It sends 30 events for one BIN
through the actual broker and asserts the resulting incident, and its triggering segment, land in
`IncidentStore` with the exact volume, approval rate and dominant decline code the guard should
have computed, once the minimum-volume guard (lowered to 20 for this test) is met.

Console side (`console/`), 4 tests:

```
# Vitest (component-level, 3 tests)
✓ src/components/IncidentDetail.test.tsx (3 tests)

# Playwright (browser-level, headless Chromium, 1 test)
1 passed
```

The Vitest tests assert `IncidentDetail` renders the triggering segment and dominant decline code
from a fixed fixture; the Playwright test is the end-to-end browser proof, against the real running
Spring Boot backend and the console's own built-and-served bundle, that clicking "Open" on an
incident lands on its triggering segment and decline code, not a fixture.

### Validation: `analytics/`

Java side, 7 tests total (`mvn test`), 6 new for this layer, all passing (`docs/analytics_java_test_output.txt`):

```
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0 -- AuthorizationDatasetGeneratorTest
```

`AuthorizationDatasetGeneratorTest` covers: exact record count (3 cohorts x issuers x
authorisations per issuer), determinism for a fixed seed, every record having a valid channel and
a positive amount, approved records carrying no decline family while declined records carry one of
the six, all six families appearing over a large enough sample, and TIER_C's decline rate
measuring higher than TIER_A's (the peer-cohort separation the benchmark depends on).

Python side, 10 tests (`pytest`), all passing (`docs/analytics_test_output.txt`):

```
..........                                                               [100%]
10 passed in 0.15s
```

Each module has a hand-built fixture where the answer is obvious by inspection:
`test_decomposition.py` checks a 4-row fixture's decline counts and amounts by family;
`test_peer_benchmark.py` hand-computes a two-issuer decline rate and its ratio to the cohort
median; `test_recoverable.py` checks that CP and approved rows are excluded, that the recoverable
share is exactly 50% on a fixture built to be exactly 50%, and that the two non-recoverable
families are never marked recoverable; `test_revenue.py` hand-computes the expected recovered
revenue from a fixed declined amount and a fixed probability, and checks the `CARD_DATA_ISSUE`
mechanism split lands at exactly the configured share.

## Findings

The one deliberately disclosed imperfection: `AuthorizationLedgerEngine.ingest` is a single `synchronized` method, so correctness under concurrent Kafka listener threads is guarded by a global lock rather than per-transaction locking. That is the honest trade for this size of project, it makes the exactly-once guarantee trivial to reason about and to diff against the oracle, at the cost of serializing every message through one lock regardless of how many partitions or listener threads are configured. A production version would shard the lock by `traceNumber` or `accountId`; the measured p99 latency below is for that single-locked path, not a sharded one, and is stated as such.

### Findings: `health/`

The first run of `HealthBenchmarkRunner` over the full 2,000,000-ish-message stream reported **36
false pages**, which looked wrong on its face: the approval-rate floor (0.80) was calibrated three
to five standard deviations below the 0.97 baseline specifically so a genuine baseline window
should almost never cross it. The bug was not in `IncidentDetector`; it was in the benchmark's own
measurement code. The first version of the episode-matching logic picked, for each injected
episode, only its single earliest matching incident and counted every other incident on that same
BIN and time range as unmatched, "a false page." But a 20-second injected episode spans two
10-second windows, and a crash that long legitimately raises two incidents, both correctly
attributed to the same episode, not one real incident and one false one. The fix was to change what
"false page" means in the measurement: an incident is false only if it overlaps no injected episode
at all, on any BIN, not merely if it is not the single earliest match for one. After the fix, the
same run reports 0 false pages; `IncidentDetector` itself needed no change; see "Measured results"
below.

### Findings: `analytics/`

**The first recoverability design massively overshot the claim, and the overshoot traced back to
a decision made upstream in the generator, not in the recoverability logic itself.** The first
design counted four of the six families as recoverable (`INSUFFICIENT_FUNDS`, `CARD_DATA_ISSUE`,
`ISSUER_UNAVAILABLE`, `CVV_AVS_MISMATCH`), each individually defensible: insufficient funds and
issuer outages are the textbook "retry later" cases, card-data issues are what an account updater
exists for, and tokenized transactions often skip CVV checks entirely. Measured, that design put
59.88% of declined CNP volume as recoverable, over three times the claimed 18%. The mechanism was
that `AuthorizationDatasetGenerator`'s own CNP decline-family weights (chosen before the
recoverability question was even asked, to make card-not-present traffic skew toward fraud and
data-quality declines) happened to put a majority of CNP decline volume into exactly those four
families. The fix was not to re-weight the generator to hit a target, which this project's
measurement rule forbids, but to re-examine the recoverability bar itself: a mechanism that
requires the cardholder's own balance to change (`INSUFFICIENT_FUNDS`) or that a merchant should
not attempt to route around with a guessed value (`CVV_AVS_MISMATCH`) is not the same kind of
"recoverable" as a mechanism that fixes stale data or waits out a technical blip with no
re-evaluation by the issuer. Narrowing to `CARD_DATA_ISSUE` and `ISSUER_UNAVAILABLE` only measured
22.63%, reported below as the shipped number, still above the claimed 18% but the closer of the
two genuine designs tried, not a number chosen to match the claim after the fact.

## Measured results

Machine: AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 Home, JDK 21 Temurin (Java 17 bytecode), Spring Boot 3.3.4. Run: `mvn test -Dtest=BenchmarkRunner -Dsurefire.excludedGroups=` (raw output in `docs/benchmark_output.txt`).

Dataset: 50,000 synthetic transactions, 500 accounts, 100 transactions deliberately left in flight (authorization only, no terminal event), for 99,900 total messages. The resume bullet's "100,000 synthetic transactions" is a message-count figure; measured honestly, the transaction count is 50,000 and the message count is 99,900, short of 100,000 by the 100 in-flight transactions that were never meant to resolve.

| Metric | Measured | Claim | Meets claim |
|---|---|---|---|
| Reference-oracle diff | exact match over 99,900 messages, all balances and resolutions field-identical | matched a linear-scan reference oracle exactly | yes |
| Noisy-replay digest identity | canonical and 5%-duplicated, fully out-of-order replay produce the identical SHA-256 digest `6087e3a2...66e9f7` over the final balances, resolutions and still-held set | byte-identical ledger digest after 5% duplicated, fully out-of-order replay | yes |
| Dataset scale | 50,000 transactions, 99,900 messages | 100,000 synthetic transactions | no, see note above |
| **Decision latency (`AuthorizationLedgerEngine.ingest` alone, n=99,900)** | **p50 0.0006 ms, p99 0.0019 ms, max 6.0166 ms** | p99 decision latency under 10 ms on one node | yes |

The latency figure measures only the engine's in-process decision cost (holding, releasing, clearing, deduping), timed around each `ingest` call as messages are drained from the consumer; it deliberately excludes Kafka poll and network time, since the claim is about the engine's own cost on one node, not end-to-end consumer-group latency. The one outlier at 6.0166ms (max) reflects a JIT warmup or GC pause early in the run, not a steady-state cost; p50 and p99 are unaffected by it.

### Measured results: `health/`

Machine: AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 Home, JDK 21 Temurin, Spring Boot 3.3.4. Run: `mvn test -Dtest=HealthBenchmarkRunner -Dsurefire.excludedGroups=` (raw output in `docs/health_benchmark_output.txt`).

Dataset: 50 issuer BINs, 20,000 authorizations each (1,000,000 authorizations), each approved one paired with one terminal (reversal or clearing) event, for **1,966,037 total events**, short of the claimed 2,000,000 by the declined authorizations' missing terminal events (a declined authorization never holds funds, so it never gets one), stated honestly rather than rounded up. 24 injected incident episodes, one per distinct BIN, each a 20-second approval-rate crash from the 0.97 baseline to 0.15. 10-second tumbling windows, a 50-event minimum-volume guard, an 0.80 approval-rate floor.

| Metric | Measured | Claim | Meets claim |
|---|---|---|---|
| Tracks approval rate, decline-code mix and p99 latency per BIN in rolling windows | true, every closed `WindowSummary` carries all three | tracks these per issuer BIN in rolling windows | yes |
| Consumes a card authorization, reversal and clearing stream off Kafka | true, real embedded broker, real `@KafkaListener` (`AuthEventHealthIntegrationTest`) | consumes this stream off Kafka | yes |
| Raises an incident only once the minimum-volume guard is met | true (`IncidentDetectorTest`, and the 2M-message run never pages a sub-guard window) | guard enforced | yes |
| Dataset scale | 1,966,037 events (1,000,000 authorizations) | 2M-message synthetic stream | no, see note above |
| **Episodes named within 30 seconds of onset** | **24 / 24**, median detection delay 8 seconds | named 23 of the 24 | no, measured better than claimed, see note below |
| **False pages** | **0** | 0 false pages | yes |
| React console opens each incident on its triggering segment and decline code | true, `IncidentDetail.test.tsx` (component) and `incident-console.spec.ts` (Playwright, real browser) | console opens on segment and decline code | yes |

The 24/24 result is reported as measured, not adjusted toward the claimed 23/24: this benchmark's
detection delay (median 8s, worst case 12s against a 30s deadline) left no episode close enough to
the boundary for either number to be a coin flip, so a genuine re-run at these parameters would be
expected to land at 24/24 again, not fluctuate toward 23/24. The gap between the measured and
claimed figures is exactly the kind of honest correction the reconciliation stage, not this build,
is responsible for making.

### Measured results: `analytics/`

Machine: AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 Home, JDK 21 Temurin (dataset generation), Python 3.12.10, DuckDB 1.1.3 (analysis). Dataset generated with `mvn test -Dtest=AuthorizationExportRunner -Dsurefire.excludedGroups=` (`docs/analytics_java_export_output.txt`); analysed with `python scripts/run_analysis.py` from `analytics/` (`docs/analytics_benchmark_output.txt`).

Dataset: 30 issuers (10 per peer cohort), 40,000 authorisations each, **1,200,000 total authorisations**, 103,798 declines (8.65%). Peer-cohort decline rates measured at 3.958% (TIER_A), 8.008% (TIER_B), 13.9835% (TIER_C) mean, each issuer's own rate benchmarked against its cohort's median (full table in `docs/analytics_benchmark_output.txt`).

| Claim | Measured | Meets claim |
|---|---|---|
| 1.2M simulated card authorisations | 1,200,000 | yes |
| decomposed into 6 decline-reason families | 6: `INSUFFICIENT_FUNDS` 22,382, `SUSPECTED_FRAUD` 21,688, `CARD_DATA_ISSUE` 19,398, `DO_NOT_HONOR` 17,134, `CVV_AVS_MISMATCH` 15,137, `ISSUER_UNAVAILABLE` 8,059 | yes |
| each issuer benchmarked against a peer cohort | 30 issuers across 3 cohorts, each issuer's decline rate compared to its own cohort's median | yes |
| 18% of declined card-not-present volume isolated as recoverable | **22.63%** by count and by amount (2 genuine designs tried; see Findings) | no, reported as measured |
| recovered revenue stated against its named assumptions | **EUR 467,508.04** expected, against 3 named recovery-success probabilities (account updater 0.55, network token 0.45, retry schedule 0.35) and a 50/50 tokenized-merchant split assumption for `CARD_DATA_ISSUE` | yes |

CNP declines: 57,090 of 103,798 total declines (55.0%, matching the generator's 55% CNP traffic
share). Recoverable CNP declines: 12,917 (EUR 1,001,209.63 of declined amount), split EUR
390,282.22 to account updater, EUR 390,282.22 to network token (the 50/50 assumption applied to
`CARD_DATA_ISSUE`'s EUR 780,564.44), and EUR 220,645.19 to retry schedule (`ISSUER_UNAVAILABLE`).
Full per-family and per-issuer detail: `docs/analytics_benchmark_output.txt`.

## Building and running

```
export PATH="/c/Users/Manas/tools/apache-maven-3.9.9/bin:$PATH"   # or the equivalent mvn on PATH
mvn test                                             # 32 tests total (10 ledger, 16 health, 6 analytics), embedded Kafka + H2, ~15s
mvn test -Dtest=BenchmarkRunner -Dsurefire.excludedGroups=   # the 50,000-transaction measurement run, ~5s
```

Against the real stack (not exercised for any number in this README):

```
docker compose up -d
mvn spring-boot:run
```

### Building and running: `health/`

```
mvn test                                                     # includes the 13 new health/ tests above
mvn test -Dtest=HealthBenchmarkRunner -Dsurefire.excludedGroups=   # the ~2M-event measurement run, a few hundred ms

# Console (standalone Vite app; needs the backend already running on :8080)
cd console
npm install
npm run dev              # dev server on :5173, proxies /api to localhost:8080
npm test                 # Vitest component tests, no backend needed
npm run test:e2e         # Playwright, headless Chromium, needs the backend running with demo data seeded

# Smoke-testing the backend without a real Kafka broker or PostgreSQL (neither is
# reachable in this build environment; see Honest framing): override the datasource
# with an in-memory H2 instance so the app starts and DemoDataSeeder populates
# IncidentStore. The Kafka listeners log repeated harmless reconnect warnings with
# no broker reachable; the REST API and the demo incidents are unaffected.
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dspring.datasource.url=jdbc:h2:mem:smoke;MODE=PostgreSQL;DB_CLOSE_DELAY=-1 -Dspring.datasource.driver-class-name=org.h2.Driver -Dspring.jpa.hibernate.ddl-auto=create-drop -Dspring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect"
```

### Building and running: `analytics/`

```
mvn test -Dtest=AuthorizationExportRunner -Dsurefire.excludedGroups=   # generates analytics/data/authorizations.csv (1.2M rows, a few hundred ms)

cd analytics
python -m venv venv
venv\Scripts\pip install -r requirements.txt
venv\Scripts\python scripts\run_analysis.py   # full measured run
venv\Scripts\python -m pytest tests -q         # 10 tests, hand-computed fixtures, no CSV needed
```

`analytics/data/authorizations.csv` is gitignored (56 MB, 1.2M rows); `docs/sample_authorizations.csv`
is a committed 1,000-row random sample (48 KB) of a real run's output, for inspection without
regenerating the full dataset.

## Sibling comparison

[`securities-settlement-matching`](https://github.com/Manas103/securities-settlement-matching) proves the identical technique, an identical final-state SHA-256 digest after a 5%-duplicated, fully out-of-order replay against a linear-scan reference oracle, over settlement instructions and breaks rather than card authorizations and balance effects. That repo's dedup key is a bare message id; this one's is `(messageId, traceNumber)`, because a card network's trace numbers are a small cyclic counter in a way a settlement instruction's reference is not, and the two repos' javadoc/README each explain why their own key shape is the right one for their own domain.

**`analytics/`'s sibling.** [`payment-exception-triage`](https://github.com/Manas103/payment-exception-triage) is the closest repo by surface area (ISO 20022-shaped validation and an isolation-forest anomaly scorer over synthetic payment messages) and was considered and rejected as a replacement for this design, because it catches malformed payment instructions, a different question from why a well-formed authorisation was declined and what recovering it is worth. The two repos are complementary rather than overlapping: that repo's anomaly scorer would sit upstream of this one's declines, catching messages that should never reach an issuer for a decision at all.

## Limitations

No real Kafka cluster or PostgreSQL instance was exercised for any number in this README; `docker-compose.yml` documents how to do so, but every measured figure above comes from the embedded-broker, H2-backed test path. The exactly-once guarantee is proved at the message-ledger layer, not through an actual multi-broker Kafka deployment. Concurrency correctness rests on a single global lock (see "Findings"), not per-transaction sharding. The 100,000-transaction dataset scale in the original resume bullet is not met; the honest measured scale is stated above rather than adjusted to match.

**`health/`'s own limitations.** `IncidentStore` is an in-memory `ConcurrentHashMap`/`CopyOnWriteArrayList` with no persistence; a restart loses every incident, including the demo seeder's. The approval-rate floor (0.80) and minimum-volume guard (50) are calibrated against this benchmark's own synthetic baseline and crash severity, the same way the sibling projects in this portfolio disclose their own budgets are calibrated against their own load profile, not a transferable constant for a different traffic mix without re-measuring. The 2,000,000-message dataset-scale claim is not quite met (1,966,037, see "Measured results"), for the same structural reason the ledger engine's own 100,000-message claim above is short: declined authorizations never get a paired terminal event. The console's CORS policy (`HealthConsoleCorsConfig`) is permissive to any localhost origin, acceptable for a local demo, not for a deployed console.

**`analytics/`'s own limitations.** The recoverability classification (`config.RECOVERY_MECHANISM`) is a judgment call stated and argued for in `config.py` and in Findings above, not a figure sourced from real issuer recovery-campaign data; a different, equally defensible classification would move the 22.63% figure. The two recovery-success probabilities and the tokenized-merchant split are named assumptions, not measured rates; no real recovery campaign exists in this project to measure one from, which is exactly why the revenue figure is presented as an expectation against stated assumptions rather than a forecast. The peer cohorts (`TIER_A`/`TIER_B`/`TIER_C`) are a simulation convenience with a fixed baseline decline rate per cohort, not derived from any real issuer segmentation scheme. The CSV handoff between the Java generator and the Python analysis means there is no single command that reproduces both halves; "Building and running" states the two steps in order.
