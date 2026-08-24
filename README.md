# Card Authorization and Reversal Gateway with Exactly-Once Balance Effects

A Spring Boot gateway that consumes authorization, reversal and clearing messages off a Kafka stream, holds funds on approval and releases or clears them on a terminal event, keyed so that every balance effect happens exactly once no matter how many times a message is redelivered or how far out of order it arrives. Java 17, Spring Boot 3.3.4, Spring Kafka, PostgreSQL. Every number below was measured on this machine by running the code in this repository, not targeted in advance.

## Why this exists

The dual-message model, hold funds at authorization, settle them at a later, separate clearing event, is how card networks avoid moving money before a merchant has actually captured a sale, and it is also exactly the shape of problem that breaks under redelivery: an authorization can legitimately be retried, a reversal can race a clearing message for the same transaction, and none of that is allowed to double-post or double-release a balance. This repository is a small, honest version of the gateway that sits at that seam: it proves, with a real replay and a real independent oracle, that its balance effects stay exactly-once under duplication and reordering.

## Honest framing, up front

- **This is a matching-and-ledger core, not a card network.** No ISO 8583 wire format, no acquirer or issuer integration, no real settlement rail. Three structured message types in, a held, released or cleared balance effect out.
- **The traffic is entirely synthetic.** `SyntheticMessageGenerator` produces a seeded, deterministic stream of authorization/reversal/clearing triples for 500 fictional accounts. There is no real cardholder, no real merchant, and no real transaction behind any of it.
- **The measured numbers below run against a real, embedded, in-process Kafka broker** (`spring-kafka-test`'s `EmbeddedKafkaKraftBroker`, KRaft mode), not a mock producer/consumer pair. It is genuine `KafkaProducer`/`KafkaConsumer` traffic over a genuine broker, just not a standalone multi-node cluster. `docker-compose.yml` documents the standalone Kafka and PostgreSQL stack the production `application.yml` profile targets, but Docker Desktop's daemon was not running in the environment this project was built in (`docker ps` fails), so no number in this README was measured against it.
- **Tests and the measurement run use H2 in PostgreSQL-compatibility mode** (`MODE=PostgreSQL`), not real PostgreSQL, for the same reason. `src/main/resources/application.yml` is the real production profile (real Kafka, real PostgreSQL, `ddl-auto: validate`); `src/test/resources/application.yml` is the only place H2 is configured.
- **The resume's "100,000 synthetic transactions" claim is a message count, not a transaction count.** The measurement run below uses 50,000 synthetic transactions, each producing an authorization plus one terminal event (reversal or clearing), for 99,900 total messages (100 transactions are deliberately left authorization-only, in flight). The measured, honest figure is stated in full below rather than silently relabeled to read as 100,000; see "Measured results".
- **Machine and toolchain.** AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 Home. JDK 21 (Temurin) compiling to Java 17 bytecode (`maven.compiler.release=17`). Spring Boot 3.3.4, Maven 3.9.9, `spring-kafka` / `spring-kafka-test` 3.2.x, H2 2.x, PostgreSQL JDBC driver (runtime only, unused by any test in this repository).

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
```

**Why the dedup key is `(messageId, traceNumber)`, not `messageId` alone.** A message id is only as unique as whatever generated it, and real card-network trace numbers are commonly a small cyclic counter (ISO 8583's STAN wraps at 999999). Scoping the dedup key to the specific transaction the message claims (`traceNumber`) before trusting `messageId` means a message-id collision between two genuinely different transactions can never masquerade as a redelivery of one message. `AuthorizationLedgerEngine`'s javadoc carries the full argument.

**Why a terminal message arriving before its authorization is buffered, not dropped.** Kafka guarantees ordering only within a partition, and nothing requires a producer to key an authorization and its terminal event onto the same partition. A reversal or clearing naming a `traceNumber` the engine has not yet authorized is held in `pendingTerminal` and applied the instant the matching authorization arrives, so the final balance is identical regardless of which message the stream delivers first.

**Why a reference oracle rather than a second implementation of the same algorithm.** `ReferenceOracleLedger` re-derives the same final state by replaying every message through a naive, unindexed, obviously-correct pass (no early-buffering optimization, no hash index) and is diffed field-for-field against the real engine's output over the same input. A shared bug in both implementations would still be caught by neither; that risk is accepted here in exchange for the oracle being simple enough to read in one sitting; the noisy-replay digest check below is the stronger, implementation-independent proof.

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

## Findings

The one deliberately disclosed imperfection: `AuthorizationLedgerEngine.ingest` is a single `synchronized` method, so correctness under concurrent Kafka listener threads is guarded by a global lock rather than per-transaction locking. That is the honest trade for this size of project, it makes the exactly-once guarantee trivial to reason about and to diff against the oracle, at the cost of serializing every message through one lock regardless of how many partitions or listener threads are configured. A production version would shard the lock by `traceNumber` or `accountId`; the measured p99 latency below is for that single-locked path, not a sharded one, and is stated as such.

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

## Building and running

```
export PATH="/c/Users/Manas/tools/apache-maven-3.9.9/bin:$PATH"   # or the equivalent mvn on PATH
mvn test                                             # 10 tests, embedded Kafka + H2, ~10s
mvn test -Dtest=BenchmarkRunner -Dsurefire.excludedGroups=   # the 50,000-transaction measurement run, ~5s
```

Against the real stack (not exercised for any number in this README):

```
docker compose up -d
mvn spring-boot:run
```

## Sibling comparison

[`securities-settlement-matching`](https://github.com/Manas103/securities-settlement-matching) proves the identical technique, an identical final-state SHA-256 digest after a 5%-duplicated, fully out-of-order replay against a linear-scan reference oracle, over settlement instructions and breaks rather than card authorizations and balance effects. That repo's dedup key is a bare message id; this one's is `(messageId, traceNumber)`, because a card network's trace numbers are a small cyclic counter in a way a settlement instruction's reference is not, and the two repos' javadoc/README each explain why their own key shape is the right one for their own domain.

## Limitations

No real Kafka cluster or PostgreSQL instance was exercised for any number in this README; `docker-compose.yml` documents how to do so, but every measured figure above comes from the embedded-broker, H2-backed test path. The exactly-once guarantee is proved at the message-ledger layer, not through an actual multi-broker Kafka deployment. Concurrency correctness rests on a single global lock (see "Findings"), not per-transaction sharding. The 100,000-transaction dataset scale in the original resume bullet is not met; the honest measured scale is stated above rather than adjusted to match.
