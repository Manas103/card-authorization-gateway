package com.manas.cardauth.bench;

import com.manas.cardauth.engine.AuthorizationLedgerEngine;
import com.manas.cardauth.engine.BalanceSnapshot;
import com.manas.cardauth.engine.LedgerOutcome;
import com.manas.cardauth.engine.TransactionResolution;
import com.manas.cardauth.generator.NoisyStreamBuilder;
import com.manas.cardauth.generator.SyntheticMessageGenerator;
import com.manas.cardauth.kafka.CardMessageJson;
import com.manas.cardauth.model.CardMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real measurement run behind claims 4 through 7 of the resume
 * bullet. Not part of the default {@code mvn test} run (tagged
 * {@code benchmark}, excluded by the surefire configuration in
 * {@code pom.xml}); run explicitly with
 * {@code mvn test -Dtest=BenchmarkRunner -Dsurefire.excludedGroups=}.
 *
 * <p>Uses a real, in-process Kafka broker (spring-kafka-test's
 * {@code EmbeddedKafkaKraftBroker}, KRaft mode, not a mock producer/
 * consumer) for both the canonical 100,000-message run and the noisy
 * replay. See the README's "honest framing" section for exactly what
 * this does and does not claim about production Kafka.
 *
 * <p>"Decision latency" (claim 7) is measured as the wall-clock time of
 * {@link AuthorizationLedgerEngine#ingest(CardMessage)} alone, timed
 * individually around each call as messages are drained from the
 * consumer, deliberately excluding Kafka poll/network time, since the
 * claim is about the engine's own decision cost on one node, not about
 * end-to-end Kafka consumer-group latency.
 */
class BenchmarkRunner {

    private static final int TRANSACTION_COUNT = 50_000; // 100,000 messages total (auth + one terminal each, minus in-flight)
    private static final int ACCOUNT_COUNT = 500;
    private static final int IN_FLIGHT_COUNT = 100;
    private static final double REVERSAL_FRACTION = 0.15;
    private static final long GENERATOR_SEED = 20260810L;
    private static final long NOISE_SEED = 837465291L;

    @Test
    @Tag("benchmark")
    void measureFullSyntheticStreamOverEmbeddedKafka() throws Exception {
        StringBuilder report = new StringBuilder();
        line(report, "=== Card Authorization Gateway -- benchmark run ===");
        line(report, "started: " + Instant.now());

        SyntheticMessageGenerator generator = new SyntheticMessageGenerator();
        SyntheticMessageGenerator.GeneratedStream stream =
                generator.generate(TRANSACTION_COUNT, ACCOUNT_COUNT, IN_FLIGHT_COUNT, REVERSAL_FRACTION, GENERATOR_SEED);

        line(report, "");
        line(report, "-- dataset --");
        line(report, "transactions: " + TRANSACTION_COUNT);
        line(report, "accounts: " + ACCOUNT_COUNT);
        line(report, "in-flight (authorization only, no terminal): " + IN_FLIGHT_COUNT);
        line(report, "total messages: " + stream.messages().size());

        EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 4, "bench-canonical", "bench-noisy");
        broker.afterPropertiesSet();
        try {
            String bootstrapServers = broker.getBrokersAsString();

            line(report, "");
            line(report, "-- canonical run (in order, no duplicates) --");
            long t0 = System.nanoTime();
            produce(bootstrapServers, "bench-canonical", stream.messages());
            AuthorizationLedgerEngine canonicalEngine = new AuthorizationLedgerEngine(stream.openingBalances());
            List<Long> canonicalLatenciesNanos = new ArrayList<>(stream.messages().size());
            consumeInto(bootstrapServers, "bench-canonical", "bench-group-canonical", stream.messages().size(), canonicalEngine, canonicalLatenciesNanos);
            long canonicalMillis = (System.nanoTime() - t0) / 1_000_000;

            List<TransactionResolution> canonicalResolutions = canonicalEngine.resolutions();
            List<String> canonicalStillHeld = canonicalEngine.stillHeldTraceNumbers();

            line(report, "elapsed: " + canonicalMillis + " ms");
            line(report, "resolved transactions: " + canonicalResolutions.size());
            line(report, "still held (authorization only, no terminal seen): " + canonicalStillHeld.size());
            line(report, "distinct (messageId, traceNumber) keys seen: " + canonicalEngine.distinctKeysSeen());

            long resolvedClearedCount = canonicalResolutions.stream().filter(r -> r.effect().name().equals("CLEARED")).count();
            long resolvedReleasedCount = canonicalResolutions.size() - resolvedClearedCount;
            line(report, "cleared: " + resolvedClearedCount + ", released (reversed): " + resolvedReleasedCount);

            reportLatencyPercentiles(report, "canonical", canonicalLatenciesNanos);

            String canonicalDigest = digestFinalState(canonicalEngine.balanceSnapshots(), canonicalResolutions, canonicalStillHeld);
            line(report, "");
            line(report, "canonical final-state digest (SHA-256): " + canonicalDigest);

            line(report, "");
            line(report, "-- noisy replay (5% duplicated, fully shuffled out of transaction order) --");
            List<CardMessage> noisy = NoisyStreamBuilder.buildNoisyStream(stream.messages(), 0.05, NOISE_SEED);
            line(report, "noisy stream size: " + noisy.size() + " (" + (noisy.size() - stream.messages().size()) + " duplicated deliveries)");

            long t1 = System.nanoTime();
            produce(bootstrapServers, "bench-noisy", noisy);
            AuthorizationLedgerEngine noisyEngine = new AuthorizationLedgerEngine(stream.openingBalances());
            List<Long> noisyLatenciesNanos = new ArrayList<>(noisy.size());
            consumeInto(bootstrapServers, "bench-noisy", "bench-group-noisy", noisy.size(), noisyEngine, noisyLatenciesNanos);
            long noisyMillis = (System.nanoTime() - t1) / 1_000_000;

            List<TransactionResolution> noisyResolutions = noisyEngine.resolutions();
            List<String> noisyStillHeld = noisyEngine.stillHeldTraceNumbers();
            String noisyDigest = digestFinalState(noisyEngine.balanceSnapshots(), noisyResolutions, noisyStillHeld);

            line(report, "elapsed: " + noisyMillis + " ms");
            line(report, "resolved transactions: " + noisyResolutions.size());
            line(report, "still held: " + noisyStillHeld.size());
            line(report, "distinct (messageId, traceNumber) keys seen (post-dedup): " + noisyEngine.distinctKeysSeen());
            line(report, "noisy final-state digest (SHA-256): " + noisyDigest);

            boolean identical = canonicalDigest.equals(noisyDigest);
            line(report, "");
            line(report, "identical final state after noisy replay: " + identical);

            line(report, "");
            line(report, "finished: " + Instant.now());

            System.out.println(report);

            assertThat(canonicalResolutions.size() + canonicalStillHeld.size()).isEqualTo(TRANSACTION_COUNT);
            assertThat(noisyEngine.distinctKeysSeen()).isEqualTo(stream.messages().size());
            assertThat(identical).isTrue();
        } finally {
            broker.destroy();
        }
    }

    private void reportLatencyPercentiles(StringBuilder report, String label, List<Long> latenciesNanos) {
        List<Long> sorted = new ArrayList<>(latenciesNanos);
        Collections.sort(sorted);
        int n = sorted.size();
        double p50Ms = sorted.get((int) (n * 0.50)) / 1_000_000.0;
        double p99Ms = sorted.get((int) (n * 0.99)) / 1_000_000.0;
        double maxMs = sorted.get(n - 1) / 1_000_000.0;
        line(report, "");
        line(report, "-- " + label + " decision latency (AuthorizationLedgerEngine.ingest alone, n=" + n + ") --");
        line(report, "p50: " + String.format("%.4f", p50Ms) + " ms");
        line(report, "p99: " + String.format("%.4f", p99Ms) + " ms");
        line(report, "max: " + String.format("%.4f", maxMs) + " ms");
    }

    private void line(StringBuilder sb, String text) {
        sb.append(text).append(System.lineSeparator());
    }

    private void produce(String bootstrapServers, String topic, List<CardMessage> messages) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (CardMessage message : messages) {
                producer.send(new ProducerRecord<>(topic, message.traceNumber(), CardMessageJson.toJson(message)));
            }
            producer.flush();
        }
    }

    private void consumeInto(String bootstrapServers, String topic, String groupId, int expectedCount,
                              AuthorizationLedgerEngine engine, List<Long> latenciesNanosOut) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            int consumed = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (consumed < expectedCount && System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    CardMessage message = CardMessageJson.fromJson(record.value());
                    long start = System.nanoTime();
                    LedgerOutcome outcome = engine.ingest(message);
                    long elapsed = System.nanoTime() - start;
                    latenciesNanosOut.add(elapsed);
                    if (outcome == null) {
                        throw new IllegalStateException("engine.ingest returned null");
                    }
                    consumed++;
                }
            }
            if (consumed < expectedCount) {
                throw new IllegalStateException("consumed only " + consumed + " of " + expectedCount + " expected records from " + topic);
            }
        }
    }

    private String digestFinalState(List<BalanceSnapshot> balances, List<TransactionResolution> resolutions, List<String> stillHeld) {
        List<String> balanceLines = balances.stream()
                .map(b -> "BALANCE|" + b.accountId() + "|" + b.availableBalance().toPlainString()
                        + "|" + b.heldAmount().toPlainString() + "|" + b.postedBalance().toPlainString())
                .sorted()
                .toList();
        List<String> resolutionLines = resolutions.stream()
                .map(r -> "RESOLUTION|" + r.traceNumber() + "|" + r.accountId() + "|" + r.effect()
                        + "|" + r.amount().toPlainString())
                .sorted()
                .toList();
        List<String> stillHeldLines = stillHeld.stream()
                .map(t -> "STILLHELD|" + t)
                .sorted()
                .toList();

        List<String> all = new ArrayList<>(balanceLines.size() + resolutionLines.size() + stillHeldLines.size());
        all.addAll(balanceLines);
        all.addAll(resolutionLines);
        all.addAll(stillHeldLines);
        String canonicalSerialization = String.join("\n", all);

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonicalSerialization.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
