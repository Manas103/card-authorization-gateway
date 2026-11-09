package com.manas.cardauth.health;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: a real (embedded) Kafka broker, the real
 * {@code @KafkaListener} ({@link HealthMonitorService}), and the real
 * aggregator/detector pipeline, all wired through the actual Spring
 * context (claim: "consumes a card authorization, reversal and clearing
 * stream off Kafka"). Kept to a small sample; the 2,000,000-message
 * measurement run lives in {@code HealthBenchmarkRunner} and is not part of
 * the default test suite.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"card-authorization-health-stream-test"})
class AuthEventHealthIntegrationTest {

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Autowired
    private KafkaTemplate<String, AuthEvent> authEventKafkaTemplate;

    @Autowired
    private IncidentStore store;

    @Test
    void realKafkaStreamRaisesAnIncidentOnceTheMinimumVolumeGuardIsMet() {
        String bin = "BIN-000099";
        Instant base = Instant.parse("2026-10-01T00:00:00Z");
        long seq = 0;

        // 5 healthy authorizations: approval rate alone must not be enough.
        for (int i = 0; i < 5; i++) {
            authEventKafkaTemplate.send("card-authorization-health-stream-test", bin,
                    new AuthEvent("EVT-" + seq, bin, AuthEventKind.AUTHORIZATION, true, null, base.plusSeconds(i), seq));
            seq++;
        }
        // 25 declines in the same window: volume (30) now clears the 20-event guard
        // configured for this test profile, and the approval rate (5/30 = 16.7%) is
        // well below the 0.80 floor.
        for (int i = 0; i < 25; i++) {
            authEventKafkaTemplate.send("card-authorization-health-stream-test", bin,
                    new AuthEvent("EVT-" + seq, bin, AuthEventKind.AUTHORIZATION, false, DeclineCode.SUSPECTED_FRAUD,
                            base.plusSeconds(5 + i % 4), seq));
            seq++;
        }
        // An event in the next window closes the first one.
        authEventKafkaTemplate.send("card-authorization-health-stream-test", bin,
                new AuthEvent("EVT-" + seq, bin, AuthEventKind.AUTHORIZATION, true, null, base.plusSeconds(15), seq));
        authEventKafkaTemplate.flush();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Incident> onBin = store.all().stream().filter(i -> i.issuerBin().equals(bin)).toList();
            assertThat(onBin).hasSize(1);
            Incident incident = onBin.get(0);
            assertThat(incident.windowVolume()).isEqualTo(30);
            assertThat(incident.approvalRateObserved()).isCloseTo(5.0 / 30.0, org.assertj.core.data.Offset.offset(1e-9));
            assertThat(incident.dominantDeclineCode()).isEqualTo(DeclineCode.SUSPECTED_FRAUD);

            WindowSummary segment = store.triggeringSegmentOf(incident.id());
            assertThat(segment).isNotNull();
            assertThat(segment.issuerBin()).isEqualTo(bin);
            assertThat(Duration.between(segment.windowStart(), segment.windowEnd())).isEqualTo(Duration.ofSeconds(10));
        });
    }
}
