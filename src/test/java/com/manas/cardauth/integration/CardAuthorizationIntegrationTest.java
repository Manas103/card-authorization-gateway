package com.manas.cardauth.integration;

import com.manas.cardauth.generator.SyntheticMessageGenerator;
import com.manas.cardauth.model.CardMessage;
import com.manas.cardauth.repository.AccountBalanceRepository;
import com.manas.cardauth.repository.LedgerEntryRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: a real (embedded) Kafka broker, the real
 * {@code @KafkaListener}, the real ledger engine, and a real H2 database
 * in PostgreSQL-compatibility mode, all wired through the actual Spring
 * context. Kept to a modest sample (500 transactions); the full
 * 100,000-message measurement run lives in {@code bench.BenchmarkRunner}
 * and is not part of the default test suite.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"card-authorization-messages-test"})
class CardAuthorizationIntegrationTest {

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Autowired
    private KafkaTemplate<String, CardMessage> kafkaTemplate;

    @Autowired
    private AccountBalanceRepository accountBalanceRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Test
    void endToEndThroughRealKafkaAndH2() {
        // The test profile seeds 20 accounts (see src/test/resources/application.yml);
        // keep transactions confined to that set and leave nothing in-flight so every
        // transaction resolves and the ledger table gets exactly 500 rows.
        SyntheticMessageGenerator generator = new SyntheticMessageGenerator();
        var stream = generator.generate(500, 20, 0, 0.15, 7L);

        for (CardMessage message : stream.messages()) {
            kafkaTemplate.send("card-authorization-messages-test", message.traceNumber(), message);
        }
        kafkaTemplate.flush();

        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(ledgerEntryRepository.count()).isEqualTo(500));

        assertThat(accountBalanceRepository.count()).isEqualTo(20);

        BigDecimal totalAvailable = BigDecimal.ZERO;
        BigDecimal totalHeld = BigDecimal.ZERO;
        BigDecimal totalPosted = BigDecimal.ZERO;
        BigDecimal totalOpening = BigDecimal.ZERO;
        for (var account : accountBalanceRepository.findAll()) {
            totalAvailable = totalAvailable.add(account.getAvailableBalance());
            totalHeld = totalHeld.add(account.getHeldAmount());
            totalPosted = totalPosted.add(account.getPostedBalance());
            totalOpening = totalOpening.add(account.getOpeningBalance());
            // Conservation identity per account: nothing created, nothing destroyed.
            assertThat(account.getAvailableBalance().add(account.getHeldAmount()).add(account.getPostedBalance()))
                    .isEqualByComparingTo(account.getOpeningBalance());
        }
        assertThat(totalAvailable.add(totalHeld).add(totalPosted)).isEqualByComparingTo(totalOpening);
        // No transaction was left in-flight, so nothing should still be on hold.
        assertThat(totalHeld).isEqualByComparingTo(BigDecimal.ZERO);
    }
}
