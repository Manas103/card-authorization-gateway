package com.manas.cardauth.health;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class HealthStreamGeneratorTest {

    @Test
    void producesTheRequestedNumberOfEpisodesOnDistinctBins() {
        HealthStreamGenerator.GeneratedHealthStream stream = new HealthStreamGenerator()
                .generate(10, 500, 0.95, 0.20, 4, Duration.ofSeconds(20), Duration.ofSeconds(2000), 42L);

        assertEquals(4, stream.episodes().size());
        long distinctBins = stream.episodes().stream().map(HealthStreamGenerator.IncidentEpisode::issuerBin).distinct().count();
        assertEquals(4, distinctBins);
    }

    @Test
    void sameSeedAndParametersProduceTheIdenticalStream() {
        HealthStreamGenerator generator = new HealthStreamGenerator();
        var first = generator.generate(5, 200, 0.95, 0.20, 2, Duration.ofSeconds(20), Duration.ofSeconds(1000), 7L);
        var second = generator.generate(5, 200, 0.95, 0.20, 2, Duration.ofSeconds(20), Duration.ofSeconds(1000), 7L);

        assertEquals(first.events(), second.events());
        assertEquals(first.episodes(), second.episodes());
    }

    @Test
    void eventsAreSortedByOccurredAt() {
        var stream = new HealthStreamGenerator().generate(8, 300, 0.95, 0.20, 3,
                Duration.ofSeconds(20), Duration.ofSeconds(1500), 99L);
        for (int i = 1; i < stream.events().size(); i++) {
            assertFalse(stream.events().get(i).occurredAt().isBefore(stream.events().get(i - 1).occurredAt()));
        }
    }

    @Test
    void everyEpisodeFallsFullyInsideTheActiveSpanWithMargin() {
        Instant epoch = Instant.parse("2026-09-01T00:00:00Z");
        Duration span = Duration.ofSeconds(1000);
        Duration episodeDuration = Duration.ofSeconds(20);
        var stream = new HealthStreamGenerator().generate(6, 300, 0.95, 0.20, 3, episodeDuration, span, 11L);

        for (var episode : stream.episodes()) {
            long onsetOffset = episode.onset().getEpochSecond() - epoch.getEpochSecond();
            assertTrue(onsetOffset >= 0 && onsetOffset + episodeDuration.getSeconds() <= span.getSeconds());
        }
    }
}
