package com.manas.cardauth.health;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real measurement run behind the Authorization Health Monitor's
 * claims: the 2,000,000-ish-message stream, the 24 injected incidents, the
 * "named within 30 seconds of onset" catch rate, and the false-page count.
 * Not part of the default {@code mvn test} run (tagged {@code benchmark},
 * same convention as {@code BenchmarkRunner}); run explicitly with
 * {@code mvn test -Dtest=HealthBenchmarkRunner -Dsurefire.excludedGroups=}.
 *
 * <p>Runs {@link HealthStreamGenerator}'s output directly through
 * {@link BinWindowAggregator} and {@link IncidentDetector} in process,
 * bypassing Kafka at this scale for the same reason {@code BenchmarkRunner}
 * times {@code AuthorizationLedgerEngine.ingest} alone: the claim under
 * measurement is the detector's own decision quality and the aggregator's
 * own per-event cost, not Kafka consumer-group throughput. Kafka itself is
 * exercised for real, at a far smaller scale, by
 * {@code AuthEventHealthIntegrationTest}.
 */
class HealthBenchmarkRunner {

    private static final int BIN_COUNT = 50;
    private static final int AUTHS_PER_BIN = 20_000;
    private static final double BASELINE_APPROVAL_RATE = 0.97;
    private static final double CRASHED_APPROVAL_RATE = 0.15;
    private static final int EPISODE_COUNT = 24;
    private static final Duration EPISODE_DURATION = Duration.ofSeconds(20);
    private static final Duration ACTIVE_SPAN = Duration.ofSeconds(2000);
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(10);
    private static final int MINIMUM_VOLUME = 50;
    private static final double APPROVAL_RATE_FLOOR = 0.80;
    private static final long SEED = 20260907L;
    private static final Duration DETECTION_DEADLINE = Duration.ofSeconds(30);

    @Test
    @Tag("benchmark")
    void measureIncidentDetectionOverTheFullSyntheticStream() {
        StringBuilder report = new StringBuilder();
        line(report, "=== Authorization Health Monitor and Incident Console -- benchmark run ===");
        line(report, "started: " + Instant.now());

        HealthStreamGenerator generator = new HealthStreamGenerator();
        HealthStreamGenerator.GeneratedHealthStream stream = generator.generate(
                BIN_COUNT, AUTHS_PER_BIN, BASELINE_APPROVAL_RATE, CRASHED_APPROVAL_RATE,
                EPISODE_COUNT, EPISODE_DURATION, ACTIVE_SPAN, SEED);

        line(report, "");
        line(report, "-- dataset --");
        line(report, "issuer BINs: " + BIN_COUNT);
        line(report, "authorizations per BIN: " + AUTHS_PER_BIN);
        line(report, "total events (authorizations + reversals/clearings): " + stream.events().size());
        line(report, "injected incident episodes: " + stream.episodes().size());
        line(report, "window size: " + WINDOW_SIZE.getSeconds() + "s, minimum-volume guard: " + MINIMUM_VOLUME
                + ", approval-rate floor: " + APPROVAL_RATE_FLOOR);

        IncidentDetector detector = new IncidentDetector(MINIMUM_VOLUME, APPROVAL_RATE_FLOOR);
        BinWindowAggregator aggregator = new BinWindowAggregator(WINDOW_SIZE, detector::onWindowClose);

        long t0 = System.nanoTime();
        for (AuthEvent event : stream.events()) {
            aggregator.record(event);
        }
        aggregator.flush();
        long elapsedMillis = (System.nanoTime() - t0) / 1_000_000;

        List<Incident> incidents = detector.incidents();
        line(report, "");
        line(report, "-- processing --");
        line(report, "elapsed: " + elapsedMillis + " ms for " + stream.events().size() + " events");
        line(report, "incidents raised: " + incidents.size());

        // Match each injected episode to the earliest incident on the same BIN whose
        // window overlaps [onset, onset + episodeDuration), the ground truth this
        // benchmark alone has access to; IncidentDetector never sees episode data. An
        // episode spanning more than one window can legitimately raise more than one
        // incident (both attributed to the same episode, neither a false page); a
        // false page is an incident that overlaps no episode at all, on any BIN.
        int caughtWithinDeadline = 0;
        List<String> perEpisodeLines = new ArrayList<>();
        for (HealthStreamGenerator.IncidentEpisode episode : stream.episodes()) {
            Instant episodeEnd = episode.onset().plus(episode.duration());
            Incident earliest = null;
            for (Incident incident : incidents) {
                if (!incident.issuerBin().equals(episode.issuerBin())) continue;
                boolean overlaps = incident.windowStart().isBefore(episodeEnd) && incident.windowEnd().isAfter(episode.onset());
                if (overlaps && (earliest == null || incident.detectedAt().isBefore(earliest.detectedAt()))) {
                    earliest = incident;
                }
            }
            if (earliest != null) {
                Duration delay = Duration.between(episode.onset(), earliest.detectedAt());
                boolean withinDeadline = !delay.isNegative() && delay.compareTo(DETECTION_DEADLINE) <= 0;
                if (withinDeadline) caughtWithinDeadline++;
                perEpisodeLines.add(episode.issuerBin() + " onset=" + episode.onset() + " detectedAt=" + earliest.detectedAt()
                        + " delay=" + delay.getSeconds() + "s dominantCode=" + earliest.dominantDeclineCode()
                        + " injectedCode=" + episode.dominantCode() + " withinDeadline=" + withinDeadline);
            } else {
                perEpisodeLines.add(episode.issuerBin() + " onset=" + episode.onset() + " MISSED (no overlapping incident)");
            }
        }

        long falsePages = incidents.stream()
                .filter(incident -> stream.episodes().stream().noneMatch(episode ->
                        episode.issuerBin().equals(incident.issuerBin())
                                && incident.windowStart().isBefore(episode.onset().plus(episode.duration()))
                                && incident.windowEnd().isAfter(episode.onset())))
                .count();

        line(report, "");
        line(report, "-- per-episode detection --");
        for (String l : perEpisodeLines) line(report, l);

        line(report, "");
        line(report, "-- summary --");
        line(report, "episodes named within " + DETECTION_DEADLINE.getSeconds() + "s of onset: "
                + caughtWithinDeadline + " / " + EPISODE_COUNT);
        line(report, "false pages (incidents not matching any injected episode): " + falsePages);
        line(report, "finished: " + Instant.now());

        System.out.println(report);

        assertThat(stream.events().size()).isGreaterThan(1_800_000);
        assertThat(incidents).isNotEmpty();
    }

    private void line(StringBuilder sb, String text) {
        sb.append(text).append(System.lineSeparator());
    }
}
