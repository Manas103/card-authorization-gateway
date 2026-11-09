package com.manas.cardauth.health;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Deterministic, seeded synthetic authorization/reversal/clearing traffic
 * across many issuer BINs, with a fixed number of injected incident episodes
 * at known onset times (ground truth used only for measurement, never fed to
 * {@link IncidentDetector}). Mirrors {@code SyntheticMessageGenerator}'s
 * seeded-and-reproducible convention for the ledger engine.
 */
public final class HealthStreamGenerator {

    private static final DeclineCode[] CODES = DeclineCode.values();

    public record IncidentEpisode(String issuerBin, Instant onset, Duration duration, DeclineCode dominantCode) {
    }

    public record GeneratedHealthStream(List<AuthEvent> events, List<IncidentEpisode> episodes) {
    }

    /**
     * @param binCount           number of issuer BINs sharing the stream
     * @param authsPerBin        authorizations generated for each BIN
     * @param baselineApprovalRate approval rate outside any injected episode
     * @param crashedApprovalRate approval rate inside an injected episode
     * @param episodeCount       number of injected episodes, one per distinct BIN (capped at binCount)
     * @param episodeDuration    how long each injected episode lasts in synthetic event time
     * @param activeSpan         the synthetic time span BINs are active over, starting at epoch
     * @param seed               RNG seed; same seed and parameters always produce the same stream
     */
    public GeneratedHealthStream generate(int binCount, int authsPerBin, double baselineApprovalRate,
                                           double crashedApprovalRate, int episodeCount,
                                           Duration episodeDuration, Duration activeSpan, long seed) {
        if (episodeCount > binCount) {
            throw new IllegalArgumentException("cannot inject more episodes than BINs");
        }
        Random random = new Random(seed);
        Instant epoch = Instant.parse("2026-09-01T00:00:00Z");

        List<IncidentEpisode> episodes = new ArrayList<>(episodeCount);
        // Margin keeps every episode fully inside the active span, away from the edges.
        long marginSeconds = episodeDuration.getSeconds() * 2;
        long activeSeconds = activeSpan.getSeconds();
        for (int i = 0; i < episodeCount; i++) {
            String bin = "BIN-%06d".formatted(i);
            long onsetOffset = marginSeconds + (long) (random.nextDouble() * (activeSeconds - 2 * marginSeconds));
            episodes.add(new IncidentEpisode(bin, epoch.plusSeconds(onsetOffset), episodeDuration,
                    CODES[random.nextInt(CODES.length)]));
        }

        List<AuthEvent> events = new ArrayList<>(authsPerBin * binCount * 2);
        long sequence = 0;
        for (int b = 0; b < binCount; b++) {
            String bin = "BIN-%06d".formatted(b);
            IncidentEpisode episode = b < episodeCount ? episodes.get(b) : null;

            for (int i = 0; i < authsPerBin; i++) {
                long offsetSeconds = (long) ((i / (double) authsPerBin) * activeSeconds)
                        + random.nextInt(2);
                Instant occurredAt = epoch.plusSeconds(offsetSeconds);

                boolean inEpisode = episode != null
                        && !occurredAt.isBefore(episode.onset())
                        && occurredAt.isBefore(episode.onset().plus(episode.duration()));
                double approvalRate = inEpisode ? crashedApprovalRate : baselineApprovalRate;
                boolean approved = random.nextDouble() < approvalRate;
                DeclineCode declineCode = null;
                if (!approved) {
                    declineCode = inEpisode ? episode.dominantCode() : CODES[random.nextInt(CODES.length)];
                }

                events.add(new AuthEvent("EVT-AUTH-%010d".formatted(sequence), bin, AuthEventKind.AUTHORIZATION,
                        approved, declineCode, occurredAt, sequence));
                sequence++;

                if (approved) {
                    Instant terminalAt = occurredAt.plusSeconds(1 + random.nextInt(20));
                    AuthEventKind terminalKind = random.nextDouble() < 0.08 ? AuthEventKind.REVERSAL : AuthEventKind.CLEARING;
                    events.add(new AuthEvent("EVT-TERM-%010d".formatted(sequence), bin, terminalKind,
                            false, null, terminalAt, sequence));
                    sequence++;
                }
            }
        }

        events.sort((a, c) -> a.occurredAt().compareTo(c.occurredAt()));
        return new GeneratedHealthStream(events, episodes);
    }
}
