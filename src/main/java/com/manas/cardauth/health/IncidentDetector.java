package com.manas.cardauth.health;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Raises an incident only once a minimum-volume guard is met (claim 3): a
 * window with too few authorizations to trust its approval rate statistically
 * (a handful of declines on a quiet BIN) is never paged on, no matter how low
 * that rate reads. {@code approvalRateFloor} is calibrated well below this
 * repository's synthetic baseline (roughly 0.95 to 0.97, see
 * HealthStreamGenerator), so a genuine baseline window never crosses it; see
 * the README's Measured results for the honest false-page count this
 * produced.
 */
public final class IncidentDetector {

    private final int minimumVolume;
    private final double approvalRateFloor;
    private final List<Incident> incidents = new ArrayList<>();
    private final AtomicLong nextId = new AtomicLong(1);

    public IncidentDetector(int minimumVolume, double approvalRateFloor) {
        this.minimumVolume = minimumVolume;
        this.approvalRateFloor = approvalRateFloor;
    }

    public void onWindowClose(WindowSummary summary) {
        evaluate(summary).ifPresent(incidents::add);
    }

    /** Same decision {@link #onWindowClose} makes, without mutating this detector's own list. */
    public Optional<Incident> evaluate(WindowSummary summary) {
        if (summary.totalAuthorizations() < minimumVolume) {
            return Optional.empty();
        }
        if (summary.approvalRate() >= approvalRateFloor) {
            return Optional.empty();
        }
        return Optional.of(new Incident(
                nextId.getAndIncrement(),
                summary.issuerBin(),
                summary.windowStart(),
                summary.windowEnd(),
                summary.windowEnd(),
                summary.approvalRate(),
                summary.totalAuthorizations(),
                summary.dominantDeclineCode()
        ));
    }

    public List<Incident> incidents() {
        return List.copyOf(incidents);
    }

    public int minimumVolume() {
        return minimumVolume;
    }

    public double approvalRateFloor() {
        return approvalRateFloor;
    }
}
