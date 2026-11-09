package com.manas.cardauth.health;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Tumbling, fixed-size rolling windows per issuer BIN (claim 1: "tracks
 * approval rate, decline-code mix and p99 decision latency per issuer BIN in
 * rolling windows"). Keyed per BIN rather than globally because an incident
 * on one BIN must not be diluted by, or hidden behind, healthy traffic on
 * every other BIN.
 *
 * <p>Windows are bucketed on each event's own {@code occurredAt} (synthetic
 * event time), not wall-clock arrival time, which is what makes replaying a
 * 2,000,000-message stream in a tight loop produce the same window
 * boundaries a real-time Kafka consumer would see. Events for a given BIN
 * must arrive in non-decreasing {@code occurredAt} order (true of the
 * generator's output and of a real, partition-ordered Kafka stream keyed by
 * BIN); a window is closed and emitted the instant a later event for the
 * same BIN crosses its boundary, not on a wall-clock timer.
 */
public final class BinWindowAggregator {

    private final Duration windowSize;
    private final Consumer<WindowSummary> onWindowClose;
    private final Map<String, Accumulator> openWindows = new HashMap<>();

    public BinWindowAggregator(Duration windowSize, Consumer<WindowSummary> onWindowClose) {
        this.windowSize = windowSize;
        this.onWindowClose = onWindowClose;
    }

    /** Records one event, timing its own accumulation cost for the per-BIN latency percentiles. */
    public void record(AuthEvent event) {
        long start = System.nanoTime();
        Instant windowStart = floorToWindow(event.occurredAt());

        Accumulator acc = openWindows.get(event.issuerBin());
        if (acc != null && !acc.windowStart.equals(windowStart)) {
            emit(acc);
            acc = null;
        }
        if (acc == null) {
            acc = new Accumulator(event.issuerBin(), windowStart);
            openWindows.put(event.issuerBin(), acc);
        }

        switch (event.kind()) {
            case AUTHORIZATION -> {
                acc.totalAuthorizations++;
                if (event.approved()) {
                    acc.approvedCount++;
                } else {
                    acc.declinedCount++;
                    acc.declineCodeCounts.merge(event.declineCode(), 1, Integer::sum);
                }
            }
            case REVERSAL -> acc.reversalCount++;
            case CLEARING -> acc.clearingCount++;
        }

        long elapsed = System.nanoTime() - start;
        acc.latenciesNanos.add(elapsed);
    }

    /** Closes every still-open window (end of stream); must be called once, after the last event. */
    public void flush() {
        for (Accumulator acc : new ArrayList<>(openWindows.values())) {
            emit(acc);
        }
        openWindows.clear();
    }

    private void emit(Accumulator acc) {
        List<Long> sorted = new ArrayList<>(acc.latenciesNanos);
        sorted.sort(null);
        long p50 = percentile(sorted, 0.50);
        long p99 = percentile(sorted, 0.99);

        onWindowClose.accept(new WindowSummary(
                acc.issuerBin,
                acc.windowStart,
                acc.windowStart.plus(windowSize),
                acc.totalAuthorizations,
                acc.approvedCount,
                acc.declinedCount,
                new EnumMap<>(acc.declineCodeCounts),
                acc.reversalCount,
                acc.clearingCount,
                p50,
                p99
        ));
        openWindows.remove(acc.issuerBin);
    }

    private Instant floorToWindow(Instant instant) {
        long windowSeconds = windowSize.getSeconds();
        long epochSeconds = instant.getEpochSecond();
        long flooredSeconds = (epochSeconds / windowSeconds) * windowSeconds;
        return Instant.ofEpochSecond(flooredSeconds);
    }

    private static long percentile(List<Long> sortedNanos, double p) {
        if (sortedNanos.isEmpty()) return 0L;
        int index = (int) Math.ceil(p * sortedNanos.size()) - 1;
        return sortedNanos.get(Math.max(0, Math.min(index, sortedNanos.size() - 1)));
    }

    private static final class Accumulator {
        final String issuerBin;
        final Instant windowStart;
        int totalAuthorizations;
        int approvedCount;
        int declinedCount;
        int reversalCount;
        int clearingCount;
        final Map<DeclineCode, Integer> declineCodeCounts = new EnumMap<>(DeclineCode.class);
        final List<Long> latenciesNanos = new ArrayList<>();

        Accumulator(String issuerBin, Instant windowStart) {
            this.issuerBin = issuerBin;
            this.windowStart = windowStart;
        }
    }
}
