package com.manas.cardauth.health;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BinWindowAggregatorTest {

    private static AuthEvent auth(String bin, boolean approved, DeclineCode code, Instant at, long seq) {
        return new AuthEvent("EVT-" + seq, bin, AuthEventKind.AUTHORIZATION, approved, code, at, seq);
    }

    @Test
    void closesAWindowWhenALaterEventForTheSameBinCrossesTheBoundary() {
        List<WindowSummary> closed = new ArrayList<>();
        BinWindowAggregator agg = new BinWindowAggregator(Duration.ofSeconds(10), closed::add);

        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        agg.record(auth("BIN-000001", true, null, base, 0));
        agg.record(auth("BIN-000001", false, DeclineCode.DO_NOT_HONOR, base.plusSeconds(1), 1));
        assertTrue(closed.isEmpty(), "window should still be open");

        agg.record(auth("BIN-000001", true, null, base.plusSeconds(10), 2));

        assertEquals(1, closed.size());
        WindowSummary first = closed.get(0);
        assertEquals(2, first.totalAuthorizations());
        assertEquals(1, first.approvedCount());
        assertEquals(1, first.declinedCount());
        assertEquals(DeclineCode.DO_NOT_HONOR, first.dominantDeclineCode());
    }

    @Test
    void tracksEachBinInAnIndependentWindow() {
        List<WindowSummary> closed = new ArrayList<>();
        BinWindowAggregator agg = new BinWindowAggregator(Duration.ofSeconds(10), closed::add);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        agg.record(auth("BIN-000001", true, null, base, 0));
        agg.record(auth("BIN-000002", true, null, base, 1));
        // Crossing BIN-000001's window boundary closes its first window immediately,
        // independently of BIN-000002, which is still mid-window.
        agg.record(auth("BIN-000001", true, null, base.plusSeconds(10), 2));
        agg.flush();

        // BIN-000001's first window (closed on the boundary crossing), BIN-000001's
        // second window and BIN-000002's only window (both closed by flush).
        assertEquals(3, closed.size());
        assertEquals(1, closed.stream().filter(w -> w.issuerBin().equals("BIN-000002")).count());
        assertEquals(2, closed.stream().filter(w -> w.issuerBin().equals("BIN-000001")).count());
    }

    @Test
    void flushEmitsEveryStillOpenWindowExactlyOnce() {
        List<WindowSummary> closed = new ArrayList<>();
        BinWindowAggregator agg = new BinWindowAggregator(Duration.ofSeconds(10), closed::add);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        agg.record(auth("BIN-000001", true, null, base, 0));
        agg.flush();
        agg.flush();

        assertEquals(1, closed.size(), "a second flush with no new events must not re-emit");
    }

    @Test
    void p99LatencyIsNeverSmallerThanP50() {
        List<WindowSummary> closed = new ArrayList<>();
        BinWindowAggregator agg = new BinWindowAggregator(Duration.ofSeconds(10), closed::add);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        for (int i = 0; i < 200; i++) {
            agg.record(auth("BIN-000001", true, null, base.plusSeconds(i / 25), i));
        }
        agg.flush();

        assertFalse(closed.isEmpty());
        for (WindowSummary w : closed) {
            assertTrue(w.decisionLatencyP99Nanos() >= w.decisionLatencyP50Nanos());
        }
    }
}
