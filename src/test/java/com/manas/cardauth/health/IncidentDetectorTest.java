package com.manas.cardauth.health;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IncidentDetectorTest {

    private static WindowSummary summary(String bin, int total, int approved, Map<DeclineCode, Integer> codes) {
        return new WindowSummary(bin, Instant.EPOCH, Instant.EPOCH.plusSeconds(10),
                total, approved, total - approved, codes, 0, 0, 100, 200);
    }

    @Test
    void doesNotRaiseAnIncidentWhenTheMinimumVolumeGuardIsNotMet() {
        IncidentDetector detector = new IncidentDetector(50, 0.80);
        Map<DeclineCode, Integer> codes = new EnumMap<>(DeclineCode.class);
        codes.put(DeclineCode.DO_NOT_HONOR, 10);
        // 10 total, all declined: a 0% approval rate, but below the 50-event guard.
        detector.onWindowClose(summary("BIN-000001", 10, 0, codes));

        assertTrue(detector.incidents().isEmpty(), "a low-volume window must never page regardless of its rate");
    }

    @Test
    void raisesAnIncidentWhenVolumeIsMetAndApprovalRateCrossesTheFloor() {
        IncidentDetector detector = new IncidentDetector(50, 0.80);
        Map<DeclineCode, Integer> codes = new EnumMap<>(DeclineCode.class);
        codes.put(DeclineCode.SUSPECTED_FRAUD, 70);
        codes.put(DeclineCode.INVALID_PIN, 10);
        detector.onWindowClose(summary("BIN-000002", 100, 20, codes));

        assertEquals(1, detector.incidents().size());
        Incident incident = detector.incidents().get(0);
        assertEquals("BIN-000002", incident.issuerBin());
        assertEquals(DeclineCode.SUSPECTED_FRAUD, incident.dominantDeclineCode());
        assertEquals(0.20, incident.approvalRateObserved(), 1e-9);
    }

    @Test
    void doesNotRaiseAnIncidentWhenApprovalRateIsAtOrAboveTheFloor() {
        IncidentDetector detector = new IncidentDetector(50, 0.80);
        Map<DeclineCode, Integer> codes = new EnumMap<>(DeclineCode.class);
        codes.put(DeclineCode.EXPIRED_CARD, 20);
        detector.onWindowClose(summary("BIN-000003", 100, 80, codes));

        assertTrue(detector.incidents().isEmpty(), "exactly-at-floor approval rate must not page");
    }

    @Test
    void assignsIncreasingIdsAcrossMultipleIncidents() {
        IncidentDetector detector = new IncidentDetector(50, 0.80);
        Map<DeclineCode, Integer> codes = new EnumMap<>(DeclineCode.class);
        codes.put(DeclineCode.DO_NOT_HONOR, 90);
        detector.onWindowClose(summary("BIN-000004", 100, 10, codes));
        detector.onWindowClose(summary("BIN-000005", 100, 10, codes));

        assertEquals(2, detector.incidents().size());
        assertTrue(detector.incidents().get(1).id() > detector.incidents().get(0).id());
    }
}
