package com.manas.cardauth.health;

import java.time.Instant;

/**
 * One raised incident: an issuer BIN whose rolling window's approval rate
 * crossed below the floor while the minimum-volume guard was met (claim 3).
 * {@code detectedAt} is the window's own close time (synthetic event time),
 * which is the earliest moment the detector has a complete window to decide
 * from; "named within 30 seconds of onset" (claim 5) is measured as the gap
 * between this and the injected episode's own onset time, not wall-clock
 * processing time.
 */
public record Incident(
        long id,
        String issuerBin,
        Instant windowStart,
        Instant windowEnd,
        Instant detectedAt,
        double approvalRateObserved,
        int windowVolume,
        DeclineCode dominantDeclineCode
) {
}
