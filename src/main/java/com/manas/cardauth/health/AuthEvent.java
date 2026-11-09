package com.manas.cardauth.health;

import java.time.Instant;

/**
 * One health-monitor-facing event off the authorization/reversal/clearing
 * stream: {@code issuerBin} is the 6-digit issuer BIN the per-BIN rolling
 * windows are keyed on. {@code approved} and {@code declineCode} are only
 * meaningful when {@code kind == AUTHORIZATION}; a reversal or clearing
 * event carries neither (approved is false, declineCode is null) because
 * they have no approve/decline outcome of their own.
 */
public record AuthEvent(
        String eventId,
        String issuerBin,
        AuthEventKind kind,
        boolean approved,
        DeclineCode declineCode,
        Instant occurredAt,
        long sequenceNumber
) {
}
