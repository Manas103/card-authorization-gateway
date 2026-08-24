package com.manas.cardauth.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One message off the card authorization Kafka topic: an authorization
 * hold, a reversal (void), or a clearing (settle). {@code traceNumber} is
 * the payments-vocabulary identifier for the underlying card transaction,
 * the ISO 8583 "System Trace Audit Number" analog: every message that
 * belongs to the same transaction (the AUTHORIZATION and its one terminal
 * event) carries the same traceNumber. {@code messageId} identifies this
 * specific physical message (a redelivery of the same message carries the
 * same messageId).
 *
 * <p>Exactly-once balance effects (claim 3) are keyed on the pair
 * {@code (messageId, traceNumber)}, not messageId alone. See
 * {@link com.manas.cardauth.engine.AuthorizationLedgerEngine} for why: a
 * message identifier by itself is not guaranteed globally unique across an
 * unbounded transaction stream (real card-network trace numbers are
 * cyclic, and a message id generated the same way inherits the same
 * cyclic risk), so it is scoped to the transaction it claims to belong to
 * before being trusted as a dedup key.
 */
public record CardMessage(
        String messageId,
        String traceNumber,
        MessageType type,
        String accountId,
        BigDecimal amount,
        String merchantId,
        Instant occurredAt,
        long sequenceNumber
) {
}
