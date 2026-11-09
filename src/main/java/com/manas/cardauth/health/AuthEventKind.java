package com.manas.cardauth.health;

/**
 * The three message kinds the health monitor consumes off the same
 * conceptual card authorization, reversal and clearing Kafka stream the
 * ledger engine does (claim: "consumes a card authorization, reversal and
 * clearing stream off Kafka"). Only AUTHORIZATION carries an approve/decline
 * outcome; REVERSAL and CLEARING are counted toward stream volume but excluded
 * from the approval-rate and decline-code-mix math, which is specifically an
 * authorization-decision metric.
 */
public enum AuthEventKind {
    AUTHORIZATION,
    REVERSAL,
    CLEARING
}
