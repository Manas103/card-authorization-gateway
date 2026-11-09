package com.manas.cardauth.health;

/**
 * Issuer decline reasons tracked by the health monitor's decline-code mix
 * (claim 1). Not exhaustive of real card-network reason codes; five common
 * ones chosen to give the incident detector's "dominant decline code"
 * something real to disagree over during a crash.
 */
public enum DeclineCode {
    INSUFFICIENT_FUNDS,
    DO_NOT_HONOR,
    SUSPECTED_FRAUD,
    INVALID_PIN,
    EXPIRED_CARD
}
