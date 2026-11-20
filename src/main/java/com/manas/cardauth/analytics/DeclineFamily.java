package com.manas.cardauth.analytics;

/**
 * Six decline-reason families for the authorisation performance review
 * (claim: "decomposed into 6 decline-reason families"). Deliberately a
 * separate taxonomy from {@code health.DeclineCode}: that enum exists so
 * the health monitor's incident detector has something real to disagree
 * over during a crash episode, this one exists so a decline can be
 * classified as recoverable or not, which is a different question and is
 * answered in the Python analytics layer (see {@code analytics/decline_analytics/recoverable.py}),
 * not baked into this enum.
 */
public enum DeclineFamily {
    INSUFFICIENT_FUNDS,
    SUSPECTED_FRAUD,
    CARD_DATA_ISSUE,
    DO_NOT_HONOR,
    ISSUER_UNAVAILABLE,
    CVV_AVS_MISMATCH
}
