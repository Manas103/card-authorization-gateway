package com.manas.cardauth.analytics;

/**
 * One simulated authorisation decision: which issuer, which peer cohort
 * that issuer belongs to, the channel (card-present or card-not-present),
 * the amount, and the decline family (null if approved).
 */
public record AuthorizationRecord(
        long eventId,
        String issuerBin,
        String peerCohort,
        String channel,
        double amountEur,
        DeclineFamily declineFamily
) {
    public boolean approved() {
        return declineFamily == null;
    }
}
