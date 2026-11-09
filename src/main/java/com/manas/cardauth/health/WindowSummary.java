package com.manas.cardauth.health;

import java.time.Instant;
import java.util.Map;

/**
 * One closed rolling window for one issuer BIN: the approval rate,
 * decline-code mix, and decision-latency percentiles the health monitor
 * tracks per BIN (claim 1). {@code decisionLatencyP50Nanos} and
 * {@code decisionLatencyP99Nanos} measure {@link BinWindowAggregator#record}'s
 * own in-process cost, the same "engine-only" latency convention this
 * repository's ledger engine benchmark already uses, not end-to-end Kafka
 * latency.
 */
public record WindowSummary(
        String issuerBin,
        Instant windowStart,
        Instant windowEnd,
        int totalAuthorizations,
        int approvedCount,
        int declinedCount,
        Map<DeclineCode, Integer> declineCodeCounts,
        int reversalCount,
        int clearingCount,
        long decisionLatencyP50Nanos,
        long decisionLatencyP99Nanos
) {

    public double approvalRate() {
        return totalAuthorizations == 0 ? 1.0 : approvedCount / (double) totalAuthorizations;
    }

    /** The single most frequent decline code in this window, or null if none declined. */
    public DeclineCode dominantDeclineCode() {
        DeclineCode dominant = null;
        int max = 0;
        for (Map.Entry<DeclineCode, Integer> e : declineCodeCounts.entrySet()) {
            if (e.getValue() > max) {
                max = e.getValue();
                dominant = e.getKey();
            }
        }
        return dominant;
    }
}
