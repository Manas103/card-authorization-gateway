package com.manas.cardauth.analytics;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Deterministic, seeded synthetic authorisation stream across many
 * issuers grouped into three peer cohorts with different baseline
 * decline rates (the substrate claim 3, "each issuer benchmarked
 * against a peer cohort", needs real cohort-level variation to
 * benchmark against), and two channels (card-present / card-not-present)
 * with different decline-family mixes, since card-not-present traffic
 * is where fraud and CVV/AVS declines concentrate in real issuer data.
 * Mirrors {@code HealthStreamGenerator}'s seeded-and-reproducible
 * convention.
 */
public final class AuthorizationDatasetGenerator {

    public static final String CNP = "CNP";
    public static final String CP = "CP";

    private record CohortSpec(String name, double declineRate) {
    }

    private static final CohortSpec[] COHORTS = {
            new CohortSpec("TIER_A", 0.04),
            new CohortSpec("TIER_B", 0.08),
            new CohortSpec("TIER_C", 0.14),
    };

    // Decline-family mix, by channel. Must each sum to 1.0. LinkedHashMap, not
    // Map.of, because pickFamily walks these in iteration order to turn a
    // cumulative draw into a family, and that order must be fixed and
    // reproducible rather than left to Map.of's unspecified ordering.
    private static final Map<DeclineFamily, Double> CNP_FAMILY_WEIGHTS = orderedWeights(
            DeclineFamily.SUSPECTED_FRAUD, 0.28,
            DeclineFamily.CVV_AVS_MISMATCH, 0.22,
            DeclineFamily.CARD_DATA_ISSUE, 0.18,
            DeclineFamily.INSUFFICIENT_FUNDS, 0.15,
            DeclineFamily.DO_NOT_HONOR, 0.12,
            DeclineFamily.ISSUER_UNAVAILABLE, 0.05
    );
    private static final Map<DeclineFamily, Double> CP_FAMILY_WEIGHTS = orderedWeights(
            DeclineFamily.INSUFFICIENT_FUNDS, 0.30,
            DeclineFamily.DO_NOT_HONOR, 0.22,
            DeclineFamily.CARD_DATA_ISSUE, 0.20,
            DeclineFamily.SUSPECTED_FRAUD, 0.12,
            DeclineFamily.ISSUER_UNAVAILABLE, 0.11,
            DeclineFamily.CVV_AVS_MISMATCH, 0.05
    );

    private static Map<DeclineFamily, Double> orderedWeights(Object... pairs) {
        Map<DeclineFamily, Double> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((DeclineFamily) pairs[i], (Double) pairs[i + 1]);
        }
        return map;
    }

    private static final double CNP_SHARE = 0.55;

    public List<AuthorizationRecord> generate(int issuersPerCohort, int authsPerIssuer, long seed) {
        Random random = new Random(seed);
        List<AuthorizationRecord> out = new ArrayList<>(issuersPerCohort * COHORTS.length * authsPerIssuer);

        long eventId = 0;
        for (CohortSpec cohort : COHORTS) {
            for (int i = 0; i < issuersPerCohort; i++) {
                String issuerBin = "BIN-%s-%04d".formatted(cohort.name(), i);
                for (int t = 0; t < authsPerIssuer; t++) {
                    boolean isCnp = random.nextDouble() < CNP_SHARE;
                    String channel = isCnp ? CNP : CP;
                    double amountEur = isCnp
                            ? 5 + random.nextDouble() * 145   // CNP tickets skew smaller (online retail)
                            : 10 + random.nextDouble() * 290; // CP tickets skew larger
                    amountEur = BigDecimal.valueOf(amountEur).setScale(2, RoundingMode.HALF_UP).doubleValue();

                    boolean approved = random.nextDouble() >= cohort.declineRate();
                    DeclineFamily family = approved ? null
                            : pickFamily(isCnp ? CNP_FAMILY_WEIGHTS : CP_FAMILY_WEIGHTS, random.nextDouble());

                    out.add(new AuthorizationRecord(eventId, issuerBin, cohort.name(), channel, amountEur, family));
                    eventId++;
                }
            }
        }
        return out;
    }

    private static DeclineFamily pickFamily(Map<DeclineFamily, Double> weights, double draw) {
        double cumulative = 0.0;
        for (Map.Entry<DeclineFamily, Double> entry : weights.entrySet()) {
            cumulative += entry.getValue();
            if (draw < cumulative) {
                return entry.getKey();
            }
        }
        // Falls through only on floating-point rounding at the very top of the range.
        return weights.keySet().iterator().next();
    }
}
