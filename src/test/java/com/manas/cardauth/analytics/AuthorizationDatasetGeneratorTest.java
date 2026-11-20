package com.manas.cardauth.analytics;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class AuthorizationDatasetGeneratorTest {

    private static final long SEED = 20261115L;

    @Test
    void generatesExactRecordCount() {
        List<AuthorizationRecord> records = new AuthorizationDatasetGenerator().generate(10, 1_000, SEED);
        assertThat(records).hasSize(3 * 10 * 1_000);
    }

    @Test
    void isDeterministicForAFixedSeed() {
        List<AuthorizationRecord> first = new AuthorizationDatasetGenerator().generate(5, 500, SEED);
        List<AuthorizationRecord> second = new AuthorizationDatasetGenerator().generate(5, 500, SEED);
        assertThat(first).isEqualTo(second);
    }

    @Test
    void everyRecordHasAValidChannelAndPositiveAmount() {
        List<AuthorizationRecord> records = new AuthorizationDatasetGenerator().generate(5, 2_000, SEED);
        Set<String> channels = records.stream().map(AuthorizationRecord::channel).collect(Collectors.toSet());
        assertThat(channels).containsExactlyInAnyOrder(AuthorizationDatasetGenerator.CNP, AuthorizationDatasetGenerator.CP);
        assertThat(records).allSatisfy(r -> assertThat(r.amountEur()).isGreaterThan(0.0));
    }

    @Test
    void approvedRecordsHaveNoDeclineFamilyAndDeclinedRecordsHaveOneOfTheSix() {
        List<AuthorizationRecord> records = new AuthorizationDatasetGenerator().generate(10, 5_000, SEED);
        assertThat(records).allSatisfy(r -> {
            if (r.approved()) {
                assertThat(r.declineFamily()).isNull();
            } else {
                assertThat(r.declineFamily()).isIn(EnumSet.allOf(DeclineFamily.class));
            }
        });
    }

    @Test
    void allSixDeclineFamiliesAppearOverALargeEnoughSample() {
        List<AuthorizationRecord> records = new AuthorizationDatasetGenerator().generate(10, 5_000, SEED);
        Set<DeclineFamily> seen = records.stream()
                .filter(r -> !r.approved())
                .map(AuthorizationRecord::declineFamily)
                .collect(Collectors.toSet());
        assertThat(seen).isEqualTo(EnumSet.allOf(DeclineFamily.class));
    }

    @Test
    void tierCPeerCohortDeclinesMoreOftenThanTierA() {
        List<AuthorizationRecord> records = new AuthorizationDatasetGenerator().generate(10, 5_000, SEED);
        double tierARate = declineRate(records, "TIER_A");
        double tierCRate = declineRate(records, "TIER_C");
        assertThat(tierCRate).isGreaterThan(tierARate);
    }

    private double declineRate(List<AuthorizationRecord> records, String cohort) {
        List<AuthorizationRecord> cohortRecords = records.stream()
                .filter(r -> r.peerCohort().equals(cohort)).toList();
        long declined = cohortRecords.stream().filter(r -> !r.approved()).count();
        return declined / (double) cohortRecords.size();
    }
}
