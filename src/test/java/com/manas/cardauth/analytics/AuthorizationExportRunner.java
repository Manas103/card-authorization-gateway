package com.manas.cardauth.analytics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real measurement run behind the authorisation-performance-review
 * claims: 1.2M simulated authorisations, decomposed into 6 decline-reason
 * families and across 3 peer cohorts. Not part of the default
 * {@code mvn test} run (tagged {@code benchmark}, same convention as
 * {@code BenchmarkRunner} and {@code HealthBenchmarkRunner}); run
 * explicitly with
 * {@code mvn test -Dtest=AuthorizationExportRunner -Dsurefire.excludedGroups=}.
 *
 * <p>Writes the generated stream to {@code analytics/data/authorizations.csv}
 * (repo-relative, gitignored), which is what the Python/DuckDB decline-
 * decomposition and peer-benchmark layer under {@code analytics/} reads;
 * see that layer's own README section and {@code docs/analytics_benchmark_output.txt}
 * for the recoverable-CNP-decline and revenue-sizing claims this dataset
 * feeds.
 */
class AuthorizationExportRunner {

    private static final int ISSUERS_PER_COHORT = 10;
    private static final int AUTHS_PER_ISSUER = 40_000; // 10 * 3 cohorts * 40,000 = 1,200,000
    private static final long SEED = 20261115L;

    @Test
    @Tag("benchmark")
    void generateAndExportTheAuthorisationStream() throws IOException {
        StringBuilder report = new StringBuilder();
        line(report, "=== Authorisation Performance Review -- dataset export ===");
        line(report, "started: " + Instant.now());
        line(report, "issuers per cohort: " + ISSUERS_PER_COHORT + ", authorisations per issuer: " + AUTHS_PER_ISSUER
                + ", seed: " + SEED);

        List<AuthorizationRecord> records = new AuthorizationDatasetGenerator()
                .generate(ISSUERS_PER_COHORT, AUTHS_PER_ISSUER, SEED);

        line(report, "");
        line(report, "total records generated: " + records.size());

        long approved = records.stream().filter(AuthorizationRecord::approved).count();
        long declined = records.size() - approved;
        line(report, "approved: " + approved + ", declined: " + declined
                + " (" + String.format("%.4f", declined / (double) records.size()) + ")");

        Map<DeclineFamily, Long> byFamily = new EnumMap<>(DeclineFamily.class);
        for (AuthorizationRecord r : records) {
            if (!r.approved()) {
                byFamily.merge(r.declineFamily(), 1L, Long::sum);
            }
        }
        line(report, "");
        line(report, "-- decline-reason family mix (of " + declined + " declines) --");
        for (Map.Entry<DeclineFamily, Long> e : byFamily.entrySet()) {
            line(report, e.getKey() + ": " + e.getValue()
                    + " (" + String.format("%.4f", e.getValue() / (double) declined) + ")");
        }

        Path csvPath = Path.of("analytics", "data", "authorizations.csv");
        new CsvAuthorizationExporter().writeCsv(records, csvPath);
        line(report, "");
        line(report, "CSV written to " + csvPath.toAbsolutePath() + " (" + records.size() + " rows)");
        line(report, "finished: " + Instant.now());

        System.out.println(report);

        assertThat(records).hasSize(ISSUERS_PER_COHORT * 3 * AUTHS_PER_ISSUER);
        assertThat(byFamily.keySet()).hasSize(DeclineFamily.values().length);
    }

    private void line(StringBuilder sb, String text) {
        sb.append(text).append(System.lineSeparator());
    }
}
