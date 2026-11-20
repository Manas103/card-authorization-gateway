package com.manas.cardauth.analytics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes the generated authorisation stream to CSV so the Python/DuckDB
 * decline-decomposition and peer-benchmark layer (analytics/decline_analytics/)
 * can read it without a running Kafka broker or database, the same
 * no-broker-needed convention the rest of this repository's measured
 * runs already follow.
 */
public final class CsvAuthorizationExporter {

    public void writeCsv(List<AuthorizationRecord> records, Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write("event_id,issuer_bin,peer_cohort,channel,amount_eur,decline_family,approved");
            writer.newLine();
            for (AuthorizationRecord r : records) {
                writer.write(String.valueOf(r.eventId()));
                writer.write(',');
                writer.write(r.issuerBin());
                writer.write(',');
                writer.write(r.peerCohort());
                writer.write(',');
                writer.write(r.channel());
                writer.write(',');
                writer.write(String.valueOf(r.amountEur()));
                writer.write(',');
                writer.write(r.declineFamily() == null ? "" : r.declineFamily().name());
                writer.write(',');
                writer.write(String.valueOf(r.approved()));
                writer.newLine();
            }
        }
    }
}
