package com.manas.cardauth.generator;

import com.manas.cardauth.model.CardMessage;
import com.manas.cardauth.model.MessageType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Deterministic, seeded synthetic card-transaction traffic. Every number
 * in this project's README traces back to a run of this generator; given
 * the same seed and transaction count it always produces the same stream,
 * which is what makes the measurements reproducible rather than
 * anecdotal.
 */
public final class SyntheticMessageGenerator {

    private static final String[] MERCHANTS = {
            "MERCH-GROCERY-01", "MERCH-FUEL-02", "MERCH-ONLINE-03", "MERCH-DINING-04",
            "MERCH-TRAVEL-05", "MERCH-RETAIL-06", "MERCH-PHARMACY-07"
    };

    public record GeneratedStream(
            List<CardMessage> messages,
            List<String> inFlightTraceNumbers,
            Map<String, BigDecimal> openingBalances
    ) {
    }

    /**
     * Generates {@code transactionCount} card transactions across
     * {@code accountCount} accounts. Every transaction gets exactly one
     * AUTHORIZATION message; {@code inFlightCount} of them (evenly spread
     * across the run) are left open with no terminal event, the rest each
     * get exactly one terminal message, REVERSAL roughly
     * {@code reversalFraction} of the time and CLEARING otherwise. Every
     * account is seeded with a large enough opening balance that no hold
     * in this synthetic stream can ever exceed it (insufficient-funds
     * declines are out of scope for this project; every AUTHORIZATION
     * here succeeds).
     */
    public GeneratedStream generate(int transactionCount, int accountCount, int inFlightCount,
                                     double reversalFraction, long seed) {
        if (inFlightCount > transactionCount) {
            throw new IllegalArgumentException("cannot leave more transactions in-flight than exist");
        }
        Random random = new Random(seed);

        Map<String, BigDecimal> openingBalances = new LinkedHashMap<>();
        for (int a = 0; a < accountCount; a++) {
            openingBalances.put("ACCT-%06d".formatted(a), new BigDecimal("1000000.00"));
        }
        String[] accountIds = openingBalances.keySet().toArray(new String[0]);

        // Choose which transaction indices are left in-flight (authorization only) by evenly
        // spacing them across the run, rather than clustering them, so the stream looks like
        // real traffic with some transactions still open at the observation cutoff.
        boolean[] isInFlight = new boolean[transactionCount];
        if (inFlightCount > 0) {
            int stride = transactionCount / inFlightCount;
            for (int i = 0; i < inFlightCount; i++) {
                isInFlight[Math.min(i * stride, transactionCount - 1)] = true;
            }
        }

        List<CardMessage> out = new ArrayList<>(transactionCount * 2);
        List<String> inFlightTraceNumbers = new ArrayList<>(inFlightCount);

        for (int i = 0; i < transactionCount; i++) {
            String traceNumber = "TRC-%08d".formatted(i);
            String accountId = accountIds[random.nextInt(accountIds.length)];
            BigDecimal amount = BigDecimal.valueOf(5 + random.nextDouble() * 495).setScale(2, RoundingMode.HALF_UP);
            String merchantId = MERCHANTS[random.nextInt(MERCHANTS.length)];
            Instant authorizedAt = Instant.parse("2026-07-01T00:00:00Z").plusSeconds(i * 3L);

            out.add(new CardMessage("MSG-AUTH-%08d".formatted(i), traceNumber, MessageType.AUTHORIZATION,
                    accountId, amount, merchantId, authorizedAt, i * 2L));

            if (isInFlight[i]) {
                inFlightTraceNumbers.add(traceNumber);
                continue;
            }

            MessageType terminalType = random.nextDouble() < reversalFraction ? MessageType.REVERSAL : MessageType.CLEARING;
            Instant terminalAt = authorizedAt.plusSeconds(30 + random.nextInt(600));
            out.add(new CardMessage("MSG-TERM-%08d".formatted(i), traceNumber, terminalType,
                    accountId, amount, merchantId, terminalAt, i * 2L + 1));
        }

        return new GeneratedStream(out, inFlightTraceNumbers, openingBalances);
    }
}
