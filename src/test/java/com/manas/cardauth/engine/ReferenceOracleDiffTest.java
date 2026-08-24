package com.manas.cardauth.engine;

import com.manas.cardauth.generator.NoisyStreamBuilder;
import com.manas.cardauth.generator.SyntheticMessageGenerator;
import com.manas.cardauth.model.CardMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Diffs the real, hash-indexed {@link AuthorizationLedgerEngine} against
 * the deliberately slow, deliberately unindexed {@link ReferenceOracleLedger}
 * over the same synthetic dataset, with the real engine fed a noisy
 * (duplicated, shuffled) delivery order. BUILDER.md section 2: "a
 * reference oracle where correctness is non-obvious".
 *
 * Kept to a few thousand messages, not the full 100,000, because the
 * oracle is deliberately O(n^2).
 */
class ReferenceOracleDiffTest {

    @Test
    void realEngineAgreesWithTheReferenceOracleOverNoisyDelivery() {
        SyntheticMessageGenerator generator = new SyntheticMessageGenerator();
        var stream = generator.generate(1500, 25, 60, 0.2, 42L);

        // The oracle judges the canonical, already-complete dataset directly.
        ReferenceOracleLedger oracle = new ReferenceOracleLedger();
        ReferenceOracleLedger.OracleResult oracleResult = oracle.evaluate(stream.messages(), stream.openingBalances());

        // The real engine consumes a noisy (5% duplicated, fully shuffled) delivery of the same dataset.
        List<CardMessage> noisy = NoisyStreamBuilder.buildNoisyStream(stream.messages(), 0.05, 99L);
        AuthorizationLedgerEngine engine = new AuthorizationLedgerEngine(stream.openingBalances());
        for (CardMessage message : noisy) {
            engine.ingest(message);
        }

        Set<String> oracleResolvedRefs = oracleResult.resolutions().stream().map(TransactionResolution::traceNumber).collect(Collectors.toSet());
        Set<String> engineResolvedRefs = engine.resolutions().stream().map(TransactionResolution::traceNumber).collect(Collectors.toSet());
        assertThat(engineResolvedRefs).isEqualTo(oracleResolvedRefs);

        for (TransactionResolution oracleRes : oracleResult.resolutions()) {
            TransactionResolution engineRes = engine.resolutions().stream()
                    .filter(r -> r.traceNumber().equals(oracleRes.traceNumber()))
                    .findFirst()
                    .orElseThrow();
            assertThat(engineRes.effect()).isEqualTo(oracleRes.effect());
            assertThat(engineRes.amount()).isEqualByComparingTo(oracleRes.amount());
            assertThat(engineRes.accountId()).isEqualTo(oracleRes.accountId());
        }

        assertThat(Set.copyOf(engine.stillHeldTraceNumbers())).isEqualTo(Set.copyOf(oracleResult.stillHeldTraceNumbers()));

        for (BalanceSnapshot oracleBalance : oracleResult.balances()) {
            BalanceSnapshot engineBalance = engine.balanceSnapshotFor(oracleBalance.accountId());
            assertThat(engineBalance.availableBalance()).isEqualByComparingTo(oracleBalance.availableBalance());
            assertThat(engineBalance.heldAmount()).isEqualByComparingTo(oracleBalance.heldAmount());
            assertThat(engineBalance.postedBalance()).isEqualByComparingTo(oracleBalance.postedBalance());
        }

        assertThat(oracleResult.resolutions()).hasSize(1500 - 60);
        assertThat(oracleResult.stillHeldTraceNumbers()).hasSize(60);
    }
}
