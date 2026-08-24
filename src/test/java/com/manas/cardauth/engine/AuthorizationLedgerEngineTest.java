package com.manas.cardauth.engine;

import com.manas.cardauth.model.CardMessage;
import com.manas.cardauth.model.LedgerEffect;
import com.manas.cardauth.model.MessageType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AuthorizationLedgerEngine}: holding, releasing,
 * clearing, out-of-order arrival, and duplicate handling before and after
 * a transaction resolves. Mirrors the level of scrutiny
 * securities-settlement-matching's MatchingEngineTest applies to its
 * engine, adapted to a hold-then-release balance model instead of a
 * two-sided match.
 */
class AuthorizationLedgerEngineTest {

    private static final String ACCT = "ACCT-000001";

    private AuthorizationLedgerEngine newEngine(BigDecimal opening) {
        Map<String, BigDecimal> balances = new LinkedHashMap<>();
        balances.put(ACCT, opening);
        return new AuthorizationLedgerEngine(balances);
    }

    private CardMessage auth(String messageId, String trace, BigDecimal amount) {
        return new CardMessage(messageId, trace, MessageType.AUTHORIZATION, ACCT, amount, "MERCH-01", Instant.now(), 0);
    }

    private CardMessage terminal(String messageId, String trace, MessageType type, BigDecimal amount) {
        return new CardMessage(messageId, trace, type, ACCT, amount, "MERCH-01", Instant.now(), 1);
    }

    @Test
    void authorizationHoldsFundsAndClearingReleasesToPosted() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("1000.00"));
        BigDecimal amount = new BigDecimal("100.00");

        LedgerOutcome held = engine.ingest(auth("A1", "T1", amount));
        assertThat(held.type()).isEqualTo(LedgerOutcome.Type.HELD);
        BalanceSnapshot afterHold = engine.balanceSnapshotFor(ACCT);
        assertThat(afterHold.availableBalance()).isEqualByComparingTo("900.00");
        assertThat(afterHold.heldAmount()).isEqualByComparingTo("100.00");
        assertThat(afterHold.postedBalance()).isEqualByComparingTo("0.00");

        LedgerOutcome cleared = engine.ingest(terminal("C1", "T1", MessageType.CLEARING, amount));
        assertThat(cleared.type()).isEqualTo(LedgerOutcome.Type.CLEARED);
        assertThat(cleared.resolution().effect()).isEqualTo(LedgerEffect.CLEARED);
        BalanceSnapshot afterClear = engine.balanceSnapshotFor(ACCT);
        assertThat(afterClear.availableBalance()).isEqualByComparingTo("900.00");
        assertThat(afterClear.heldAmount()).isEqualByComparingTo("0.00");
        assertThat(afterClear.postedBalance()).isEqualByComparingTo("100.00");
    }

    @Test
    void authorizationHoldsFundsAndReversalReturnsThemToAvailable() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("1000.00"));
        BigDecimal amount = new BigDecimal("250.00");

        engine.ingest(auth("A1", "T1", amount));
        LedgerOutcome released = engine.ingest(terminal("R1", "T1", MessageType.REVERSAL, amount));

        assertThat(released.type()).isEqualTo(LedgerOutcome.Type.RELEASED);
        BalanceSnapshot after = engine.balanceSnapshotFor(ACCT);
        assertThat(after.availableBalance()).isEqualByComparingTo("1000.00");
        assertThat(after.heldAmount()).isEqualByComparingTo("0.00");
        assertThat(after.postedBalance()).isEqualByComparingTo("0.00");
    }

    @Test
    void terminalArrivingBeforeAuthorizationIsBufferedThenAppliedOnceAuthorized() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("1000.00"));
        BigDecimal amount = new BigDecimal("50.00");

        LedgerOutcome bufferedOutcome = engine.ingest(terminal("C1", "T1", MessageType.CLEARING, amount));
        assertThat(bufferedOutcome.type()).isEqualTo(LedgerOutcome.Type.BUFFERED);
        assertThat(engine.balanceSnapshotFor(ACCT).heldAmount()).isEqualByComparingTo("0.00");

        LedgerOutcome resolvedOnAuth = engine.ingest(auth("A1", "T1", amount));
        assertThat(resolvedOnAuth.type()).isEqualTo(LedgerOutcome.Type.CLEARED);
        BalanceSnapshot after = engine.balanceSnapshotFor(ACCT);
        assertThat(after.heldAmount()).isEqualByComparingTo("0.00");
        assertThat(after.postedBalance()).isEqualByComparingTo("50.00");
        assertThat(engine.stillHeldTraceNumbers()).doesNotContain("T1");
    }

    @Test
    void duplicateMessageBeforeResolutionIsANoOp() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("1000.00"));
        BigDecimal amount = new BigDecimal("75.00");

        engine.ingest(auth("A1", "T1", amount));
        LedgerOutcome duplicateAuth = engine.ingest(auth("A1", "T1", amount));
        assertThat(duplicateAuth.type()).isEqualTo(LedgerOutcome.Type.DUPLICATE);
        assertThat(engine.balanceSnapshotFor(ACCT).heldAmount()).isEqualByComparingTo("75.00");
    }

    @Test
    void duplicateTerminalAfterResolutionCannotDoubleRelease() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("1000.00"));
        BigDecimal amount = new BigDecimal("75.00");

        engine.ingest(auth("A1", "T1", amount));
        engine.ingest(terminal("R1", "T1", MessageType.REVERSAL, amount));
        BalanceSnapshot afterFirstRelease = engine.balanceSnapshotFor(ACCT);

        LedgerOutcome redelivered = engine.ingest(terminal("R1", "T1", MessageType.REVERSAL, amount));
        assertThat(redelivered.type()).isEqualTo(LedgerOutcome.Type.DUPLICATE);
        assertThat(engine.balanceSnapshotFor(ACCT).availableBalance()).isEqualByComparingTo(afterFirstRelease.availableBalance());
        assertThat(engine.resolutions()).hasSize(1);
    }

    @Test
    void aSecondDistinctTerminalMessageForAnAlreadyResolvedTraceIsGuardedInert() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("1000.00"));
        BigDecimal amount = new BigDecimal("75.00");

        engine.ingest(auth("A1", "T1", amount));
        engine.ingest(terminal("C1", "T1", MessageType.CLEARING, amount));
        // A different messageId for the same, already-cleared traceNumber: not a valid
        // redelivery (a redelivery would share the messageId), but must never double-clear.
        LedgerOutcome anomalous = engine.ingest(terminal("C2", "T1", MessageType.CLEARING, amount));

        assertThat(anomalous.type()).isEqualTo(LedgerOutcome.Type.DUPLICATE);
        assertThat(engine.resolutions()).hasSize(1);
        assertThat(engine.balanceSnapshotFor(ACCT).postedBalance()).isEqualByComparingTo("75.00");
    }

    @Test
    void anAuthorizationWithNoTerminalStaysHeld() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("1000.00"));
        engine.ingest(auth("A1", "T1", new BigDecimal("40.00")));

        assertThat(engine.stillHeldTraceNumbers()).containsExactly("T1");
        assertThat(engine.heldAmountFor("T1")).isEqualByComparingTo("40.00");
        assertThat(engine.resolutions()).isEmpty();
    }

    @Test
    void conservationIdentityHoldsAcrossHoldAndClear() {
        AuthorizationLedgerEngine engine = newEngine(new BigDecimal("500.00"));
        engine.ingest(auth("A1", "T1", new BigDecimal("120.00")));
        engine.ingest(terminal("C1", "T1", MessageType.CLEARING, new BigDecimal("120.00")));

        BalanceSnapshot snapshot = engine.balanceSnapshotFor(ACCT);
        BigDecimal total = snapshot.availableBalance().add(snapshot.heldAmount()).add(snapshot.postedBalance());
        assertThat(total).isEqualByComparingTo(engine.openingBalanceFor(ACCT));
    }
}
