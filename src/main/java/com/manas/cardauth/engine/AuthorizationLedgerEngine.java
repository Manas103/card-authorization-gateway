package com.manas.cardauth.engine;

import com.manas.cardauth.model.CardMessage;
import com.manas.cardauth.model.LedgerEffect;
import com.manas.cardauth.model.MessageType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The real engine: hash-indexed, holds funds on an AUTHORIZATION and
 * releases them on whichever terminal event (REVERSAL or CLEARING) closes
 * the transaction out, regardless of the physical arrival order of the
 * three message types (claims 1 and 2).
 *
 * <p><b>Why the dedup key is (messageId, traceNumber), not messageId
 * alone.</b> A message id by itself is only as unique as whatever
 * generated it; real card-network trace numbers are commonly a small
 * cyclic counter (ISO 8583's STAN wraps at 999999), and a naively
 * generated message id inherits the same cyclic risk over an unbounded
 * transaction stream. Scoping the key to the specific transaction the
 * message claims to belong to (traceNumber) before trusting it as a dedup
 * key means a message id collision between two genuinely different
 * transactions cannot masquerade as a redelivery of one message; only a
 * message carrying the exact same id for the exact same transaction is
 * ever treated as a duplicate. See {@code CardMessage}'s javadoc and the
 * README's "why the dedup key is a pair" note.
 *
 * <p><b>Why a terminal message arriving before its AUTHORIZATION is
 * buffered, not dropped.</b> Kafka guarantees ordering only within a
 * partition, and even there, nothing requires a producer to key
 * AUTHORIZATION and its terminal event onto the same partition or emit
 * them adjacently. A REVERSAL or CLEARING that names a traceNumber this
 * engine has not yet authorized is held in {@code pendingTerminal} and
 * applied the instant the matching AUTHORIZATION arrives, so the final
 * balance effect is identical no matter which of the two messages the
 * stream happens to deliver first.
 *
 * <p>Not thread-safe by contract with itself; the single {@code ingest}
 * entry point is synchronized so concurrent Kafka listener container
 * threads cannot interleave a check-then-act on a transaction's state or
 * an account's balance.
 */
public final class AuthorizationLedgerEngine {

    private static final class TransactionState {
        String accountId;
        BigDecimal holdAmount;
        boolean authorized;
        boolean resolved;
        CardMessage pendingTerminal;
    }

    private static final class MutableBalance {
        BigDecimal available;
        BigDecimal held = BigDecimal.ZERO.setScale(2);
        BigDecimal posted = BigDecimal.ZERO.setScale(2);

        MutableBalance(BigDecimal opening) {
            this.available = opening;
        }
    }

    private final Set<String> seenKeys = new HashSet<>();
    private final Map<String, TransactionState> transactions = new HashMap<>();
    private final Map<String, MutableBalance> balances;
    private final Map<String, BigDecimal> openingBalances;
    private final List<TransactionResolution> resolutions = new ArrayList<>();

    public AuthorizationLedgerEngine(Map<String, BigDecimal> openingBalances) {
        this.openingBalances = new LinkedHashMap<>(openingBalances);
        this.balances = new LinkedHashMap<>();
        for (Map.Entry<String, BigDecimal> entry : this.openingBalances.entrySet()) {
            balances.put(entry.getKey(), new MutableBalance(entry.getValue()));
        }
    }

    public synchronized LedgerOutcome ingest(CardMessage message) {
        String key = message.messageId() + "|" + message.traceNumber();
        if (!seenKeys.add(key)) {
            return LedgerOutcome.duplicate(message);
        }

        TransactionState state = transactions.computeIfAbsent(message.traceNumber(), t -> new TransactionState());

        if (message.type() == MessageType.AUTHORIZATION) {
            if (state.authorized) {
                // A second AUTHORIZATION for a traceNumber that already has one is a data
                // anomaly, not a valid redelivery (a true redelivery would have shared the
                // same messageId and been caught by the key check above). Guarded, inert.
                return LedgerOutcome.duplicate(message);
            }
            state.accountId = message.accountId();
            state.holdAmount = message.amount();
            state.authorized = true;
            applyHold(message.accountId(), message.amount());

            if (state.pendingTerminal != null) {
                CardMessage terminal = state.pendingTerminal;
                state.pendingTerminal = null;
                return resolve(state, terminal);
            }
            return LedgerOutcome.held(message.traceNumber(), message.accountId(), message.amount());
        }

        // REVERSAL or CLEARING
        if (state.resolved) {
            // No double release/clear: an unexpected second terminal event for an
            // already-resolved transaction never touches a balance.
            return LedgerOutcome.duplicate(message);
        }
        if (!state.authorized) {
            state.pendingTerminal = message;
            return LedgerOutcome.buffered(message);
        }
        return resolve(state, message);
    }

    private LedgerOutcome resolve(TransactionState state, CardMessage terminal) {
        state.resolved = true;
        BigDecimal amount = state.holdAmount;
        LedgerEffect effect;
        if (terminal.type() == MessageType.REVERSAL) {
            releaseHold(state.accountId, amount);
            effect = LedgerEffect.RELEASED;
        } else {
            clearHold(state.accountId, amount);
            effect = LedgerEffect.CLEARED;
        }
        TransactionResolution resolution = new TransactionResolution(
                terminal.traceNumber(), state.accountId, amount, effect, terminal.messageId());
        resolutions.add(resolution);
        return LedgerOutcome.resolved(effect == LedgerEffect.RELEASED ? LedgerOutcome.Type.RELEASED : LedgerOutcome.Type.CLEARED, resolution);
    }

    private void applyHold(String accountId, BigDecimal amount) {
        MutableBalance balance = balances.get(accountId);
        balance.available = balance.available.subtract(amount);
        balance.held = balance.held.add(amount);
    }

    private void releaseHold(String accountId, BigDecimal amount) {
        MutableBalance balance = balances.get(accountId);
        balance.held = balance.held.subtract(amount);
        balance.available = balance.available.add(amount);
    }

    private void clearHold(String accountId, BigDecimal amount) {
        MutableBalance balance = balances.get(accountId);
        balance.held = balance.held.subtract(amount);
        balance.posted = balance.posted.add(amount);
    }

    public synchronized List<TransactionResolution> resolutions() {
        return new ArrayList<>(resolutions);
    }

    public synchronized List<String> stillHeldTraceNumbers() {
        List<String> held = new ArrayList<>();
        for (Map.Entry<String, TransactionState> entry : transactions.entrySet()) {
            if (entry.getValue().authorized && !entry.getValue().resolved) {
                held.add(entry.getKey());
            }
        }
        return held;
    }

    public synchronized BigDecimal heldAmountFor(String traceNumber) {
        TransactionState state = transactions.get(traceNumber);
        return state == null ? null : state.holdAmount;
    }

    public synchronized List<BalanceSnapshot> balanceSnapshots() {
        List<BalanceSnapshot> out = new ArrayList<>();
        for (Map.Entry<String, MutableBalance> entry : balances.entrySet()) {
            MutableBalance b = entry.getValue();
            out.add(new BalanceSnapshot(entry.getKey(), b.available, b.held, b.posted));
        }
        return out;
    }

    public synchronized BalanceSnapshot balanceSnapshotFor(String accountId) {
        MutableBalance b = balances.get(accountId);
        return b == null ? null : new BalanceSnapshot(accountId, b.available, b.held, b.posted);
    }

    public synchronized BigDecimal openingBalanceFor(String accountId) {
        return openingBalances.get(accountId);
    }

    public synchronized int distinctKeysSeen() {
        return seenKeys.size();
    }
}
