package com.manas.cardauth.engine;

import com.manas.cardauth.model.CardMessage;
import com.manas.cardauth.model.LedgerEffect;
import com.manas.cardauth.model.MessageType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A deliberately slow, deliberately unindexed re-implementation used only
 * to check the real {@link AuthorizationLedgerEngine} against (BUILDER.md
 * section 2, "a reference oracle where correctness is non-obvious").
 * Everything here is a plain list scan: no HashMap keyed lookup for
 * dedup, no incremental per-message state machine, nothing that could
 * hide the same bug the real engine might have. It operates on a static,
 * already-fully-received list rather than a stream, applying every
 * transaction's AUTHORIZATION before its terminal event regardless of
 * where either message physically sits in the input list, which is a
 * correct model of the question this oracle answers: does out-of-order,
 * duplicated delivery produce the same final account balances and
 * transaction outcomes as applying every transaction's two messages in
 * their logical order.
 *
 * <p>Deliberately O(n^2) (a linear scan for dedup, two linear scans per
 * traceNumber to find its authorization and terminal message); only ever
 * run over small samples in tests, never over the full 100,000-message
 * benchmark set.
 */
public final class ReferenceOracleLedger {

    public record OracleResult(List<BalanceSnapshot> balances, List<TransactionResolution> resolutions, List<String> stillHeldTraceNumbers) {
    }

    public OracleResult evaluate(List<CardMessage> allMessages, Map<String, BigDecimal> openingBalances) {
        // Step 1: dedup by (messageId, traceNumber), keeping only the first occurrence, via a plain linear scan.
        List<CardMessage> deduped = new ArrayList<>();
        List<String> seenKeys = new ArrayList<>();
        for (CardMessage message : allMessages) {
            String key = message.messageId() + "|" + message.traceNumber();
            if (!seenKeys.contains(key)) {
                seenKeys.add(key);
                deduped.add(message);
            }
        }

        // Step 2: collect the distinct trace numbers, in first-seen order, via a plain linear scan.
        List<String> traceNumbers = new ArrayList<>();
        for (CardMessage message : deduped) {
            if (!traceNumbers.contains(message.traceNumber())) {
                traceNumbers.add(message.traceNumber());
            }
        }

        Map<String, BigDecimal> available = new LinkedHashMap<>();
        Map<String, BigDecimal> held = new LinkedHashMap<>();
        Map<String, BigDecimal> posted = new LinkedHashMap<>();
        for (Map.Entry<String, BigDecimal> entry : openingBalances.entrySet()) {
            available.put(entry.getKey(), entry.getValue());
            held.put(entry.getKey(), BigDecimal.ZERO.setScale(2));
            posted.put(entry.getKey(), BigDecimal.ZERO.setScale(2));
        }

        List<TransactionResolution> resolutions = new ArrayList<>();
        List<String> stillHeld = new ArrayList<>();

        // Step 3: for each trace number, scan the whole deduped list to find its AUTHORIZATION
        // and (if present) its terminal message, applying the hold and the resolution in that
        // logical order regardless of physical position in the input list.
        for (String traceNumber : traceNumbers) {
            CardMessage authorization = null;
            CardMessage terminal = null;
            for (CardMessage message : deduped) {
                if (!message.traceNumber().equals(traceNumber)) {
                    continue;
                }
                if (message.type() == MessageType.AUTHORIZATION) {
                    authorization = message;
                } else {
                    terminal = message;
                }
            }
            if (authorization == null) {
                // A terminal event with no authorization anywhere in this dataset is not a
                // valid transaction the oracle can judge; the real engine would leave it
                // buffered forever too. Never produced by the synthetic generator.
                continue;
            }

            String accountId = authorization.accountId();
            BigDecimal amount = authorization.amount();
            available.put(accountId, available.get(accountId).subtract(amount));
            held.put(accountId, held.get(accountId).add(amount));

            if (terminal == null) {
                stillHeld.add(traceNumber);
                continue;
            }

            if (terminal.type() == MessageType.REVERSAL) {
                held.put(accountId, held.get(accountId).subtract(amount));
                available.put(accountId, available.get(accountId).add(amount));
                resolutions.add(new TransactionResolution(traceNumber, accountId, amount, LedgerEffect.RELEASED, terminal.messageId()));
            } else {
                held.put(accountId, held.get(accountId).subtract(amount));
                posted.put(accountId, posted.get(accountId).add(amount));
                resolutions.add(new TransactionResolution(traceNumber, accountId, amount, LedgerEffect.CLEARED, terminal.messageId()));
            }
        }

        List<BalanceSnapshot> balances = new ArrayList<>();
        for (String accountId : openingBalances.keySet()) {
            balances.add(new BalanceSnapshot(accountId, available.get(accountId), held.get(accountId), posted.get(accountId)));
        }

        return new OracleResult(balances, resolutions, stillHeld);
    }
}
