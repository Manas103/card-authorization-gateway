package com.manas.cardauth.engine;

import com.manas.cardauth.model.LedgerEffect;

import java.math.BigDecimal;

/**
 * A card transaction (one traceNumber) that reached a terminal state:
 * its hold was either released back to available (a reversal) or released
 * and posted as spent (a clearing). {@code resolvingMessageId} is the
 * message id of whichever REVERSAL or CLEARING message closed it out.
 */
public record TransactionResolution(
        String traceNumber,
        String accountId,
        BigDecimal amount,
        LedgerEffect effect,
        String resolvingMessageId
) {
}
