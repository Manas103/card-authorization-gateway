package com.manas.cardauth.engine;

import com.manas.cardauth.model.CardMessage;

/**
 * What happened to one ingested {@link CardMessage}. DUPLICATE and
 * BUFFERED never touch a balance; HELD, RELEASED and CLEARED each
 * represent exactly one balance effect actually applied.
 */
public final class LedgerOutcome {

    public enum Type { HELD, RELEASED, CLEARED, BUFFERED, DUPLICATE }

    private final Type type;
    private final CardMessage message;
    private final TransactionResolution resolution;
    private final String traceNumber;
    private final String accountId;
    private final java.math.BigDecimal amount;

    private LedgerOutcome(Type type, CardMessage message, TransactionResolution resolution,
                           String traceNumber, String accountId, java.math.BigDecimal amount) {
        this.type = type;
        this.message = message;
        this.resolution = resolution;
        this.traceNumber = traceNumber;
        this.accountId = accountId;
        this.amount = amount;
    }

    public static LedgerOutcome held(String traceNumber, String accountId, java.math.BigDecimal amount) {
        return new LedgerOutcome(Type.HELD, null, null, traceNumber, accountId, amount);
    }

    public static LedgerOutcome resolved(Type type, TransactionResolution resolution) {
        return new LedgerOutcome(type, null, resolution, resolution.traceNumber(), resolution.accountId(), resolution.amount());
    }

    public static LedgerOutcome buffered(CardMessage message) {
        return new LedgerOutcome(Type.BUFFERED, message, null, message.traceNumber(), message.accountId(), message.amount());
    }

    public static LedgerOutcome duplicate(CardMessage message) {
        return new LedgerOutcome(Type.DUPLICATE, message, null, message.traceNumber(), message.accountId(), message.amount());
    }

    public Type type() {
        return type;
    }

    public CardMessage message() {
        return message;
    }

    public TransactionResolution resolution() {
        return resolution;
    }

    public String traceNumber() {
        return traceNumber;
    }

    public String accountId() {
        return accountId;
    }

    public java.math.BigDecimal amount() {
        return amount;
    }
}
