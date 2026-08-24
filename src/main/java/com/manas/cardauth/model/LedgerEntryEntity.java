package com.manas.cardauth.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One applied balance effect: a hold placed, a hold released by reversal,
 * or a hold released and posted by clearing. One row per resolved
 * {@code traceNumber}, the audit trail behind every number in the
 * account_balance table. Recording the effect and the message that caused
 * it (rather than only the current balance) is what makes "no double
 * release" independently checkable: a traceNumber with more than one
 * ledger entry is a bug, not a feature.
 */
@Entity
@Table(name = "ledger_entry", uniqueConstraints = @jakarta.persistence.UniqueConstraint(columnNames = "trace_number"))
public class LedgerEntryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "trace_number", nullable = false)
    private String traceNumber;

    @Column(name = "account_id", nullable = false)
    private String accountId;

    @Column(name = "effect", nullable = false)
    @jakarta.persistence.Enumerated(jakarta.persistence.EnumType.STRING)
    private LedgerEffect effect;

    @Column(name = "amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(name = "resolving_message_id", nullable = false)
    private String resolvingMessageId;

    @Column(name = "applied_at", nullable = false)
    private Instant appliedAt;

    protected LedgerEntryEntity() {
        // JPA
    }

    public LedgerEntryEntity(String traceNumber, String accountId, LedgerEffect effect, BigDecimal amount,
                              String resolvingMessageId, Instant appliedAt) {
        this.traceNumber = traceNumber;
        this.accountId = accountId;
        this.effect = effect;
        this.amount = amount;
        this.resolvingMessageId = resolvingMessageId;
        this.appliedAt = appliedAt;
    }

    public Long getId() {
        return id;
    }

    public String getTraceNumber() {
        return traceNumber;
    }

    public String getAccountId() {
        return accountId;
    }

    public LedgerEffect getEffect() {
        return effect;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getResolvingMessageId() {
        return resolvingMessageId;
    }

    public Instant getAppliedAt() {
        return appliedAt;
    }
}
