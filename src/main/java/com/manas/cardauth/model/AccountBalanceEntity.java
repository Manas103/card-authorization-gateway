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
 * The current balance state of one account: how much is freely spendable
 * ({@code availableBalance}), how much is on hold against an outstanding
 * authorization awaiting a terminal event ({@code heldAmount}), and how
 * much has actually been debited by a completed clearing
 * ({@code postedBalance}). Persisted via Spring Data JPA; H2 in
 * PostgreSQL-compatibility mode for tests and measurement, real PostgreSQL
 * for the production profile.
 *
 * <p>Conservation identity (checked in tests): for every account,
 * {@code availableBalance + heldAmount + postedBalance == openingBalance},
 * always. No message effect this engine applies can create or destroy
 * money; it can only move it between these three buckets.
 */
@Entity
@Table(name = "account_balance")
public class AccountBalanceEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "account_id", nullable = false, unique = true)
    private String accountId;

    @Column(name = "opening_balance", nullable = false, precision = 19, scale = 2)
    private BigDecimal openingBalance;

    @Column(name = "available_balance", nullable = false, precision = 19, scale = 2)
    private BigDecimal availableBalance;

    @Column(name = "held_amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal heldAmount;

    @Column(name = "posted_balance", nullable = false, precision = 19, scale = 2)
    private BigDecimal postedBalance;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected AccountBalanceEntity() {
        // JPA
    }

    public AccountBalanceEntity(String accountId, BigDecimal openingBalance, BigDecimal availableBalance,
                                 BigDecimal heldAmount, BigDecimal postedBalance, Instant updatedAt) {
        this.accountId = accountId;
        this.openingBalance = openingBalance;
        this.availableBalance = availableBalance;
        this.heldAmount = heldAmount;
        this.postedBalance = postedBalance;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public String getAccountId() {
        return accountId;
    }

    public BigDecimal getOpeningBalance() {
        return openingBalance;
    }

    public BigDecimal getAvailableBalance() {
        return availableBalance;
    }

    public void setAvailableBalance(BigDecimal availableBalance) {
        this.availableBalance = availableBalance;
    }

    public BigDecimal getHeldAmount() {
        return heldAmount;
    }

    public void setHeldAmount(BigDecimal heldAmount) {
        this.heldAmount = heldAmount;
    }

    public BigDecimal getPostedBalance() {
        return postedBalance;
    }

    public void setPostedBalance(BigDecimal postedBalance) {
        this.postedBalance = postedBalance;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
