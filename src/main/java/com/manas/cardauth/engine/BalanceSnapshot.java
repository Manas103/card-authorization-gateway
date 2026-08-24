package com.manas.cardauth.engine;

import java.math.BigDecimal;

/**
 * The three-way split of one account's money at a point in time.
 * {@code availableBalance + heldAmount + postedBalance} must always equal
 * the account's opening balance; that identity is the conservation
 * invariant checked in tests.
 */
public record BalanceSnapshot(String accountId, BigDecimal availableBalance, BigDecimal heldAmount, BigDecimal postedBalance) {
}
