package com.manas.cardauth.model;

/**
 * The terminal effect a transaction's ledger entry records: funds released
 * back to available with nothing posted (a reversal), or funds released
 * from hold and posted as spent (a clearing). There is no HOLD entry in
 * the ledger table; a hold is a balance-in-progress, not a resolution, and
 * only resolved transactions get an audit row (see LedgerEntryEntity).
 */
public enum LedgerEffect {
    RELEASED,
    CLEARED
}
