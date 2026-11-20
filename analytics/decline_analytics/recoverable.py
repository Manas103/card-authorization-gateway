"""Claim: declined card-not-present volume isolated as recoverable
through network tokens, account updater and a retry schedule. Restricted
to CNP because the three named recovery mechanisms are specifically
card-on-file / repeat-merchant techniques; a card-present decline at a
physical terminal is not something a network token or account updater
retroactively fixes in the same way.
"""
from __future__ import annotations

import duckdb
import pandas as pd

from . import config


def cnp_decline_recoverability(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("""
        SELECT decline_family, COUNT(*) AS declines, SUM(amount_eur) AS declined_amount_eur
        FROM authorizations
        WHERE NOT approved AND channel = 'CNP'
        GROUP BY decline_family
    """).fetchdf()
    df["recovery_mechanism"] = df["decline_family"].map(config.RECOVERY_MECHANISM)
    df["recoverable"] = df["decline_family"].isin(config.RECOVERY_MECHANISM)
    return df


def recoverable_share_of_cnp_declines(recoverability: pd.DataFrame) -> dict:
    total_declines = recoverability["declines"].sum()
    recoverable_declines = recoverability.loc[recoverability["recoverable"], "declines"].sum()
    total_amount = recoverability["declined_amount_eur"].sum()
    recoverable_amount = recoverability.loc[recoverability["recoverable"], "declined_amount_eur"].sum()
    return {
        "total_cnp_declines": int(total_declines),
        "recoverable_cnp_declines": int(recoverable_declines),
        "recoverable_share_by_count": recoverable_declines / total_declines,
        "total_cnp_declined_amount_eur": float(total_amount),
        "recoverable_cnp_declined_amount_eur": float(recoverable_amount),
        "recoverable_share_by_amount": recoverable_amount / total_amount,
    }
