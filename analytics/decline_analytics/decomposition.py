"""Claim: declines decomposed into 6 decline-reason families."""
from __future__ import annotations

import duckdb
import pandas as pd


def decline_family_mix(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("""
        SELECT decline_family, COUNT(*) AS declines, SUM(amount_eur) AS declined_amount_eur
        FROM authorizations
        WHERE NOT approved
        GROUP BY decline_family
        ORDER BY declines DESC
    """).fetchdf()
