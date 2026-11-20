"""Claim: each issuer benchmarked against a peer cohort. An issuer's
decline rate is compared to its own peer cohort's median decline rate
(not the portfolio-wide median), because a TIER_C issuer's decline rate
is not a meaningful signal against TIER_A's baseline, and a real
peer-benchmarking exercise groups issuers before comparing them.
"""
from __future__ import annotations

import duckdb
import pandas as pd


def issuer_decline_rates(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("""
        SELECT issuer_bin, peer_cohort,
               COUNT(*) AS authorizations,
               SUM(CASE WHEN NOT approved THEN 1 ELSE 0 END) AS declines,
               AVG(CASE WHEN NOT approved THEN 1.0 ELSE 0.0 END) AS decline_rate
        FROM authorizations
        GROUP BY issuer_bin, peer_cohort
        ORDER BY peer_cohort, issuer_bin
    """).fetchdf()


def benchmark_against_peer_cohort(issuer_rates: pd.DataFrame) -> pd.DataFrame:
    """Adds each issuer's peer-cohort median decline rate and the ratio
    of the issuer's own rate to it (1.0 = exactly at the cohort median,
    above 1.0 = worse than peers)."""
    out = issuer_rates.copy()
    cohort_median = out.groupby("peer_cohort")["decline_rate"].median()
    out["peer_cohort_median_decline_rate"] = out["peer_cohort"].map(cohort_median)
    out["vs_peer_cohort_ratio"] = out["decline_rate"] / out["peer_cohort_median_decline_rate"]
    return out
