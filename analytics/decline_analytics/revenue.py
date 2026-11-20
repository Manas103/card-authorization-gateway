"""Claim: recovered revenue stated against its named assumptions. Takes
the recoverable CNP decline amount, by mechanism, and applies
config.RECOVERY_SUCCESS_PROBABILITY (a stated assumption, not a measured
figure) to get an expected recovered amount per mechanism.
"""
from __future__ import annotations

import pandas as pd

from . import config


def _split_card_data_issue(recoverable: pd.DataFrame) -> pd.DataFrame:
    """CARD_DATA_ISSUE is tagged with the combined mechanism label
    "account_updater_or_network_token"; this splits its amount between
    the two real mechanisms by the assumed tokenized-merchant share, so
    each mechanism gets its own named recovery-success probability."""
    rows = []
    for _, series in recoverable.iterrows():
        r = series.to_dict()
        if r["recovery_mechanism"] == "account_updater_or_network_token":
            share = config.CARD_DATA_ISSUE_TOKENIZED_SHARE
            rows.append({**r, "recovery_mechanism": "network_token", "declined_amount_eur": r["declined_amount_eur"] * share})
            rows.append({**r, "recovery_mechanism": "account_updater", "declined_amount_eur": r["declined_amount_eur"] * (1 - share)})
        else:
            rows.append(r)
    return pd.DataFrame(rows)


def recovered_revenue_by_mechanism(recoverability: pd.DataFrame) -> pd.DataFrame:
    recoverable = recoverability.loc[recoverability["recoverable"]].copy()
    if recoverable.empty:
        return pd.DataFrame(columns=["recovery_mechanism", "declined_amount_eur",
                                      "recovery_success_probability", "expected_recovered_revenue_eur"])
    split = _split_card_data_issue(recoverable)
    by_mechanism = split.groupby("recovery_mechanism")["declined_amount_eur"].sum().reset_index()
    by_mechanism["recovery_success_probability"] = by_mechanism["recovery_mechanism"].map(
        config.RECOVERY_SUCCESS_PROBABILITY
    )
    by_mechanism["expected_recovered_revenue_eur"] = (
        by_mechanism["declined_amount_eur"] * by_mechanism["recovery_success_probability"]
    )
    return by_mechanism


def total_recovered_revenue(by_mechanism: pd.DataFrame) -> float:
    return float(by_mechanism["expected_recovered_revenue_eur"].sum())
