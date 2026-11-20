import pandas as pd
import pytest

from decline_analytics.peer_benchmark import benchmark_against_peer_cohort, issuer_decline_rates


def test_issuer_decline_rates_hand_computed(row, make_connection):
    fixture = pd.DataFrame([
        row(issuer_bin="A", peer_cohort="TIER_A", approved=True),
        row(issuer_bin="A", peer_cohort="TIER_A", approved=True),
        row(issuer_bin="A", peer_cohort="TIER_A", approved=False, decline_family="DO_NOT_HONOR"),
        row(issuer_bin="A", peer_cohort="TIER_A", approved=False, decline_family="DO_NOT_HONOR"),
    ])
    con = make_connection(fixture)
    rates = issuer_decline_rates(con)
    assert rates.loc[rates["issuer_bin"] == "A", "decline_rate"].iloc[0] == pytest.approx(0.5)
    con.close()


def test_benchmark_against_peer_cohort_ratio(row, make_connection):
    fixture = pd.DataFrame(
        [row(issuer_bin="A", peer_cohort="T", approved=False, decline_family="DO_NOT_HONOR")] * 2
        + [row(issuer_bin="A", peer_cohort="T", approved=True)] * 8
        + [row(issuer_bin="B", peer_cohort="T", approved=False, decline_family="DO_NOT_HONOR")] * 1
        + [row(issuer_bin="B", peer_cohort="T", approved=True)] * 9
    )
    con = make_connection(fixture)
    rates = issuer_decline_rates(con)
    benchmarked = benchmark_against_peer_cohort(rates)
    # A: 0.20 decline rate, B: 0.10; cohort median = 0.15; A's ratio = 0.20/0.15
    a_row = benchmarked.loc[benchmarked["issuer_bin"] == "A"].iloc[0]
    assert a_row["decline_rate"] == pytest.approx(0.20)
    assert a_row["peer_cohort_median_decline_rate"] == pytest.approx(0.15)
    assert a_row["vs_peer_cohort_ratio"] == pytest.approx(0.20 / 0.15)
    con.close()
