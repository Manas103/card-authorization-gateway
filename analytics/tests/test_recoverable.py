import pandas as pd
import pytest

from decline_analytics.recoverable import cnp_decline_recoverability, recoverable_share_of_cnp_declines


def test_cnp_decline_recoverability_excludes_cp_and_approved(row, make_connection):
    fixture = pd.DataFrame([
        row(channel="CNP", approved=False, decline_family="INSUFFICIENT_FUNDS", amount_eur=10.0),
        row(channel="CNP", approved=False, decline_family="SUSPECTED_FRAUD", amount_eur=20.0),
        row(channel="CP", approved=False, decline_family="INSUFFICIENT_FUNDS", amount_eur=999.0),  # excluded: CP
        row(channel="CNP", approved=True, amount_eur=999.0),  # excluded: approved
    ])
    con = make_connection(fixture)
    recoverability = cnp_decline_recoverability(con)
    assert recoverability["declines"].sum() == 2
    assert set(recoverability["decline_family"]) == {"INSUFFICIENT_FUNDS", "SUSPECTED_FRAUD"}
    con.close()


def test_recoverable_share_hand_computed(row, make_connection):
    fixture = pd.DataFrame([
        row(channel="CNP", approved=False, decline_family="CARD_DATA_ISSUE", amount_eur=10.0),      # recoverable
        row(channel="CNP", approved=False, decline_family="ISSUER_UNAVAILABLE", amount_eur=10.0),   # recoverable
        row(channel="CNP", approved=False, decline_family="SUSPECTED_FRAUD", amount_eur=10.0),      # not recoverable
        row(channel="CNP", approved=False, decline_family="DO_NOT_HONOR", amount_eur=10.0),          # not recoverable
    ])
    con = make_connection(fixture)
    recoverability = cnp_decline_recoverability(con)
    share = recoverable_share_of_cnp_declines(recoverability)
    assert share["total_cnp_declines"] == 4
    assert share["recoverable_cnp_declines"] == 2
    assert share["recoverable_share_by_count"] == pytest.approx(0.5)
    assert share["recoverable_share_by_amount"] == pytest.approx(0.5)
    con.close()


def test_non_recoverable_families_are_marked_false(row, make_connection):
    fixture = pd.DataFrame([
        row(channel="CNP", approved=False, decline_family="SUSPECTED_FRAUD", amount_eur=10.0),
        row(channel="CNP", approved=False, decline_family="DO_NOT_HONOR", amount_eur=10.0),
    ])
    con = make_connection(fixture)
    recoverability = cnp_decline_recoverability(con)
    assert not recoverability["recoverable"].any()
    con.close()
