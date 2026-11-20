import duckdb
import pandas as pd
import pytest

from decline_analytics.decomposition import decline_family_mix

FIXTURE = pd.DataFrame([
    {"event_id": 1, "issuer_bin": "A", "peer_cohort": "TIER_A", "channel": "CP", "amount_eur": 10.0, "decline_family": None, "approved": True},
    {"event_id": 2, "issuer_bin": "A", "peer_cohort": "TIER_A", "channel": "CNP", "amount_eur": 20.0, "decline_family": "INSUFFICIENT_FUNDS", "approved": False},
    {"event_id": 3, "issuer_bin": "A", "peer_cohort": "TIER_A", "channel": "CNP", "amount_eur": 30.0, "decline_family": "INSUFFICIENT_FUNDS", "approved": False},
    {"event_id": 4, "issuer_bin": "B", "peer_cohort": "TIER_B", "channel": "CP", "amount_eur": 40.0, "decline_family": "DO_NOT_HONOR", "approved": False},
])


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    connection.register("fixture_df", FIXTURE)
    connection.execute("CREATE TABLE authorizations AS SELECT * FROM fixture_df")
    yield connection
    connection.close()


def test_decline_family_mix_hand_computed(con):
    mix = decline_family_mix(con).set_index("decline_family")
    assert mix.loc["INSUFFICIENT_FUNDS", "declines"] == 2
    assert mix.loc["INSUFFICIENT_FUNDS", "declined_amount_eur"] == pytest.approx(50.0)
    assert mix.loc["DO_NOT_HONOR", "declines"] == 1


def test_decline_family_mix_excludes_approved_rows(con):
    mix = decline_family_mix(con)
    assert mix["declines"].sum() == 3  # the one approved row is excluded
