import duckdb
import pandas as pd
import pytest


def _make_connection(fixture_df: pd.DataFrame) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.register("fixture_df", fixture_df)
    con.execute("CREATE TABLE authorizations AS SELECT * FROM fixture_df")
    return con


@pytest.fixture
def make_connection():
    return _make_connection


@pytest.fixture
def row():
    """A row builder with sensible defaults, so each test's fixture only
    states the fields it cares about."""
    def _row(**overrides):
        base = {
            "event_id": 0, "issuer_bin": "A", "peer_cohort": "TIER_A",
            "channel": "CP", "amount_eur": 10.0, "decline_family": None, "approved": True,
        }
        base.update(overrides)
        return base
    return _row
