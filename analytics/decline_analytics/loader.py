"""Loads the Java-generated authorisation CSV into DuckDB. A thin
wrapper, not a pipeline step on its own, so every other module can share
one connection construction and one schema check.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from . import config

EXPECTED_COLUMNS = {"event_id", "issuer_bin", "peer_cohort", "channel", "amount_eur", "decline_family", "approved"}


def connect(csv_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    path = csv_path or config.CSV_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found; generate it with "
            "'mvn test -Dtest=AuthorizationExportRunner -Dsurefire.excludedGroups=' from the repo root"
        )
    con = duckdb.connect(":memory:")
    con.execute(f"""
        CREATE TABLE authorizations AS
        SELECT * FROM read_csv('{path.as_posix()}', header=true,
            columns={{
                'event_id': 'BIGINT', 'issuer_bin': 'VARCHAR', 'peer_cohort': 'VARCHAR',
                'channel': 'VARCHAR', 'amount_eur': 'DOUBLE', 'decline_family': 'VARCHAR',
                'approved': 'BOOLEAN'
            }})
    """)
    actual_columns = {row[0] for row in con.execute("DESCRIBE authorizations").fetchall()}
    if actual_columns != EXPECTED_COLUMNS:
        raise ValueError(f"unexpected CSV schema: {actual_columns}")
    return con
