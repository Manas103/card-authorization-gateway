import pandas as pd
import pytest

from decline_analytics import config
from decline_analytics.revenue import recovered_revenue_by_mechanism, total_recovered_revenue


def test_recovered_revenue_hand_computed():
    recoverability = pd.DataFrame([
        {"decline_family": "INSUFFICIENT_FUNDS", "declines": 5, "declined_amount_eur": 100.0,
         "recovery_mechanism": "retry_schedule", "recoverable": True},
        {"decline_family": "CARD_DATA_ISSUE", "declines": 2, "declined_amount_eur": 200.0,
         "recovery_mechanism": "account_updater", "recoverable": True},
        {"decline_family": "SUSPECTED_FRAUD", "declines": 3, "declined_amount_eur": 50.0,
         "recovery_mechanism": None, "recoverable": False},
    ])
    by_mechanism = recovered_revenue_by_mechanism(recoverability)
    retry_row = by_mechanism.loc[by_mechanism["recovery_mechanism"] == "retry_schedule"].iloc[0]
    assert retry_row["declined_amount_eur"] == pytest.approx(100.0)
    assert retry_row["expected_recovered_revenue_eur"] == pytest.approx(100.0 * config.RECOVERY_SUCCESS_PROBABILITY["retry_schedule"])

    total = total_recovered_revenue(by_mechanism)
    expected_total = (100.0 * config.RECOVERY_SUCCESS_PROBABILITY["retry_schedule"]
                       + 200.0 * config.RECOVERY_SUCCESS_PROBABILITY["account_updater"])
    assert total == pytest.approx(expected_total)


def test_card_data_issue_splits_between_account_updater_and_network_token():
    recoverability = pd.DataFrame([
        {"decline_family": "CARD_DATA_ISSUE", "declines": 10, "declined_amount_eur": 1000.0,
         "recovery_mechanism": "account_updater_or_network_token", "recoverable": True},
    ])
    by_mechanism = recovered_revenue_by_mechanism(recoverability).set_index("recovery_mechanism")
    share = config.CARD_DATA_ISSUE_TOKENIZED_SHARE
    assert by_mechanism.loc["network_token", "declined_amount_eur"] == pytest.approx(1000.0 * share)
    assert by_mechanism.loc["account_updater", "declined_amount_eur"] == pytest.approx(1000.0 * (1 - share))


def test_non_recoverable_rows_excluded_from_revenue():
    recoverability = pd.DataFrame([
        {"decline_family": "SUSPECTED_FRAUD", "declines": 3, "declined_amount_eur": 999.0,
         "recovery_mechanism": None, "recoverable": False},
    ])
    by_mechanism = recovered_revenue_by_mechanism(recoverability)
    assert by_mechanism.empty
    assert total_recovered_revenue(by_mechanism) == 0.0
