"""Named assumptions shared across the decline-decomposition and
recoverable-revenue analysis. The dataset itself (issuer, cohort,
channel, amount, decline family) is measured from the Java-generated
CSV; everything in this file is a judgment call, stated here once so it
cannot drift between the analysis and the README.
"""
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "authorizations.csv"

# A decline family is "recoverable" here only if one of the three named
# mechanisms addresses the decline WITHOUT requiring the issuer to
# re-evaluate the transaction. That is a narrower bar than "could this
# decline ever succeed on a later attempt", and it is what excludes four
# of the six families, not just the two risk/policy ones:
#   - CARD_DATA_ISSUE: this is exactly what an account updater fixes
#     (an issuer-registered new PAN/expiry for a reissued card).
#   - ISSUER_UNAVAILABLE: a genuine technical blip; the issuer's own
#     system recovers on its own, so a retry schedule (not the merchant
#     doing anything differently) is enough.
#   - INSUFFICIENT_FUNDS and CVV_AVS_MISMATCH were tried in the first
#     design and dropped (see README Findings): insufficient funds
#     requires the cardholder's own balance to change, which a retry
#     schedule does not cause, only wait for; CVV/AVS mismatch is a risk
#     signal a merchant should not simply resubmit around with a guessed
#     value, and tokenization does not retroactively fix a transaction
#     that already failed the check.
#   - SUSPECTED_FRAUD and DO_NOT_HONOR are risk-based or issuer-policy
#     declines; none of the three mechanisms changes the issuer's
#     underlying risk or policy decision.
RECOVERY_MECHANISM = {
    "CARD_DATA_ISSUE": "account_updater_or_network_token",
    "ISSUER_UNAVAILABLE": "retry_schedule",
}
NON_RECOVERABLE_FAMILIES = {"SUSPECTED_FRAUD", "DO_NOT_HONOR", "INSUFFICIENT_FUNDS", "CVV_AVS_MISMATCH"}

# CARD_DATA_ISSUE declines are split between the two card-data-refresh
# mechanisms by assumed merchant integration type: a tokenized merchant's
# stale credential is fixed transparently by the network token, a
# non-tokenized merchant's by the issuer's account updater feed. Named
# assumption, not measured; the dataset carries no merchant-integration
# field.
CARD_DATA_ISSUE_TOKENIZED_SHARE = 0.5

# Recovery success probability, by mechanism: the share of a recoverable
# decline's value assumed to actually come back on retry/update, not 100%.
# These are stated assumptions, not measured figures; they are the thing
# the revenue number should be checked against, not treated as derived.
RECOVERY_SUCCESS_PROBABILITY = {
    "retry_schedule": 0.35,     # issuer has the funds shortly after, or the outage clears
    "account_updater": 0.55,    # network's account updater resolves most genuine card-data drift
    "network_token": 0.45,      # token abstracts a PAN/CVV change, but not every merchant is tokenized
}
