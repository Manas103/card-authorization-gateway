"""Full measured run over the Java-generated 1.2M-row authorisation
CSV: decline-reason decomposition, peer-cohort benchmarking, CNP
recoverable-decline isolation, and recovered-revenue sizing against
named assumptions. Run with:

    python scripts/run_analysis.py

stdout is committed verbatim as ../docs/analytics_benchmark_output.txt.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from decline_analytics.decomposition import decline_family_mix
from decline_analytics.loader import connect
from decline_analytics.peer_benchmark import benchmark_against_peer_cohort, issuer_decline_rates
from decline_analytics.recoverable import cnp_decline_recoverability, recoverable_share_of_cnp_declines
from decline_analytics.revenue import recovered_revenue_by_mechanism, total_recovered_revenue


def main() -> None:
    con = connect()
    total = con.execute("SELECT COUNT(*) FROM authorizations").fetchone()[0]
    print("=== Dataset ===")
    print(f"total authorisations: {total}")

    print("\n=== Decline-reason family decomposition ===")
    mix = decline_family_mix(con)
    print(mix.to_string(index=False))

    print("\n=== Peer-cohort benchmark ===")
    rates = issuer_decline_rates(con)
    benchmarked = benchmark_against_peer_cohort(rates)
    print(benchmarked.to_string(index=False))
    cohort_summary = benchmarked.groupby("peer_cohort")["decline_rate"].agg(["mean", "median", "std"])
    print("\nper-cohort decline rate summary:")
    print(cohort_summary.to_string())

    print("\n=== CNP decline recoverability ===")
    recoverability = cnp_decline_recoverability(con)
    print(recoverability.to_string(index=False))
    share = recoverable_share_of_cnp_declines(recoverability)
    for k, v in share.items():
        print(f"{k}: {v}")

    print("\n=== Recovered revenue by mechanism (named assumptions) ===")
    by_mechanism = recovered_revenue_by_mechanism(recoverability)
    print(by_mechanism.to_string(index=False))
    total_recovered = total_recovered_revenue(by_mechanism)
    print(f"\ntotal expected recovered revenue (EUR): {total_recovered:,.2f}")

    print("\n=== Claims summary ===")
    print(f"1.2M simulated card authorisations: {total}")
    print(f"decomposed into 6 decline-reason families: {len(mix)}")
    print(f"each issuer benchmarked against a peer cohort: {benchmarked['issuer_bin'].nunique()} issuers "
          f"across {benchmarked['peer_cohort'].nunique()} cohorts")
    print(f"18% of declined CNP volume isolated as recoverable: "
          f"{share['recoverable_share_by_count']*100:.2f}% by count, "
          f"{share['recoverable_share_by_amount']*100:.2f}% by amount")
    print(f"recovered revenue stated against named assumptions: EUR {total_recovered:,.2f} "
          f"(assumptions: {by_mechanism[['recovery_mechanism', 'recovery_success_probability']].to_dict('records')})")

    con.close()


if __name__ == "__main__":
    main()
