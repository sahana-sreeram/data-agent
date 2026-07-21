"""Independent (pandas, not Spark) validation of the loan_portfolio curated summary.

Every metric here is recomputed directly from s3://<bucket>/raw/{loans,payment_events}.parquet
using this module's own pandas logic, then compared against
s3://<bucket>/curated/loan_portfolio.parquet. This deliberately does NOT import
src.etl_spark_loan_portfolio -- reusing the exact same calculation to "validate" itself
would let a bug in that calculation pass unnoticed. Different execution engine (pandas vs
Spark) and independently written aggregation logic, same discipline as
src/validate_portfolio.py relative to src/transform.py.
"""

from __future__ import annotations

import argparse

import pandas as pd

from src.lifecycle_validation_helpers import reconciliation_check as _reconciliation_check
from src.storage import S3Storage

DEFAULT_AS_OF_DATE = "2026-07-20"


def _rules_by_id(validation_rules: dict) -> dict:
    return {rule["id"]: rule for rule in validation_rules["rules"]}


def _independent_loan_portfolio_summary(
    loans: pd.DataFrame, payment_events: pd.DataFrame, business_rules: dict, as_of_date: str
) -> dict:
    net_payment_statuses = business_rules["successful_payment_statuses"] + ["REVERSED"]
    net_paid = (
        payment_events[payment_events["payment_status"].isin(net_payment_statuses)]
        .groupby("loan_id")["amount"]
        .sum()
        if not payment_events.empty
        else pd.Series(dtype=float)
    )

    loans = loans.merge(net_paid.rename("net_paid"), on="loan_id", how="left")
    loans["net_paid"] = loans["net_paid"].fillna(0.0)
    loans["outstanding_principal"] = (loans["principal_amount"] - loans["net_paid"]).clip(lower=0.0)

    accrual_statuses = business_rules["interest_accrual"]["accrues_on_statuses"]
    as_of = pd.Timestamp(as_of_date)
    originated = pd.to_datetime(loans["originated_at"])
    days_since_origination = (as_of - originated).dt.days
    accrues_mask = loans["loan_status"].isin(accrual_statuses)
    loans["accrued_interest"] = 0.0
    loans.loc[accrues_mask, "accrued_interest"] = (
        loans.loc[accrues_mask, "principal_amount"]
        * loans.loc[accrues_mask, "interest_rate"]
        * days_since_origination[accrues_mask]
        / 365.0
    )

    return {
        "loan_count": int(len(loans)),
        "active_loan_count": int((loans["loan_status"] == "ACTIVE").sum()),
        "closed_loan_count": int((loans["loan_status"] == "CLOSED").sum()),
        "defaulted_loan_count": int((loans["loan_status"] == "DEFAULTED").sum()),
        "total_funded_principal": round(float(loans["principal_amount"].sum()), 2),
        "total_outstanding_principal": round(float(loans["outstanding_principal"].sum()), 2),
        "avg_interest_rate": round(float(loans["interest_rate"].mean()), 4),
        "total_accrued_interest": round(float(loans["accrued_interest"].sum()), 2),
    }


def validate_loan_portfolio(
    storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE
) -> dict:
    loans = storage.read_parquet("raw/loans.parquet")
    payment_events = storage.read_parquet("raw/payment_events.parquet")
    curated = storage.read_parquet("curated/loan_portfolio.parquet")

    if len(curated) != 1:
        raise ValueError(f"curated/loan_portfolio.parquet must have exactly one row, got {len(curated)}")
    actual = curated.iloc[0].to_dict()

    expected = _independent_loan_portfolio_summary(loans, payment_events, business_rules, as_of_date)

    rules = _rules_by_id(validation_rules)
    tolerances = validation_rules["tolerance"]
    checks = [
        _reconciliation_check(rules[f"{metric}_reconciliation"], tolerances, expected[metric], actual[metric])
        for metric in expected
    ]

    failed = [c for c in checks if c["status"] == "FAIL"]
    return {
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the loan_portfolio curated summary.")
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json("context/validations/loan_portfolio.json")

    results = validate_loan_portfolio(storage, business_rules, validation_rules, args.as_of_date)
    storage.write_json("curated/loan_portfolio_validation_results.json", results)

    print(f"overall_status: {results['overall_status']}")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"  [{marker}] {check['id']}  expected={check['expected']} actual={check['actual']}")

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
