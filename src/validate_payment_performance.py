"""Independent (pandas, not Spark) validation of the payment_performance curated summary.
Deliberately does NOT import src.etl_spark_payment_performance.
"""

from __future__ import annotations

import argparse

import pandas as pd

from src.lifecycle_validation_helpers import reconciliation_check as _reconciliation_check
from src.storage import S3Storage

DEFAULT_AS_OF_DATE = "2026-07-20"


def _independent_payment_performance(
    payment_schedule: pd.DataFrame, payment_events: pd.DataFrame, business_rules: dict, as_of_date: str
) -> dict:
    expected = payment_schedule[payment_schedule["due_date"] <= as_of_date]
    expected_payment_count = int(len(expected))
    expected_amount_due = round(float(expected["scheduled_amount"].sum()), 2)

    success_statuses = business_rules["successful_payment_statuses"]
    net_collected_statuses = success_statuses + ["REVERSED"]
    total_collected_amount = round(
        float(payment_events[payment_events["payment_status"].isin(net_collected_statuses)]["amount"].sum()), 2
    )
    successful_payment_count = int(payment_events["payment_status"].isin(success_statuses).sum())

    missed = payment_events[payment_events["payment_status"] == "MISSED"]
    missed_with_schedule = missed.merge(
        payment_schedule[["schedule_id", "scheduled_amount", "due_date"]], on="schedule_id", how="inner"
    )
    missed_payment_count = int(len(missed_with_schedule))
    missed_amount = round(float(missed_with_schedule["scheduled_amount"].sum()), 2) if missed_payment_count else 0.0

    late_payment_count = int((payment_events["payment_status"] == "LATE").sum())
    failed_payment_count = int((payment_events["payment_status"] == "FAILED").sum())

    threshold_days = business_rules["prepayment_threshold_days"]
    paid = payment_events[payment_events["payment_status"].isin(success_statuses)]
    paid_with_due = paid.merge(payment_schedule[["schedule_id", "due_date"]], on="schedule_id", how="inner")
    days_early = (pd.to_datetime(paid_with_due["due_date"]) - pd.to_datetime(paid_with_due["payment_date"])).dt.days
    prepaid_count = int((days_early >= threshold_days).sum())

    collection_rate = round(total_collected_amount / expected_amount_due, 4) if expected_amount_due > 0 else None
    prepayment_rate = round(prepaid_count / successful_payment_count, 4) if successful_payment_count > 0 else None

    return {
        "expected_payment_count": expected_payment_count,
        "expected_amount_due": expected_amount_due,
        "successful_payment_count": successful_payment_count,
        "total_collected_amount": total_collected_amount,
        "missed_payment_count": missed_payment_count,
        "missed_amount": missed_amount,
        "late_payment_count": late_payment_count,
        "failed_payment_count": failed_payment_count,
        "collection_rate": collection_rate,
        "prepayment_rate": prepayment_rate,
    }


def validate_payment_performance(
    storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE
) -> dict:
    payment_schedule = storage.read_parquet("raw/payment_schedule.parquet")
    payment_events = storage.read_parquet("raw/payment_events.parquet")
    curated = storage.read_parquet("curated/payment_performance.parquet")

    if len(curated) != 1:
        raise ValueError(f"curated/payment_performance.parquet must have exactly one row, got {len(curated)}")
    actual = curated.iloc[0].to_dict()

    expected = _independent_payment_performance(payment_schedule, payment_events, business_rules, as_of_date)

    rules = {rule["id"]: rule for rule in validation_rules["rules"]}
    tolerances = validation_rules["tolerance"]

    checks = [
        _reconciliation_check(rules[f"{metric}_reconciliation"], tolerances, expected_value, actual[metric])
        for metric, expected_value in expected.items()
    ]

    failed = [c for c in checks if c["status"] == "FAIL"]
    return {
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the payment_performance curated summary.")
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json("context/validations/payment_performance.json")

    results = validate_payment_performance(storage, business_rules, validation_rules, args.as_of_date)
    storage.write_json("curated/payment_performance_validation_results.json", results)

    print(f"overall_status: {results['overall_status']}")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"  [{marker}] {check['id']}  expected={check['expected']} actual={check['actual']}")

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
