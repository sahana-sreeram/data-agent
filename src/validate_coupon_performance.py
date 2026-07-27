"""Independent (pandas, not Spark) validation of the coupon_performance curated table.

Recomputes every coupon_code's redemption-funnel counts directly from raw Parquet using this
module's own pandas logic, then compares against the curated output -- aggregate reconciliation
per metric (summed across all codes) PLUS one per-code mismatch check, mirroring
src/validate_campaign_funnel.py's two-tier pattern for a multi-row curated table (an aggregate
sum alone can coincidentally still match even if individual rows are wrong in offsetting ways).
Deliberately does NOT import src.etl_spark_coupon_performance -- see that module's docstring
for why.
"""

from __future__ import annotations

import argparse

import pandas as pd

from src.lifecycle_validation_helpers import is_missing
from src.storage import S3Storage

DEFAULT_AS_OF_DATE = "2026-07-20"

COUNT_COLUMNS = ["coupon_rule_count", "currently_valid_rule_count", "offers_created", "applications_submitted", "loans_funded"]


def _independent_coupon_performance(
    coupon_rules: pd.DataFrame, prequal_offers: pd.DataFrame, applications: pd.DataFrame, loans: pd.DataFrame, as_of_date: str
) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of_date)

    valid_from = pd.to_datetime(coupon_rules["valid_from"])
    valid_to = pd.to_datetime(coupon_rules["valid_to"])
    coupon_rules = coupon_rules.assign(_currently_valid=(valid_from <= as_of) & (valid_to >= as_of))
    rules_by_code = coupon_rules.groupby("coupon_code").agg(
        coupon_rule_count=("coupon_rule_id", "nunique"), currently_valid_rule_count=("_currently_valid", "sum")
    )

    offers_as_of = prequal_offers[pd.to_datetime(prequal_offers["created_at"]).dt.normalize() <= as_of]
    coupon_offers = offers_as_of[offers_as_of["coupon_code"].notna()]
    offers_by_code = coupon_offers.groupby("coupon_code").size().rename("offers_created")

    app_coupon = applications.merge(coupon_offers[["offer_id", "coupon_code"]], on="offer_id", how="inner")
    applications_by_code = app_coupon.groupby("coupon_code").size().rename("applications_submitted")

    loans_by_code = (
        loans.merge(app_coupon[["application_id", "coupon_code"]], on="application_id", how="inner")
        .groupby("coupon_code")
        .size()
        .rename("loans_funded")
    )

    result = rules_by_code.join([offers_by_code, applications_by_code, loans_by_code], how="left").fillna(0)
    for column in COUNT_COLUMNS:
        result[column] = result[column].astype(int)
    result["redemption_rate"] = result.apply(
        lambda row: round(row["loans_funded"] / row["offers_created"], 4) if row["offers_created"] > 0 else None, axis=1
    )
    return result.reset_index()


def _reconciliation_check(rule: dict, tolerance: int, expected: int, actual: int) -> dict:
    difference = int(actual) - int(expected)
    status = "PASS" if abs(difference) <= tolerance else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": int(expected),
        "actual": int(actual),
        "difference": difference,
        "details": None,
    }


def validate_coupon_performance(
    storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE
) -> dict:
    coupon_rules = storage.read_parquet("raw/coupon_rules.parquet")
    prequal_offers = storage.read_parquet("raw/prequal_offers.parquet")
    applications = storage.read_parquet("raw/applications.parquet")
    loans = storage.read_parquet("raw/loans.parquet")
    curated = storage.read_parquet("curated/coupon_performance.parquet")

    expected = _independent_coupon_performance(coupon_rules, prequal_offers, applications, loans, as_of_date)

    rules = {rule["id"]: rule for rule in validation_rules["rules"]}
    count_tolerance = validation_rules["tolerance"]["count"]
    rate_tolerance = validation_rules["tolerance"]["rate"]

    expected_by_code = {row["coupon_code"]: row for _, row in expected.iterrows()}
    curated_by_code = {row["coupon_code"]: row for _, row in curated.iterrows()}

    checks = []
    for column in COUNT_COLUMNS:
        rule = rules[f"{column}_reconciliation"]
        total_expected = sum(int(row[column]) for row in expected_by_code.values())
        total_actual = sum(int(curated_by_code[code][column]) for code in expected_by_code if code in curated_by_code)
        checks.append(_reconciliation_check(rule, count_tolerance, total_expected, total_actual))

    def _rate_mismatch(expected_row, actual_row) -> bool:
        expected_value, actual_value = expected_row["redemption_rate"], actual_row["redemption_rate"]
        expected_missing, actual_missing = is_missing(expected_value), is_missing(actual_value)
        if expected_missing and actual_missing:
            return False
        if expected_missing != actual_missing:
            return True
        return abs(float(actual_value) - float(expected_value)) > rate_tolerance

    missing_codes = set(expected_by_code) - set(curated_by_code)
    mismatched = []
    for coupon_code, expected_row in expected_by_code.items():
        actual_row = curated_by_code.get(coupon_code)
        if actual_row is None:
            continue  # already counted in missing_codes below
        if any(int(actual_row[c]) != int(expected_row[c]) for c in COUNT_COLUMNS):
            mismatched.append(coupon_code)
        elif _rate_mismatch(expected_row, actual_row):
            mismatched.append(coupon_code)

    per_code_rule = rules["coupon_performance_row_counts_match_per_code"]
    checks.append(
        {
            "id": per_code_rule["id"],
            "description": per_code_rule["description"],
            "status": "PASS" if not mismatched and not missing_codes else "FAIL",
            "expected": 0,
            "actual": len(mismatched) + len(missing_codes),
            "difference": len(mismatched) + len(missing_codes),
            "details": f"mismatched or missing coupon_codes: {mismatched + sorted(missing_codes)}" if (mismatched or missing_codes) else None,
        }
    )

    failed = [c for c in checks if c["status"] == "FAIL"]
    return {
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the coupon_performance curated summary.")
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json("context/validations/coupon_performance.json")

    results = validate_coupon_performance(storage, business_rules, validation_rules, args.as_of_date)
    storage.write_json("curated/coupon_performance_validation_results.json", results)

    print(f"overall_status: {results['overall_status']}")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"  [{marker}] {check['id']}")

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
