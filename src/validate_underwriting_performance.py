"""Independent (pandas, not Spark) validation of the underwriting_performance curated
tables. Deliberately does NOT import src.etl_spark_underwriting_performance.
"""

from __future__ import annotations

import pandas as pd

from src.storage import S3Storage


def _breakdown(decisions: pd.DataFrame, group_column: str, breakdown_type: str) -> pd.DataFrame:
    rows = []
    for value, group in decisions.groupby(group_column):
        approved = group[group["decision"] == "APPROVED"]
        decision_count = len(group)
        approved_count = len(approved)
        rows.append(
            {
                "breakdown_type": breakdown_type,
                "breakdown_value": value,
                "decision_count": decision_count,
                "approved_count": approved_count,
                "rejected_count": int((group["decision"] == "REJECTED").sum()),
                "manual_review_count": int((group["decision"] == "MANUAL_REVIEW").sum()),
                "approval_rate": round(approved_count / decision_count, 4) if decision_count else None,
                "avg_approved_amount": round(float(approved["approved_amount"].mean()), 2) if len(approved) else None,
                "avg_approved_apr": round(float(approved["approved_apr"].mean()), 4) if len(approved) else None,
            }
        )
    return pd.DataFrame(rows)


def _independent_underwriting_performance(
    underwriting_decisions: pd.DataFrame, applications: pd.DataFrame, customers: pd.DataFrame
) -> pd.DataFrame:
    app_customer = applications[["application_id", "customer_id"]]
    decisions_with_segment = underwriting_decisions.merge(app_customer, on="application_id", how="inner").merge(
        customers[["customer_id", "risk_segment"]], on="customer_id", how="inner"
    )

    by_risk_segment = _breakdown(decisions_with_segment, "risk_segment", "risk_segment")
    by_model_version = _breakdown(underwriting_decisions, "model_version", "model_version")
    return pd.concat([by_risk_segment, by_model_version], ignore_index=True)


def _independent_rejection_distribution(underwriting_decisions: pd.DataFrame) -> pd.DataFrame:
    rejected = underwriting_decisions[underwriting_decisions["decision"] == "REJECTED"]
    counts = rejected.groupby("rejection_reason").size().reset_index(name="count")
    return counts


NUMERIC_COLUMNS = [
    "decision_count", "approved_count", "rejected_count", "manual_review_count",
    "approval_rate", "avg_approved_amount", "avg_approved_apr",
]


def validate_underwriting_performance(storage: S3Storage, validation_rules: dict) -> dict:
    underwriting_decisions = storage.read_parquet("raw/underwriting_decisions.parquet")
    applications = storage.read_parquet("raw/applications.parquet")
    customers = storage.read_parquet("raw/customers.parquet")
    curated = storage.read_parquet("curated/underwriting_performance.parquet")
    curated_rejections = storage.read_parquet("curated/underwriting_performance_rejections.parquet")

    expected = _independent_underwriting_performance(underwriting_decisions, applications, customers)
    expected_rejections = _independent_rejection_distribution(underwriting_decisions)

    rules = {rule["id"]: rule for rule in validation_rules["rules"]}
    tolerances = validation_rules["tolerance"]
    tolerance_type_by_column = {
        "decision_count": "count",
        "approved_count": "count",
        "rejected_count": "count",
        "manual_review_count": "count",
        "approval_rate": "rate",
        "avg_approved_amount": "currency",
        "avg_approved_apr": "rate",
    }

    checks = []
    expected_by_key = {(r["breakdown_type"], r["breakdown_value"]): r for _, r in expected.iterrows()}
    curated_by_key = {(r["breakdown_type"], r["breakdown_value"]): r for _, r in curated.iterrows()}

    mismatched = []
    for key, expected_row in expected_by_key.items():
        actual_row = curated_by_key.get(key)
        if actual_row is None:
            mismatched.append(key)
            continue
        for column in NUMERIC_COLUMNS:
            expected_value, actual_value = expected_row[column], actual_row[column]
            if pd.isna(expected_value) and pd.isna(actual_value):
                continue
            tol = tolerances[tolerance_type_by_column[column]]
            if abs(float(actual_value) - float(expected_value)) > tol:
                mismatched.append(key)
                break

    rule = rules["underwriting_performance_breakdown_rows_match"]
    checks.append(
        {
            "id": rule["id"],
            "description": rule["description"],
            "status": "PASS" if not mismatched else "FAIL",
            "expected": 0,
            "actual": len(mismatched),
            "difference": len(mismatched),
            "details": f"mismatched breakdown keys: {mismatched}" if mismatched else None,
        }
    )

    expected_rejection_map = dict(zip(expected_rejections["rejection_reason"], expected_rejections["count"]))
    actual_rejection_map = dict(zip(curated_rejections["rejection_reason"], curated_rejections["count"]))
    rejection_rule = rules["underwriting_performance_rejection_distribution_matches"]
    rejection_mismatch = expected_rejection_map != actual_rejection_map
    checks.append(
        {
            "id": rejection_rule["id"],
            "description": rejection_rule["description"],
            "status": "FAIL" if rejection_mismatch else "PASS",
            "expected": expected_rejection_map,
            "actual": actual_rejection_map,
            "difference": None,
            "details": None if not rejection_mismatch else "rejection distributions differ",
        }
    )

    failed = [c for c in checks if c["status"] == "FAIL"]
    return {
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def main(argv: list[str] | None = None) -> None:
    storage = S3Storage()
    validation_rules = storage.read_json("context/validations/underwriting_performance.json")
    results = validate_underwriting_performance(storage, validation_rules)
    storage.write_json("curated/underwriting_performance_validation_results.json", results)

    print(f"overall_status: {results['overall_status']}")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"  [{marker}] {check['id']}")

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
