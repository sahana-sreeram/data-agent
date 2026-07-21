"""Independent (pandas, not Spark) validation of the delinquency_default curated table.
Deliberately does NOT import src.etl_spark_delinquency_default.
"""

from __future__ import annotations

import pandas as pd

from src.storage import S3Storage

OVERALL_LABEL = "ALL"


def _metrics_for_group(loans: pd.DataFrame, delinquency_events: pd.DataFrame, defaults: pd.DataFrame, business_rules: dict) -> dict:
    loan_count = len(loans)
    total_funded_principal = round(float(loans["principal_amount"].sum()), 2)

    loan_ids = set(loans["loan_id"])
    delinquent_loan_ids = set(delinquency_events["loan_id"]) & loan_ids
    delinquent_loan_count = len(delinquent_loan_ids)

    group_defaults = defaults[defaults["loan_id"].isin(loan_ids)]
    default_count = int(len(group_defaults))
    total_balance_at_default = round(float(group_defaults["balance_at_default"].sum()), 2)
    total_recovery_amount = round(float(group_defaults["recovery_amount"].sum()), 2)

    delinquency_rate = round(delinquent_loan_count / loan_count, 4) if loan_count else None
    default_rate = round(default_count / loan_count, 4) if loan_count else None
    recovery_rate = (
        round(total_recovery_amount / total_balance_at_default, 4) if total_balance_at_default > 0 else None
    )

    denominator = {"total_funded_principal": total_funded_principal, "total_balance_at_default": total_balance_at_default}[
        business_rules["loss_rate_denominator"]
    ]
    loss_rate = round((total_balance_at_default - total_recovery_amount) / denominator, 4) if denominator > 0 else None

    return {
        "loan_count": loan_count,
        "total_funded_principal": total_funded_principal,
        "delinquent_loan_count": delinquent_loan_count,
        "delinquency_rate": delinquency_rate,
        "default_count": default_count,
        "default_rate": default_rate,
        "total_balance_at_default": total_balance_at_default,
        "total_recovery_amount": total_recovery_amount,
        "recovery_rate": recovery_rate,
        "loss_rate": loss_rate,
    }


def _independent_delinquency_default(
    loans: pd.DataFrame, customers: pd.DataFrame, delinquency_events: pd.DataFrame, defaults: pd.DataFrame, business_rules: dict
) -> pd.DataFrame:
    loans_with_segment = loans.merge(customers[["customer_id", "risk_segment"]], on="customer_id", how="inner")

    rows = [{"breakdown_value": OVERALL_LABEL, **_metrics_for_group(loans_with_segment, delinquency_events, defaults, business_rules)}]
    for segment, group in loans_with_segment.groupby("risk_segment"):
        rows.append({"breakdown_value": segment, **_metrics_for_group(group, delinquency_events, defaults, business_rules)})

    return pd.DataFrame(rows)


NUMERIC_COLUMNS = [
    "loan_count", "total_funded_principal", "delinquent_loan_count", "delinquency_rate",
    "default_count", "default_rate", "total_balance_at_default", "total_recovery_amount",
    "recovery_rate", "loss_rate",
]

# Explicit, not guessed from the column name -- a substring heuristic
# ("amount" in column or "principal" in column) previously missed
# total_balance_at_default (contains neither word), silently giving it the
# strict count tolerance (0) instead of the intended currency tolerance.
TOLERANCE_TYPE_BY_COLUMN = {
    "loan_count": "count",
    "total_funded_principal": "currency",
    "delinquent_loan_count": "count",
    "delinquency_rate": "rate",
    "default_count": "count",
    "default_rate": "rate",
    "total_balance_at_default": "currency",
    "total_recovery_amount": "currency",
    "recovery_rate": "rate",
    "loss_rate": "rate",
}


def validate_delinquency_default(storage: S3Storage, business_rules: dict, validation_rules: dict) -> dict:
    loans = storage.read_parquet("raw/loans.parquet")
    customers = storage.read_parquet("raw/customers.parquet")
    delinquency_events = storage.read_parquet("raw/delinquency_events.parquet")
    defaults = storage.read_parquet("raw/defaults.parquet")
    curated = storage.read_parquet("curated/delinquency_default.parquet")

    expected = _independent_delinquency_default(loans, customers, delinquency_events, defaults, business_rules)

    rules = {rule["id"]: rule for rule in validation_rules["rules"]}
    tolerances = validation_rules["tolerance"]

    expected_by_value = {r["breakdown_value"]: r for _, r in expected.iterrows()}
    curated_by_value = {r["breakdown_value"]: r for _, r in curated.iterrows()}

    mismatched = []
    for value, expected_row in expected_by_value.items():
        actual_row = curated_by_value.get(value)
        if actual_row is None:
            mismatched.append(value)
            continue
        for column in NUMERIC_COLUMNS:
            expected_value, actual_value = expected_row[column], actual_row[column]
            expected_missing, actual_missing = pd.isna(expected_value), pd.isna(actual_value)
            if expected_missing and actual_missing:
                continue
            if expected_missing != actual_missing:
                # Exactly one side is undefined (e.g. recovery_rate is null with zero
                # defaults on one side but a real number on the other) -- a real
                # mismatch, not something to silently let a NaN-vs-number comparison
                # (always False) pass as a match.
                mismatched.append(value)
                break
            tolerance = tolerances[TOLERANCE_TYPE_BY_COLUMN[column]]
            if abs(float(actual_value) - float(expected_value)) > tolerance:
                mismatched.append(value)
                break

    rule = rules["delinquency_default_breakdown_rows_match"]
    check = {
        "id": rule["id"],
        "description": rule["description"],
        "status": "PASS" if not mismatched else "FAIL",
        "expected": 0,
        "actual": len(mismatched),
        "difference": len(mismatched),
        "details": f"mismatched breakdown_values: {mismatched}" if mismatched else None,
    }

    failed = [check] if check["status"] == "FAIL" else []
    return {
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": 1,
        "failed_check_count": len(failed),
        "checks": [check],
    }


def main(argv: list[str] | None = None) -> None:
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json("context/validations/delinquency_default.json")

    results = validate_delinquency_default(storage, business_rules, validation_rules)
    storage.write_json("curated/delinquency_default_validation_results.json", results)

    print(f"overall_status: {results['overall_status']}")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"  [{marker}] {check['id']}  details={check['details']}")

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
