"""Independent validation of the 12-table lifecycle raw dataset (data/lifecycle/raw/).

Every check here operates ONLY on the raw tables themselves: required columns
present, enum columns within the approved vocabulary (context/business_rules.json),
and every foreign key resolving to an existing row in the referenced table. There
are no reconciliation checks -- those compare a curated/ETL output against raw
data, and no curated output exists yet for this model (see
src/etl_campaign_funnel.py et al., not yet built).

This mirrors src/validate_portfolio.py's pattern (independent recomputation,
rules/tolerances loaded from context/ but the actual check logic lives here) but
is a fully separate module: it does not import from or modify validate_portfolio.py,
and nothing here touches data/raw/ or the 3 existing scenarios.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

DEFAULT_RAW_DIR = "data/lifecycle/raw"
DEFAULT_OUTPUT_DIR = "data/lifecycle/processed"
DEFAULT_BUSINESS_RULES_FILE = "context/business_rules.json"
DEFAULT_VALIDATION_RULES_FILE = "context/validations/lifecycle_raw.json"

TABLE_FILENAMES: dict[str, str] = {
    "customers": "customers.json",
    "campaigns": "campaigns.json",
    "coupon_rules": "coupon_rules.json",
    "email_events": "email_events.json",
    "prequal_offers": "prequal_offers.json",
    "applications": "applications.json",
    "underwriting_decisions": "underwriting_decisions.json",
    "loans": "loans.json",
    "payment_schedule": "payment_schedule.json",
    "payment_events": "payment_events.json",
    "delinquency_events": "delinquency_events.json",
    "defaults": "defaults.json",
}

TABLE_REQUIRED_COLUMNS: dict[str, set[str]] = {
    "customers": {"customer_id", "created_at", "state", "income_band", "credit_score_band", "credit_score", "risk_segment"},
    "campaigns": {"campaign_id", "name", "channel", "start_date", "end_date", "target_risk_segment"},
    "coupon_rules": {"coupon_rule_id", "coupon_code", "campaign_id", "discount_type", "discount_value", "valid_from", "valid_to"},
    "email_events": {"event_id", "campaign_id", "customer_id", "event_type", "event_timestamp"},
    "prequal_offers": {"offer_id", "customer_id", "campaign_id", "coupon_code", "offer_amount", "offer_apr", "created_at", "expires_at"},
    "applications": {"application_id", "customer_id", "offer_id", "requested_amount", "submitted_at", "application_status"},
    "underwriting_decisions": {
        "decision_id", "application_id", "decision", "rejection_reason",
        "approved_amount", "approved_apr", "model_version", "decided_at",
    },
    "loans": {
        "loan_id", "application_id", "customer_id", "principal_amount", "interest_rate",
        "term_months", "originated_at", "loan_status", "scheduled_payment_amount",
    },
    "payment_schedule": {"schedule_id", "loan_id", "installment_number", "due_date", "scheduled_amount"},
    "payment_events": {"event_id", "schedule_id", "loan_id", "event_type", "payment_date", "amount", "payment_status", "payment_method"},
    "delinquency_events": {"delinquency_id", "loan_id", "as_of_date", "days_past_due", "bucket"},
    "defaults": {"default_id", "loan_id", "default_date", "balance_at_default", "recovery_amount", "recovery_date"},
}

# (table, column, business_rules key holding the valid vocabulary)
TABLE_ENUM_COLUMNS: list[tuple[str, str, str]] = [
    ("campaigns", "channel", "valid_channels"),
    ("coupon_rules", "discount_type", "valid_discount_types"),
    ("email_events", "event_type", "valid_email_event_types"),
    ("applications", "application_status", "valid_application_statuses"),
    ("underwriting_decisions", "decision", "valid_decision_values"),
    ("underwriting_decisions", "rejection_reason", "valid_rejection_reasons"),
    ("loans", "loan_status", "valid_loan_statuses"),
    ("payment_events", "event_type", "valid_payment_event_types"),
    ("payment_events", "payment_status", "valid_payment_event_statuses"),
    ("delinquency_events", "bucket", "valid_delinquency_buckets"),
]

# (from_table, from_column, to_table, to_column) -- from_column may contain nulls
# (e.g. prequal_offers.campaign_id for an organic offer); nulls are skipped, not
# treated as orphans.
TABLE_FOREIGN_KEYS: list[tuple[str, str, str, str]] = [
    ("coupon_rules", "campaign_id", "campaigns", "campaign_id"),
    ("email_events", "campaign_id", "campaigns", "campaign_id"),
    ("email_events", "customer_id", "customers", "customer_id"),
    ("prequal_offers", "customer_id", "customers", "customer_id"),
    ("prequal_offers", "campaign_id", "campaigns", "campaign_id"),
    ("applications", "customer_id", "customers", "customer_id"),
    ("applications", "offer_id", "prequal_offers", "offer_id"),
    ("underwriting_decisions", "application_id", "applications", "application_id"),
    ("loans", "application_id", "applications", "application_id"),
    ("loans", "customer_id", "customers", "customer_id"),
    ("payment_schedule", "loan_id", "loans", "loan_id"),
    ("payment_events", "loan_id", "loans", "loan_id"),
    ("payment_events", "schedule_id", "payment_schedule", "schedule_id"),
    ("delinquency_events", "loan_id", "loans", "loan_id"),
    ("defaults", "loan_id", "loans", "loan_id"),
]


class LifecycleValidationError(Exception):
    """Application-level failure: missing/malformed input files."""


def _load_table(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise LifecycleValidationError(f"{label} file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise LifecycleValidationError(f"{label} file must contain a JSON array: {path}")
    return pd.DataFrame(records)


def load_lifecycle_tables(raw_dir: Path) -> dict[str, pd.DataFrame]:
    """Load all 12 lifecycle raw tables from raw_dir into a {table_name: DataFrame} dict."""
    return {name: _load_table(raw_dir / filename, name) for name, filename in TABLE_FILENAMES.items()}


def load_business_rules(path: Path) -> dict:
    if not path.exists():
        raise LifecycleValidationError(f"business rules file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_validation_rules(path: Path) -> dict:
    if not path.exists():
        raise LifecycleValidationError(f"validation rules file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _rules_by_id(validation_rules: dict) -> dict:
    return {rule["id"]: rule for rule in validation_rules["rules"]}


def _schema_check(rule: dict, df: pd.DataFrame, required: set[str]) -> dict:
    if df.empty:
        missing: list[str] = []
    else:
        missing = sorted(required - set(df.columns))
    status = "PASS" if not missing else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": sorted(required),
        "actual": sorted(set(df.columns)),
        "difference": None,
        "details": f"missing columns: {missing}" if missing else None,
    }


def _enum_check(rule: dict, df: pd.DataFrame, column: str, valid_values: list[str]) -> dict:
    if df.empty or column not in df.columns:
        observed: set = set()
    else:
        observed = set(df[column].dropna().unique())
    invalid = sorted(observed - set(valid_values))
    status = "PASS" if not invalid else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": sorted(valid_values),
        "actual": sorted(observed),
        "difference": None,
        "details": f"unexpected values found: {invalid}" if invalid else None,
    }


def _referential_integrity_check(
    rule: dict, from_df: pd.DataFrame, from_column: str, to_df: pd.DataFrame, to_key_column: str
) -> dict:
    if from_df.empty or from_column not in from_df.columns:
        orphans: list = []
    else:
        known_keys = set(to_df[to_key_column]) if (not to_df.empty and to_key_column in to_df.columns) else set()
        non_null = from_df[from_df[from_column].notna()]
        orphan_mask = ~non_null[from_column].isin(known_keys)
        orphans = sorted(non_null.loc[orphan_mask, from_column].unique().tolist())
    status = "PASS" if not orphans else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": 0,
        "actual": len(orphans),
        "difference": len(orphans),
        "details": f"unresolved {from_column} values: {orphans}" if orphans else None,
    }


def validate_lifecycle_raw(tables: dict[str, pd.DataFrame], business_rules: dict, validation_rules: dict) -> dict:
    """Run schema, enum, and referential-integrity checks across all 12 lifecycle raw tables."""
    rules = _rules_by_id(validation_rules)
    checks = []

    for table_name, required_columns in TABLE_REQUIRED_COLUMNS.items():
        rule_id = f"{table_name}_required_columns_present"
        checks.append(_schema_check(rules[rule_id], tables[table_name], required_columns))

    for table_name, column, business_rules_key in TABLE_ENUM_COLUMNS:
        rule_id = f"{table_name}_{column}_enum_valid"
        checks.append(_enum_check(rules[rule_id], tables[table_name], column, business_rules[business_rules_key]))

    for from_table, from_column, to_table, to_column in TABLE_FOREIGN_KEYS:
        rule_id = f"{from_table}_{from_column}_references_{to_table}"
        checks.append(
            _referential_integrity_check(rules[rule_id], tables[from_table], from_column, tables[to_table], to_column)
        )

    failed = [c for c in checks if c["status"] == "FAIL"]
    return {
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def write_validation_results(path: Path, results: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
        f.write("\n")


def print_validation_results(results: dict) -> None:
    print("Lifecycle raw validation results")
    print(f"  overall_status:  {results['overall_status']}")
    print(f"  checks:          {results['total_check_count']} total, {results['failed_check_count']} failed")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"    [{marker}] {check['id']}")
        if check["status"] == "FAIL":
            print(f"           expected={check['expected']} actual={check['actual']} details={check['details']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the 12-table lifecycle raw dataset.")
    parser.add_argument("--raw-dir", type=str, default=DEFAULT_RAW_DIR)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--business-rules-file", type=str, default=DEFAULT_BUSINESS_RULES_FILE)
    parser.add_argument("--validation-rules-file", type=str, default=DEFAULT_VALIDATION_RULES_FILE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    tables = load_lifecycle_tables(Path(args.raw_dir))
    business_rules = load_business_rules(Path(args.business_rules_file))
    validation_rules = load_validation_rules(Path(args.validation_rules_file))

    results = validate_lifecycle_raw(tables, business_rules, validation_rules)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_validation_results(output_dir / "lifecycle_raw_validation_results.json", results)

    print_validation_results(results)

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
