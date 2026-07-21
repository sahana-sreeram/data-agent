"""Independent validation of the portfolio summary against raw loans/payments.

Every metric here is recomputed directly from data/raw/loans.json and
data/raw/payments.json using this module's own logic, then compared against
data/processed/portfolio_summary.json. This deliberately does NOT re-import
src.transform.compute_portfolio_summary: reusing the exact same calculation
to "validate" itself would let a bug in that calculation pass unnoticed.

Rule descriptions and tolerances are loaded from context/validation_rules.json,
and which payment statuses count as successful/valid comes from
context/business_rules.json -- both are executed here, not just documented.

This module intentionally does not repair anything, call any agent, or model
the future PAID->SETTLED bug -- see README.md for scope.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.transform import (
    REQUIRED_LOAN_COLUMNS,
    REQUIRED_PAYMENT_COLUMNS,
    REQUIRED_PAYMENT_EVENT_COLUMNS,
    load_business_rules,
    load_loans,
    load_payment_events,
    load_payments,
)

DEFAULT_LOANS_FILE = "data/raw/loans.json"
DEFAULT_PAYMENTS_FILE = "data/raw/payments.json"
DEFAULT_SUMMARY_FILE = "data/processed/portfolio_summary.json"
DEFAULT_OUTPUT_DIR = "data/processed"
DEFAULT_BUSINESS_RULES_FILE = "context/business_rules.json"
DEFAULT_VALIDATION_RULES_FILE = "context/validation_rules.json"


def load_summary(path: Path) -> dict:
    """Load the ETL's portfolio_summary.json (a single JSON object)."""
    if not path.exists():
        raise FileNotFoundError(f"portfolio summary file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"portfolio summary file must contain a JSON object: {path}")
    return data


def load_validation_rules(path: Path) -> dict:
    """Load the validation rule definitions and tolerances."""
    if not path.exists():
        raise FileNotFoundError(f"validation rules file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _rules_by_id(validation_rules: dict) -> dict:
    return {rule["id"]: rule for rule in validation_rules["rules"]}


def _tolerance_for(rule: dict, tolerances: dict) -> float:
    return tolerances[rule["tolerance_type"]]


def _reconciliation_check(rule: dict, tolerances: dict, expected, actual) -> dict:
    expected = round(float(expected), 2)
    actual = round(float(actual), 2)
    difference = round(actual - expected, 2)
    tolerance = _tolerance_for(rule, tolerances)
    status = "PASS" if abs(difference) <= tolerance else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": expected,
        "actual": actual,
        "difference": difference,
        "details": None,
    }


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
    observed = set(df[column].unique()) if not df.empty else set()
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


def _skipped_check(rule: dict, reason: str) -> dict:
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": "FAIL",
        "expected": None,
        "actual": None,
        "difference": None,
        "details": reason,
    }


def _referential_integrity_check(rule: dict, loans_df: pd.DataFrame, payments_df: pd.DataFrame) -> dict:
    if payments_df.empty:
        orphans: list[str] = []
    else:
        known_loan_ids = set(loans_df["loan_id"]) if not loans_df.empty else set()
        orphan_mask = ~payments_df["loan_id"].isin(known_loan_ids)
        orphans = sorted(payments_df.loc[orphan_mask, "payment_id"].tolist())
    status = "PASS" if not orphans else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": 0,
        "actual": len(orphans),
        "difference": len(orphans),
        "details": f"orphaned payment_ids: {orphans}" if orphans else None,
    }


def _referential_integrity_check_events(
    rule: dict, loans_df: pd.DataFrame, payment_events_df: pd.DataFrame
) -> dict:
    if payment_events_df.empty:
        orphans: list[str] = []
    else:
        known_loan_ids = set(loans_df["loan_id"]) if not loans_df.empty else set()
        orphan_mask = ~payment_events_df["loan_id"].isin(known_loan_ids)
        orphans = sorted(payment_events_df.loc[orphan_mask, "event_id"].tolist())
    status = "PASS" if not orphans else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": 0,
        "actual": len(orphans),
        "difference": len(orphans),
        "details": f"orphaned event_ids: {orphans}" if orphans else None,
    }


def validate_portfolio_from_payment_events(
    loans_df: pd.DataFrame,
    payment_events_df: pd.DataFrame,
    summary: dict,
    business_rules: dict,
    validation_rules: dict,
) -> dict:
    """Independently validate a portfolio summary computed from a payment-events
    stream, at logical-payment (entity) grain rather than event-row grain.

    Distinguishes two different things that are easy to conflate:
      A. Duplicate SETTLED events being PRESENT -- expected under
         at-least-once delivery, reported as a WARNING (never fails
         overall_status).
      B. The ETL's OUTPUT failing to reconcile to one-count-per-payment
         truth -- a hard FAILure, regardless of why the mismatch exists.

    Conflicting SETTLED amounts for the same payment_id (ambiguous: partial
    payment vs. correction vs. data error) are never silently resolved by
    picking one -- they always fail validation instead of being collapsed.
    """
    rules = _rules_by_id(validation_rules)
    tolerances = validation_rules["tolerance"]
    event_rules = business_rules["payment_event_rules"]
    successful_terminal_event = event_rules["successful_terminal_event"]
    valid_event_types = business_rules["valid_payment_event_types"]

    loans_schema_check = _schema_check(rules["loans_required_columns_present"], loans_df, REQUIRED_LOAN_COLUMNS)
    events_schema_check = _schema_check(
        rules["payment_events_required_columns_present"], payment_events_df, REQUIRED_PAYMENT_EVENT_COLUMNS
    )
    loans_schema_ok = loans_schema_check["status"] == "PASS"
    events_schema_ok = events_schema_check["status"] == "PASS"

    checks = [loans_schema_check, events_schema_check]

    if not (loans_schema_ok and events_schema_ok):
        skip_reason = "skipped: a required-column check above failed"
        remaining_rule_ids = [
            "payment_event_type_enum_valid",
            "payment_event_loan_referential_integrity",
            "duplicate_settled_events_present",
            "conflicting_settled_amounts",
            "loan_count_reconciliation",
            "active_loan_count_reconciliation",
            "closed_loan_count_reconciliation",
            "defaulted_loan_count_reconciliation",
            "total_original_principal_reconciliation",
            "successful_payment_count_reconciliation",
            "total_successful_payments_reconciliation",
            "total_outstanding_balance_reconciliation",
        ]
        checks.extend(_skipped_check(rules[rule_id], skip_reason) for rule_id in remaining_rule_ids)
        failed = [c for c in checks if c["status"] == "FAIL"]
        return {
            "as_of_date": summary.get("as_of_date"),
            "overall_status": "FAIL",
            "total_check_count": len(checks),
            "failed_check_count": len(failed),
            "checks": checks,
        }

    checks.append(
        _enum_check(rules["payment_event_type_enum_valid"], payment_events_df, "event_type", valid_event_types)
    )
    checks.append(
        _referential_integrity_check_events(rules["payment_event_loan_referential_integrity"], loans_df, payment_events_df)
    )

    settled = (
        payment_events_df[payment_events_df["event_type"] == successful_terminal_event]
        if not payment_events_df.empty
        else payment_events_df
    )

    # A. Duplicate presence -- informational only, WARNING, never fails overall_status.
    if settled.empty:
        duplicated_payment_ids: list[str] = []
    else:
        settled_counts_by_payment = settled.groupby("payment_id").size()
        duplicated_payment_ids = sorted(settled_counts_by_payment[settled_counts_by_payment > 1].index.tolist())
    duplicate_rule = rules["duplicate_settled_events_present"]
    checks.append(
        {
            "id": duplicate_rule["id"],
            "description": duplicate_rule["description"],
            "status": "WARNING" if duplicated_payment_ids else "PASS",
            "expected": 0,
            "actual": len(duplicated_payment_ids),
            "difference": len(duplicated_payment_ids),
            "details": f"payment_ids with multiple SETTLED events: {duplicated_payment_ids}"
            if duplicated_payment_ids
            else None,
        }
    )

    # Conflicting amounts -- hard FAIL, never silently resolved by picking one.
    if settled.empty:
        conflicting_payment_ids: list[str] = []
    else:
        distinct_amounts = settled.groupby("payment_id")["amount"].nunique()
        conflicting_payment_ids = sorted(distinct_amounts[distinct_amounts > 1].index.tolist())
    conflict_rule = rules["conflicting_settled_amounts"]
    checks.append(
        {
            "id": conflict_rule["id"],
            "description": conflict_rule["description"],
            "status": "FAIL" if conflicting_payment_ids else "PASS",
            "expected": 0,
            "actual": len(conflicting_payment_ids),
            "difference": len(conflicting_payment_ids),
            "details": f"payment_ids with conflicting SETTLED amounts: {conflicting_payment_ids}"
            if conflicting_payment_ids
            else None,
        }
    )

    # B. Entity-grain truth: collapse to the latest SETTLED event per
    # payment_id (by event_timestamp), excluding payment_ids with
    # conflicting amounts -- those can't be safely collapsed and are
    # already flagged as a hard failure above.
    safe_settled = settled[~settled["payment_id"].isin(conflicting_payment_ids)] if not settled.empty else settled
    if safe_settled.empty:
        collapsed = safe_settled
    else:
        collapsed = safe_settled.sort_values(["payment_id", "event_timestamp"]).drop_duplicates(
            subset=["payment_id"], keep="last"
        )

    expected_successful_payment_count = int(len(collapsed))
    expected_total_successful_payments = round(float(collapsed["amount"].sum()), 2) if not collapsed.empty else 0.0
    expected_total_original_principal = (
        round(float(loans_df["principal_amount"].sum()), 2) if not loans_df.empty else 0.0
    )
    expected_total_outstanding_balance = round(
        expected_total_original_principal - expected_total_successful_payments, 2
    )
    expected_active = int((loans_df["loan_status"] == "ACTIVE").sum()) if not loans_df.empty else 0
    expected_closed = int((loans_df["loan_status"] == "CLOSED").sum()) if not loans_df.empty else 0
    expected_defaulted = int((loans_df["loan_status"] == "DEFAULTED").sum()) if not loans_df.empty else 0

    checks.extend(
        [
            _reconciliation_check(
                rules["loan_count_reconciliation"], tolerances, len(loans_df), summary.get("loan_count", 0)
            ),
            _reconciliation_check(
                rules["active_loan_count_reconciliation"], tolerances, expected_active, summary.get("active_loan_count", 0)
            ),
            _reconciliation_check(
                rules["closed_loan_count_reconciliation"], tolerances, expected_closed, summary.get("closed_loan_count", 0)
            ),
            _reconciliation_check(
                rules["defaulted_loan_count_reconciliation"],
                tolerances,
                expected_defaulted,
                summary.get("defaulted_loan_count", 0),
            ),
            _reconciliation_check(
                rules["total_original_principal_reconciliation"],
                tolerances,
                expected_total_original_principal,
                summary.get("total_original_principal", 0.0),
            ),
            _reconciliation_check(
                rules["successful_payment_count_reconciliation"],
                tolerances,
                expected_successful_payment_count,
                summary.get("successful_payment_count", 0),
            ),
            _reconciliation_check(
                rules["total_successful_payments_reconciliation"],
                tolerances,
                expected_total_successful_payments,
                summary.get("total_successful_payments", 0.0),
            ),
            _reconciliation_check(
                rules["total_outstanding_balance_reconciliation"],
                tolerances,
                expected_total_outstanding_balance,
                summary.get("total_outstanding_balance", 0.0),
            ),
        ]
    )

    failed = [check for check in checks if check["status"] == "FAIL"]
    return {
        "as_of_date": summary.get("as_of_date"),
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def validate_portfolio(
    loans_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    summary: dict,
    business_rules: dict,
    validation_rules: dict,
) -> dict:
    """Independently recompute portfolio metrics from raw data and compare
    them against the ETL's reported portfolio summary.
    """
    rules = _rules_by_id(validation_rules)
    tolerances = validation_rules["tolerance"]
    success_statuses = business_rules["successful_payment_statuses"]
    valid_payment_statuses = business_rules["valid_payment_statuses"]
    valid_loan_statuses = business_rules["valid_loan_statuses"]

    loans_schema_check = _schema_check(rules["loans_required_columns_present"], loans_df, REQUIRED_LOAN_COLUMNS)
    payments_schema_check = _schema_check(
        rules["payments_required_columns_present"], payments_df, REQUIRED_PAYMENT_COLUMNS
    )
    loans_schema_ok = loans_schema_check["status"] == "PASS"
    payments_schema_ok = payments_schema_check["status"] == "PASS"

    checks = [loans_schema_check, payments_schema_check]

    # Every remaining check reads columns the schema checks above just
    # confirmed exist. If a required column is missing, skip straight to a
    # FAIL rather than letting a KeyError crash the whole validation run.
    if not (loans_schema_ok and payments_schema_ok):
        skip_reason = "skipped: a required-column check above failed"
        remaining_rule_ids = [
            "payment_status_enum_valid",
            "loan_status_enum_valid",
            "payment_loan_referential_integrity",
            "loan_count_reconciliation",
            "payment_count_reconciliation",
            "successful_payment_count_reconciliation",
            "active_loan_count_reconciliation",
            "closed_loan_count_reconciliation",
            "defaulted_loan_count_reconciliation",
            "total_original_principal_reconciliation",
            "total_successful_payments_reconciliation",
            "total_outstanding_balance_reconciliation",
        ]
        checks.extend(_skipped_check(rules[rule_id], skip_reason) for rule_id in remaining_rule_ids)
        failed = [check for check in checks if check["status"] == "FAIL"]
        return {
            "as_of_date": summary.get("as_of_date"),
            "overall_status": "FAIL",
            "total_check_count": len(checks),
            "failed_check_count": len(failed),
            "checks": checks,
        }

    checks.append(_enum_check(rules["payment_status_enum_valid"], payments_df, "payment_status", valid_payment_statuses))
    checks.append(_enum_check(rules["loan_status_enum_valid"], loans_df, "loan_status", valid_loan_statuses))
    checks.append(_referential_integrity_check(rules["payment_loan_referential_integrity"], loans_df, payments_df))

    if payments_df.empty:
        expected_successful_payment_count = 0
        expected_total_successful_payments = 0.0
    else:
        successful_mask = payments_df["payment_status"].isin(success_statuses)
        expected_successful_payment_count = int(successful_mask.sum())
        expected_total_successful_payments = round(float(payments_df.loc[successful_mask, "amount_paid"].sum()), 2)

    expected_total_original_principal = (
        round(float(loans_df["principal_amount"].sum()), 2) if not loans_df.empty else 0.0
    )
    expected_total_outstanding_balance = round(
        expected_total_original_principal - expected_total_successful_payments, 2
    )
    expected_active = int((loans_df["loan_status"] == "ACTIVE").sum()) if not loans_df.empty else 0
    expected_closed = int((loans_df["loan_status"] == "CLOSED").sum()) if not loans_df.empty else 0
    expected_defaulted = int((loans_df["loan_status"] == "DEFAULTED").sum()) if not loans_df.empty else 0

    checks.extend(
        [
            _reconciliation_check(rules["loan_count_reconciliation"], tolerances, len(loans_df), summary.get("loan_count", 0)),
            _reconciliation_check(rules["payment_count_reconciliation"], tolerances, len(payments_df), summary.get("payment_count", 0)),
            _reconciliation_check(
                rules["successful_payment_count_reconciliation"],
                tolerances,
                expected_successful_payment_count,
                summary.get("successful_payment_count", 0),
            ),
            _reconciliation_check(rules["active_loan_count_reconciliation"], tolerances, expected_active, summary.get("active_loan_count", 0)),
            _reconciliation_check(rules["closed_loan_count_reconciliation"], tolerances, expected_closed, summary.get("closed_loan_count", 0)),
            _reconciliation_check(
                rules["defaulted_loan_count_reconciliation"], tolerances, expected_defaulted, summary.get("defaulted_loan_count", 0)
            ),
            _reconciliation_check(
                rules["total_original_principal_reconciliation"],
                tolerances,
                expected_total_original_principal,
                summary.get("total_original_principal", 0.0),
            ),
            _reconciliation_check(
                rules["total_successful_payments_reconciliation"],
                tolerances,
                expected_total_successful_payments,
                summary.get("total_successful_payments", 0.0),
            ),
            _reconciliation_check(
                rules["total_outstanding_balance_reconciliation"],
                tolerances,
                expected_total_outstanding_balance,
                summary.get("total_outstanding_balance", 0.0),
            ),
        ]
    )

    failed = [check for check in checks if check["status"] == "FAIL"]
    return {
        "as_of_date": summary.get("as_of_date"),
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def _loans_without_payments_check(rule: dict, loans_df: pd.DataFrame, payments_df: pd.DataFrame) -> dict:
    """Informational: loans with zero payment records at all (e.g. newly originated loans
    whose first payment is not yet due). Never a FAIL by itself -- see
    loan_count/total_original_principal/total_outstanding_balance reconciliation for whether
    the ETL correctly included them anyway."""
    if loans_df.empty:
        missing_loan_ids: list[str] = []
    else:
        known_payment_loan_ids = set(payments_df["loan_id"]) if not payments_df.empty else set()
        missing_mask = ~loans_df["loan_id"].isin(known_payment_loan_ids)
        missing_loan_ids = sorted(loans_df.loc[missing_mask, "loan_id"].tolist())

    total_principal = (
        round(float(loans_df.loc[loans_df["loan_id"].isin(missing_loan_ids), "principal_amount"].sum()), 2)
        if missing_loan_ids
        else 0.0
    )
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": "WARNING" if missing_loan_ids else "PASS",
        "expected": 0,
        "actual": len(missing_loan_ids),
        "difference": len(missing_loan_ids),
        "details": (
            f"loan_ids with no payment records: {missing_loan_ids}; total principal: {total_principal}"
            if missing_loan_ids
            else None
        ),
    }


def validate_portfolio_with_join_profile(
    loans_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    summary: dict,
    business_rules: dict,
    validation_rules: dict,
) -> dict:
    """validate_portfolio() plus one additional informational check: loans with zero payment
    records. Never affects overall_status by itself (WARNING only) -- exists to give the
    diagnosis agent a direct, generic signal that some loans may be excluded upstream of the
    reconciliation numbers, without validate_portfolio() itself needing to know about joins.
    """
    base = validate_portfolio(loans_df, payments_df, summary, business_rules, validation_rules)
    rules = _rules_by_id(validation_rules)
    profile_check = _loans_without_payments_check(rules["loans_without_payment_records_present"], loans_df, payments_df)
    checks = [*base["checks"], profile_check]
    failed = [c for c in checks if c["status"] == "FAIL"]
    return {
        "as_of_date": base["as_of_date"],
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def write_validation_results(path: Path, results: dict) -> None:
    """Write the validation results as a single JSON object with a trailing newline."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
        f.write("\n")


def print_validation_results(results: dict) -> None:
    """Print a human-readable rendering of the validation results."""
    print("Validation results")
    print(f"  as_of_date:      {results['as_of_date']}")
    print(f"  overall_status:  {results['overall_status']}")
    print(f"  checks:          {results['total_check_count']} total, {results['failed_check_count']} failed")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"    [{marker}] {check['id']}")
        if check["status"] == "FAIL":
            print(f"           expected={check['expected']} actual={check['actual']} details={check['details']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the portfolio summary against raw loans/payments.")
    parser.add_argument("--loans-file", type=str, default=DEFAULT_LOANS_FILE)
    parser.add_argument("--payments-file", type=str, default=DEFAULT_PAYMENTS_FILE)
    parser.add_argument("--summary-file", type=str, default=DEFAULT_SUMMARY_FILE)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--business-rules-file", type=str, default=DEFAULT_BUSINESS_RULES_FILE)
    parser.add_argument("--validation-rules-file", type=str, default=DEFAULT_VALIDATION_RULES_FILE)
    return parser.parse_args(argv)


def run_validation_from_files(
    loans_file: str,
    payments_file: str,
    summary_file: str,
    business_rules_file: str,
    validation_rules_file: str,
) -> dict:
    """Load all inputs from disk and return the validation results dict."""
    loans_df = load_loans(Path(loans_file))
    payments_df = load_payments(Path(payments_file))
    summary = load_summary(Path(summary_file))
    business_rules = load_business_rules(Path(business_rules_file))
    validation_rules = load_validation_rules(Path(validation_rules_file))
    return validate_portfolio(loans_df, payments_df, summary, business_rules, validation_rules)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    results = run_validation_from_files(
        args.loans_file,
        args.payments_file,
        args.summary_file,
        args.business_rules_file,
        args.validation_rules_file,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_validation_results(output_dir / "validation_results.json", results)

    print_validation_results(results)

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
