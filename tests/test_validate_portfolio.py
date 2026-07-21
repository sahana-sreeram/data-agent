"""Tests for independent portfolio validation (clean-run reconciliation and failure reporting)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.transform import load_business_rules
from src.validate_portfolio import (
    load_summary,
    load_validation_rules,
    main,
    run_validation_from_files,
    validate_portfolio,
    validate_portfolio_with_join_profile,
)

BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "valid_payment_statuses": ["SCHEDULED", "PAID", "LATE", "MISSED", "FAILED"],
    "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"],
}

VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0},
    "rules": [
        {"id": "loans_required_columns_present", "type": "schema", "tolerance_type": None, "description": "d"},
        {"id": "payments_required_columns_present", "type": "schema", "tolerance_type": None, "description": "d"},
        {"id": "payment_status_enum_valid", "type": "enum", "tolerance_type": None, "description": "d"},
        {"id": "loan_status_enum_valid", "type": "enum", "tolerance_type": None, "description": "d"},
        {"id": "payment_loan_referential_integrity", "type": "referential_integrity", "tolerance_type": None, "description": "d"},
        {"id": "loan_count_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "payment_count_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "successful_payment_count_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "active_loan_count_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "closed_loan_count_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "defaulted_loan_count_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "total_original_principal_reconciliation", "type": "reconciliation", "tolerance_type": "currency", "description": "d"},
        {"id": "total_successful_payments_reconciliation", "type": "reconciliation", "tolerance_type": "currency", "description": "d"},
        {"id": "total_outstanding_balance_reconciliation", "type": "reconciliation", "tolerance_type": "currency", "description": "d"},
    ],
}

LOANS = [
    {"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"},
    {"loan_id": "L000002", "customer_id": "C000002", "principal_amount": 2000.0, "loan_status": "ACTIVE"},
    {"loan_id": "L000003", "customer_id": "C000003", "principal_amount": 3000.0, "loan_status": "DEFAULTED"},
]

PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 1000.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000002", "amount_paid": 500.0, "payment_status": "PAID"},
    {"payment_id": "P0000003", "loan_id": "L000002", "amount_paid": 0.0, "payment_status": "SCHEDULED"},
    {"payment_id": "P0000004", "loan_id": "L000003", "amount_paid": 300.0, "payment_status": "LATE"},
    {"payment_id": "P0000005", "loan_id": "L000003", "amount_paid": 0.0, "payment_status": "MISSED"},
]

# Correct, independently-derived summary for LOANS/PAYMENTS above:
# total_original_principal = 6000.0, total_successful_payments (PAID only) = 1500.0
CLEAN_SUMMARY = {
    "as_of_date": "2026-07-20",
    "loan_count": 3,
    "active_loan_count": 1,
    "closed_loan_count": 1,
    "defaulted_loan_count": 1,
    "payment_count": 5,
    "successful_payment_count": 2,
    "total_original_principal": 6000.0,
    "total_successful_payments": 1500.0,
    "total_outstanding_balance": 4500.0,
}


@pytest.fixture()
def loans_df():
    return pd.DataFrame(LOANS)


@pytest.fixture()
def payments_df():
    return pd.DataFrame(PAYMENTS)


def _checks_by_id(results: dict) -> dict:
    return {check["id"]: check for check in results["checks"]}


# --- validate_portfolio_with_join_profile: validate_portfolio() unmodified, plus one
# additional informational WARNING check for loans with zero payment records at all.

VALIDATION_RULES_WITH_JOIN_PROFILE = {
    "tolerance": VALIDATION_RULES["tolerance"],
    "rules": [
        *VALIDATION_RULES["rules"],
        {
            "id": "loans_without_payment_records_present",
            "type": "profile",
            "tolerance_type": None,
            "description": "d",
        },
    ],
}

# L000004 has zero payment rows at all (e.g. newly originated, first payment not yet due).
LOANS_WITH_NO_PAYMENT_LOAN = [
    *LOANS,
    {"loan_id": "L000004", "customer_id": "C000004", "principal_amount": 4000.0, "loan_status": "ACTIVE"},
]

SUMMARY_CORRECTLY_INCLUDING_NO_PAYMENT_LOAN = {
    **CLEAN_SUMMARY,
    "loan_count": 4,
    "active_loan_count": 2,
    "total_original_principal": 10000.0,
    "total_outstanding_balance": 8500.0,
}


def test_join_profile_flags_loan_with_no_payments_as_warning_not_failure():
    loans_df = pd.DataFrame(LOANS_WITH_NO_PAYMENT_LOAN)
    payments_df = pd.DataFrame(PAYMENTS)
    result = validate_portfolio_with_join_profile(
        loans_df, payments_df, SUMMARY_CORRECTLY_INCLUDING_NO_PAYMENT_LOAN, BUSINESS_RULES, VALIDATION_RULES_WITH_JOIN_PROFILE
    )
    checks = _checks_by_id(result)
    profile_check = checks["loans_without_payment_records_present"]
    assert profile_check["status"] == "WARNING"
    assert profile_check["actual"] == 1
    assert "L000004" in profile_check["details"]
    # A WARNING must never affect overall_status when every hard check passes.
    assert result["overall_status"] == "PASS"


def test_join_profile_check_passes_when_every_loan_has_a_payment():
    loans_df = pd.DataFrame(LOANS)
    payments_df = pd.DataFrame(PAYMENTS)
    result = validate_portfolio_with_join_profile(
        loans_df, payments_df, CLEAN_SUMMARY, BUSINESS_RULES, VALIDATION_RULES_WITH_JOIN_PROFILE
    )
    checks = _checks_by_id(result)
    assert checks["loans_without_payment_records_present"]["status"] == "PASS"
    assert checks["loans_without_payment_records_present"]["details"] is None


def test_join_profile_still_fails_overall_when_a_hard_check_fails():
    # Simulates the buggy join ETL's actual output: the no-payment loan is silently dropped
    # from loan_count/active_loan_count/total_original_principal/total_outstanding_balance.
    loans_df = pd.DataFrame(LOANS_WITH_NO_PAYMENT_LOAN)
    payments_df = pd.DataFrame(PAYMENTS)
    buggy_summary = {**CLEAN_SUMMARY}  # loan_count=3 etc. -- doesn't include L000004 at all
    result = validate_portfolio_with_join_profile(
        loans_df, payments_df, buggy_summary, BUSINESS_RULES, VALIDATION_RULES_WITH_JOIN_PROFILE
    )
    checks = _checks_by_id(result)
    assert checks["loan_count_reconciliation"]["status"] == "FAIL"
    assert checks["active_loan_count_reconciliation"]["status"] == "FAIL"
    assert checks["total_original_principal_reconciliation"]["status"] == "FAIL"
    assert checks["total_outstanding_balance_reconciliation"]["status"] == "FAIL"
    # Payment-related checks still reconcile -- the useful "it's missing loans, not
    # miscalculated payments" signal.
    assert checks["successful_payment_count_reconciliation"]["status"] == "PASS"
    assert checks["total_successful_payments_reconciliation"]["status"] == "PASS"
    assert checks["loans_without_payment_records_present"]["status"] == "WARNING"
    assert result["overall_status"] == "FAIL"


def test_join_profile_does_not_mutate_validate_portfolio_checks():
    # validate_portfolio() itself must be untouched by this wrapper -- same checks, same
    # results, just with one extra entry appended.
    loans_df = pd.DataFrame(LOANS)
    payments_df = pd.DataFrame(PAYMENTS)
    base = validate_portfolio(loans_df, payments_df, CLEAN_SUMMARY, BUSINESS_RULES, VALIDATION_RULES)
    wrapped = validate_portfolio_with_join_profile(
        loans_df, payments_df, CLEAN_SUMMARY, BUSINESS_RULES, VALIDATION_RULES_WITH_JOIN_PROFILE
    )
    assert wrapped["checks"][: len(base["checks"])] == base["checks"]
    assert wrapped["total_check_count"] == base["total_check_count"] + 1


def test_clean_data_passes_all_checks(loans_df, payments_df):
    results = validate_portfolio(loans_df, payments_df, CLEAN_SUMMARY, BUSINESS_RULES, VALIDATION_RULES)
    assert results["overall_status"] == "PASS"
    assert results["failed_check_count"] == 0
    assert results["total_check_count"] == 14
    assert all(check["status"] == "PASS" for check in results["checks"])


def test_incorrect_outstanding_balance_fails_with_diagnostic_evidence(loans_df, payments_df):
    corrupted_summary = {**CLEAN_SUMMARY, "total_outstanding_balance": 4600.0}
    results = validate_portfolio(loans_df, payments_df, corrupted_summary, BUSINESS_RULES, VALIDATION_RULES)
    assert results["overall_status"] == "FAIL"
    check = _checks_by_id(results)["total_outstanding_balance_reconciliation"]
    assert check["status"] == "FAIL"
    assert check["expected"] == 4500.0
    assert check["actual"] == 4600.0
    assert check["difference"] == 100.0


def test_incorrect_successful_payments_total_fails(loans_df, payments_df):
    corrupted_summary = {**CLEAN_SUMMARY, "total_successful_payments": 1000.0}
    results = validate_portfolio(loans_df, payments_df, corrupted_summary, BUSINESS_RULES, VALIDATION_RULES)
    check = _checks_by_id(results)["total_successful_payments_reconciliation"]
    assert check["status"] == "FAIL"
    assert check["expected"] == 1500.0
    assert check["actual"] == 1000.0


def test_unexpected_payment_status_value_fails_enum_check(loans_df):
    # Simulates the future upstream-drift scenario (e.g. PAID relabeled SETTLED)
    # without touching real generated data: an unrecognized status must be
    # caught by the enum check even though it wasn't in scope for this milestone.
    dirty_payments = pd.DataFrame(
        [
            {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 1000.0, "payment_status": "SETTLED"},
        ]
    )
    results = validate_portfolio(loans_df, dirty_payments, CLEAN_SUMMARY, BUSINESS_RULES, VALIDATION_RULES)
    check = _checks_by_id(results)["payment_status_enum_valid"]
    assert results["overall_status"] == "FAIL"
    assert check["status"] == "FAIL"
    assert "SETTLED" in check["details"]


def test_orphaned_payment_fails_referential_integrity(loans_df):
    orphan_payments = pd.DataFrame(
        [
            {"payment_id": "P9999999", "loan_id": "L_DOES_NOT_EXIST", "amount_paid": 100.0, "payment_status": "PAID"},
        ]
    )
    results = validate_portfolio(loans_df, orphan_payments, CLEAN_SUMMARY, BUSINESS_RULES, VALIDATION_RULES)
    check = _checks_by_id(results)["payment_loan_referential_integrity"]
    assert results["overall_status"] == "FAIL"
    assert check["status"] == "FAIL"
    assert "P9999999" in check["details"]


def test_missing_required_loan_column_fails_schema_check(payments_df):
    bad_loans_df = pd.DataFrame([{"loan_id": "L1", "loan_status": "ACTIVE"}])  # no principal_amount
    results = validate_portfolio(bad_loans_df, payments_df, CLEAN_SUMMARY, BUSINESS_RULES, VALIDATION_RULES)
    check = _checks_by_id(results)["loans_required_columns_present"]
    assert results["overall_status"] == "FAIL"
    assert check["status"] == "FAIL"
    assert "principal_amount" in check["details"]


def test_load_summary_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_summary(tmp_path / "does_not_exist.json")


def test_load_validation_rules_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_validation_rules(tmp_path / "does_not_exist.json")


def test_end_to_end_against_real_generated_and_processed_data():
    loans_path = Path("data/raw/loans.json")
    payments_path = Path("data/raw/payments.json")
    summary_path = Path("data/processed/portfolio_summary.json")
    if not (loans_path.exists() and payments_path.exists() and summary_path.exists()):
        pytest.skip("data/raw or data/processed not generated yet")

    results = run_validation_from_files(
        str(loans_path),
        str(payments_path),
        str(summary_path),
        "context/business_rules.json",
        "context/validation_rules.json",
    )
    assert results["overall_status"] == "PASS"
    assert results["failed_check_count"] == 0


def _write_context_files(tmp_path: Path) -> tuple[Path, Path]:
    business_rules_path = tmp_path / "business_rules.json"
    validation_rules_path = tmp_path / "validation_rules.json"
    business_rules_path.write_text(json.dumps(BUSINESS_RULES))
    validation_rules_path.write_text(json.dumps(VALIDATION_RULES))
    return business_rules_path, validation_rules_path


def test_cli_writes_results_and_exits_zero_on_pass(tmp_path):
    loans_path, payments_path, summary_path = tmp_path / "loans.json", tmp_path / "payments.json", tmp_path / "summary.json"
    loans_path.write_text(json.dumps(LOANS))
    payments_path.write_text(json.dumps(PAYMENTS))
    summary_path.write_text(json.dumps(CLEAN_SUMMARY))
    business_rules_path, validation_rules_path = _write_context_files(tmp_path)

    output_dir = tmp_path / "processed"
    main(
        [
            "--loans-file", str(loans_path),
            "--payments-file", str(payments_path),
            "--summary-file", str(summary_path),
            "--output-dir", str(output_dir),
            "--business-rules-file", str(business_rules_path),
            "--validation-rules-file", str(validation_rules_path),
        ]
    )

    results = json.loads((output_dir / "validation_results.json").read_text())
    assert results["overall_status"] == "PASS"


def test_cli_exits_nonzero_on_failure(tmp_path):
    loans_path, payments_path, summary_path = tmp_path / "loans.json", tmp_path / "payments.json", tmp_path / "summary.json"
    loans_path.write_text(json.dumps(LOANS))
    payments_path.write_text(json.dumps(PAYMENTS))
    summary_path.write_text(json.dumps({**CLEAN_SUMMARY, "total_outstanding_balance": 999999.0}))
    business_rules_path, validation_rules_path = _write_context_files(tmp_path)

    output_dir = tmp_path / "processed"
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--loans-file", str(loans_path),
                "--payments-file", str(payments_path),
                "--summary-file", str(summary_path),
                "--output-dir", str(output_dir),
                "--business-rules-file", str(business_rules_path),
                "--validation-rules-file", str(validation_rules_path),
            ]
        )
    assert exc_info.value.code == 1

    results = json.loads((output_dir / "validation_results.json").read_text())
    assert results["overall_status"] == "FAIL"
