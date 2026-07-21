"""Tests for the transform -> validate pipeline orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.run_pipeline import main, run_pipeline

AS_OF_DATE = "2026-07-20"

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
]

PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 1000.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000002", "amount_paid": 500.0, "payment_status": "PAID"},
]


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    loans_path = tmp_path / "loans.json"
    payments_path = tmp_path / "payments.json"
    business_rules_path = tmp_path / "business_rules.json"
    validation_rules_path = tmp_path / "validation_rules.json"
    loans_path.write_text(json.dumps(LOANS))
    payments_path.write_text(json.dumps(PAYMENTS))
    business_rules_path.write_text(json.dumps(BUSINESS_RULES))
    validation_rules_path.write_text(json.dumps(VALIDATION_RULES))
    return loans_path, payments_path, business_rules_path, validation_rules_path


def test_clean_pipeline_run_succeeds_end_to_end(tmp_path):
    loans_path, payments_path, business_rules_path, validation_rules_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed"

    run_record = run_pipeline(
        str(loans_path), str(payments_path), str(output_dir), AS_OF_DATE, str(business_rules_path), str(validation_rules_path)
    )

    assert run_record["etl_status"] == "SUCCESS"
    assert run_record["validation_status"] == "PASS"
    assert run_record["overall_status"] == "SUCCESS"
    assert run_record["etl_error"] is None
    assert run_record["validation_error"] is None
    assert (output_dir / "portfolio_summary.json").exists()
    assert (output_dir / "validation_results.json").exists()


def test_pipeline_reports_etl_failure_on_missing_loans_file(tmp_path):
    _, payments_path, business_rules_path, validation_rules_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed"

    run_record = run_pipeline(
        str(tmp_path / "does_not_exist.json"),
        str(payments_path),
        str(output_dir),
        AS_OF_DATE,
        str(business_rules_path),
        str(validation_rules_path),
    )

    assert run_record["etl_status"] == "FAILURE"
    assert run_record["etl_error"] is not None
    assert run_record["validation_status"] == "ERROR"
    assert run_record["overall_status"] == "FAILURE"
    assert not (output_dir / "portfolio_summary.json").exists()


def test_pipeline_reports_validation_failure_when_summary_is_wrong(tmp_path, monkeypatch):
    loans_path, payments_path, business_rules_path, validation_rules_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed"

    import src.run_pipeline as run_pipeline_module

    original_write_summary = run_pipeline_module.write_summary

    def _corrupting_write_summary(path, summary):
        corrupted = {**summary, "total_outstanding_balance": 999999.0}
        original_write_summary(path, corrupted)

    monkeypatch.setattr(run_pipeline_module, "write_summary", _corrupting_write_summary)

    run_record = run_pipeline_module.run_pipeline(
        str(loans_path), str(payments_path), str(output_dir), AS_OF_DATE, str(business_rules_path), str(validation_rules_path)
    )

    assert run_record["etl_status"] == "SUCCESS"
    assert run_record["validation_status"] == "FAIL"
    assert run_record["overall_status"] == "FAILURE"
    results = json.loads((output_dir / "validation_results.json").read_text())
    assert results["overall_status"] == "FAIL"


def test_pipeline_run_record_includes_timestamps_and_artifacts(tmp_path):
    loans_path, payments_path, business_rules_path, validation_rules_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed"

    run_record = run_pipeline(
        str(loans_path), str(payments_path), str(output_dir), AS_OF_DATE, str(business_rules_path), str(validation_rules_path)
    )

    assert run_record["run_started_at"] <= run_record["run_completed_at"]
    assert run_record["artifacts"]["portfolio_summary"] == str(output_dir / "portfolio_summary.json")
    assert run_record["artifacts"]["validation_results"] == str(output_dir / "validation_results.json")


def test_cli_writes_pipeline_run_and_exits_zero_on_success(tmp_path):
    loans_path, payments_path, business_rules_path, validation_rules_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed"

    main(
        [
            "--loans-file", str(loans_path),
            "--payments-file", str(payments_path),
            "--output-dir", str(output_dir),
            "--as-of-date", AS_OF_DATE,
            "--business-rules-file", str(business_rules_path),
            "--validation-rules-file", str(validation_rules_path),
        ]
    )

    run_record = json.loads((output_dir / "pipeline_run.json").read_text())
    assert run_record["overall_status"] == "SUCCESS"


def test_cli_exits_nonzero_on_etl_failure(tmp_path):
    _, payments_path, business_rules_path, validation_rules_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed"

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--loans-file", str(tmp_path / "missing.json"),
                "--payments-file", str(payments_path),
                "--output-dir", str(output_dir),
                "--as-of-date", AS_OF_DATE,
                "--business-rules-file", str(business_rules_path),
                "--validation-rules-file", str(validation_rules_path),
            ]
        )
    assert exc_info.value.code == 1


def test_validation_business_rules_file_lets_validator_check_against_a_different_rule(tmp_path):
    # Models "approved rule change, stale ETL output": the ETL runs (and its
    # output is written) under the OLD rule; the validator independently
    # recomputes under the NEW rule. The ETL is not re-run under the new rule
    # -- only the summary it already wrote is checked against it.
    loans_path = tmp_path / "loans.json"
    payments_path = tmp_path / "payments.json"
    old_rules_path = tmp_path / "old_business_rules.json"
    new_rules_path = tmp_path / "new_business_rules.json"
    validation_rules_path = tmp_path / "validation_rules.json"

    loans = [{"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"}]
    payments = [
        {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 500.0, "payment_status": "PAID"},
        {"payment_id": "P0000002", "loan_id": "L000001", "amount_paid": 200.0, "payment_status": "SETTLED"},
    ]
    old_rules = {
        "successful_payment_statuses": ["PAID"],
        "valid_payment_statuses": ["PAID", "SETTLED", "MISSED", "SCHEDULED", "LATE", "FAILED"],
        "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"],
    }
    new_rules = {
        "successful_payment_statuses": ["PAID", "SETTLED"],
        "valid_payment_statuses": ["PAID", "SETTLED", "MISSED", "SCHEDULED", "LATE", "FAILED"],
        "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"],
    }
    loans_path.write_text(json.dumps(loans))
    payments_path.write_text(json.dumps(payments))
    old_rules_path.write_text(json.dumps(old_rules))
    new_rules_path.write_text(json.dumps(new_rules))
    validation_rules_path.write_text(json.dumps(VALIDATION_RULES))

    output_dir = tmp_path / "processed"
    run_record = run_pipeline(
        str(loans_path),
        str(payments_path),
        str(output_dir),
        AS_OF_DATE,
        str(old_rules_path),
        str(validation_rules_path),
        validation_business_rules_file=str(new_rules_path),
    )

    assert run_record["etl_status"] == "SUCCESS"
    assert run_record["validation_status"] == "FAIL"
    assert run_record["overall_status"] == "FAILURE"

    summary = json.loads((output_dir / "portfolio_summary.json").read_text())
    assert summary["total_successful_payments"] == 500.0  # ETL still PAID-only, per old_rules

    results = json.loads((output_dir / "validation_results.json").read_text())
    checks_by_id = {c["id"]: c for c in results["checks"]}
    assert checks_by_id["payment_status_enum_valid"]["status"] == "PASS"  # SETTLED is approved now
    assert checks_by_id["total_successful_payments_reconciliation"]["status"] == "FAIL"
    assert checks_by_id["total_successful_payments_reconciliation"]["expected"] == 700.0  # PAID + SETTLED
    assert checks_by_id["total_outstanding_balance_reconciliation"]["difference"] == 200.0  # overstated by SETTLED amount


def test_validation_business_rules_file_defaults_to_etl_rules_file_when_omitted(tmp_path):
    loans_path, payments_path, business_rules_path, validation_rules_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed"

    run_record = run_pipeline(
        str(loans_path), str(payments_path), str(output_dir), AS_OF_DATE, str(business_rules_path), str(validation_rules_path)
    )
    assert run_record["overall_status"] == "SUCCESS"  # unchanged prior behavior


def test_end_to_end_real_data_pipeline_succeeds():
    if not Path("data/raw/loans.json").exists():
        pytest.skip("data/raw not generated yet")

    run_record = run_pipeline(
        "data/raw/loans.json",
        "data/raw/payments.json",
        "data/processed",
        AS_OF_DATE,
        "context/business_rules.json",
        "context/validation_rules.json",
    )
    assert run_record["overall_status"] == "SUCCESS"


def test_real_settled_rule_adopted_scenario_is_healed_after_self_healing():
    # This scenario originally demonstrated a stale-config failure (approved
    # rule change, ETL still pointed at the old rules file) -- see git
    # history / README.md for the original FAIL state it was built to prove.
    # It has since been repaired by src.run_self_healing (a real,
    # deterministically-verified repair, not a hand-edit), and the checked-in
    # files now reflect that HEALED state. To regenerate the original broken
    # state for a repeat demo, rerun the commands in README.md's
    # "settled_rule_adopted" section -- generation is fully deterministic.
    results_path = Path("data/scenarios/settled_rule_adopted/validation_results.json")
    if not results_path.exists():
        pytest.skip("settled_rule_adopted scenario not generated yet")

    results = json.loads(results_path.read_text())
    assert results["overall_status"] == "PASS"
    for check in results["checks"]:
        assert check["status"] == "PASS", check


def test_real_settled_rule_adopted_pipeline_run_shows_healed_success():
    run_path = Path("data/scenarios/settled_rule_adopted/pipeline_run.json")
    if not run_path.exists():
        pytest.skip("settled_rule_adopted scenario not generated yet")

    run_record = json.loads(run_path.read_text())
    assert run_record["etl_status"] == "SUCCESS"
    assert run_record["validation_status"] == "PASS"
    assert run_record["overall_status"] == "SUCCESS"
