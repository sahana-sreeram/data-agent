"""Tests for the controlled PAID -> SETTLED upstream-change simulation.

Covers both the injection script in isolation and the full before/after
contrast: clean data validates PASS, the scenario's corrupted data still
lets the ETL succeed but makes validation FAIL with a specific diagnostic.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from src.legacy.run_pipeline import run_pipeline
from src.legacy.simulate_upstream_change import (
    load_payment_records,
    main,
    relabel_paid_to_settled,
    write_payment_records,
)

PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 100.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000001", "amount_paid": 100.0, "payment_status": "PAID"},
    {"payment_id": "P0000003", "loan_id": "L000001", "amount_paid": 100.0, "payment_status": "PAID"},
    {"payment_id": "P0000004", "loan_id": "L000001", "amount_paid": 100.0, "payment_status": "PAID"},
    {"payment_id": "P0000005", "loan_id": "L000001", "amount_paid": 0.0, "payment_status": "MISSED"},
]

LOANS = [{"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"}]

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


def test_relabel_is_deterministic_for_same_seed():
    rng1 = random.Random(99)
    rng2 = random.Random(99)
    updated1, flipped1 = relabel_paid_to_settled(PAYMENTS, rng1, 0.5)
    updated2, flipped2 = relabel_paid_to_settled(PAYMENTS, rng2, 0.5)
    assert updated1 == updated2
    assert flipped1 == flipped2


def test_relabel_different_seeds_can_differ():
    flips = set()
    for seed in range(10):
        _, flipped = relabel_paid_to_settled(PAYMENTS, random.Random(seed), 0.5)
        flips.add(tuple(flipped))
    assert len(flips) > 1


def test_relabel_only_touches_paid_payments():
    _, flipped_ids = relabel_paid_to_settled(PAYMENTS, random.Random(1), 1.0)
    # All 4 PAID payments should be eligible; the MISSED one must never be touched.
    assert "P0000005" not in flipped_ids
    assert len(flipped_ids) == 4


def test_relabel_preserves_amount_paid_and_only_changes_status():
    updated, flipped_ids = relabel_paid_to_settled(PAYMENTS, random.Random(1), 1.0)
    by_id = {p["payment_id"]: p for p in updated}
    for payment_id in flipped_ids:
        assert by_id[payment_id]["payment_status"] == "SETTLED"
        assert by_id[payment_id]["amount_paid"] == 100.0  # unchanged


def test_relabel_invalid_fraction_raises():
    with pytest.raises(ValueError):
        relabel_paid_to_settled(PAYMENTS, random.Random(1), 0.0)
    with pytest.raises(ValueError):
        relabel_paid_to_settled(PAYMENTS, random.Random(1), 1.5)


def test_relabel_with_no_paid_payments_is_a_no_op():
    no_paid = [{"payment_id": "P1", "loan_id": "L1", "amount_paid": 0.0, "payment_status": "MISSED"}]
    updated, flipped_ids = relabel_paid_to_settled(no_paid, random.Random(1), 0.5)
    assert updated == no_paid
    assert flipped_ids == []


def test_load_payment_records_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_payment_records(tmp_path / "does_not_exist.json")


def test_cli_writes_scenario_file_and_leaves_input_untouched(tmp_path):
    input_path = tmp_path / "payments.json"
    output_path = tmp_path / "scenario" / "payments.json"
    input_path.write_text(json.dumps(PAYMENTS))

    main(["--payments-file", str(input_path), "--output-file", str(output_path), "--seed", "1", "--fraction", "1.0"])

    assert json.loads(input_path.read_text()) == PAYMENTS  # clean input untouched
    scenario_payments = json.loads(output_path.read_text())
    settled = [p for p in scenario_payments if p["payment_status"] == "SETTLED"]
    assert len(settled) == 4


def test_cli_same_seed_produces_identical_scenario_output(tmp_path):
    input_path = tmp_path / "payments.json"
    input_path.write_text(json.dumps(PAYMENTS))
    out1, out2 = tmp_path / "out1.json", tmp_path / "out2.json"

    for out in (out1, out2):
        main(["--payments-file", str(input_path), "--output-file", str(out), "--seed", "5", "--fraction", "0.5"])

    assert out1.read_bytes() == out2.read_bytes()


def _write_scenario_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    loans_path = tmp_path / "loans.json"
    clean_payments_path = tmp_path / "payments.json"
    business_rules_path = tmp_path / "business_rules.json"
    validation_rules_path = tmp_path / "validation_rules.json"
    loans_path.write_text(json.dumps(LOANS))
    clean_payments_path.write_text(json.dumps(PAYMENTS))
    business_rules_path.write_text(json.dumps(BUSINESS_RULES))
    validation_rules_path.write_text(json.dumps(VALIDATION_RULES))
    return loans_path, clean_payments_path, business_rules_path, validation_rules_path


def test_scenario_pipeline_etl_succeeds_but_validation_fails_on_enum(tmp_path):
    loans_path, clean_payments_path, business_rules_path, validation_rules_path = _write_scenario_inputs(tmp_path)

    corrupted_payments_path = tmp_path / "scenario_payments.json"
    main(
        [
            "--payments-file", str(clean_payments_path),
            "--output-file", str(corrupted_payments_path),
            "--seed", "99",
            "--fraction", "1.0",
        ]
    )

    clean_output_dir = tmp_path / "clean_out"
    scenario_output_dir = tmp_path / "scenario_out"

    clean_run = run_pipeline(
        str(loans_path), str(clean_payments_path), str(clean_output_dir), "2026-07-20",
        str(business_rules_path), str(validation_rules_path),
    )
    scenario_run = run_pipeline(
        str(loans_path), str(corrupted_payments_path), str(scenario_output_dir), "2026-07-20",
        str(business_rules_path), str(validation_rules_path),
    )

    # Clean baseline: everything passes.
    assert clean_run["overall_status"] == "SUCCESS"

    # Scenario: ETL still runs fine (it doesn't crash on an unrecognized status)...
    assert scenario_run["etl_status"] == "SUCCESS"
    # ...but the reported balance is now wrong (all 4 PAID payments got relabeled,
    # so none count as successful anymore -- balance goes from 600.0 to 1000.0).
    clean_summary = json.loads((clean_output_dir / "portfolio_summary.json").read_text())
    scenario_summary = json.loads((scenario_output_dir / "portfolio_summary.json").read_text())
    assert clean_summary["total_outstanding_balance"] == 600.0
    assert scenario_summary["total_outstanding_balance"] == 1000.0

    # ...and validation correctly flags it as broken.
    assert scenario_run["validation_status"] == "FAIL"
    assert scenario_run["overall_status"] == "FAILURE"

    scenario_results = json.loads((scenario_output_dir / "validation_results.json").read_text())
    checks_by_id = {c["id"]: c for c in scenario_results["checks"]}
    enum_check = checks_by_id["payment_status_enum_valid"]
    assert enum_check["status"] == "FAIL"
    assert "SETTLED" in enum_check["details"]

    # The reconciliation checks still PASS -- the ETL and the validator agree
    # with each other (both blindly exclude SETTLED), which is exactly why the
    # enum check, not reconciliation, is what catches this class of bug.
    assert checks_by_id["total_outstanding_balance_reconciliation"]["status"] == "PASS"
    assert checks_by_id["total_successful_payments_reconciliation"]["status"] == "PASS"


def test_real_settled_bug_scenario_is_checked_in_and_fails_validation():
    scenario_dir = Path("data/scenarios/settled_bug")
    if not (scenario_dir / "validation_results.json").exists():
        pytest.skip("settled_bug scenario not generated yet")

    results = json.loads((scenario_dir / "validation_results.json").read_text())
    assert results["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in results["checks"]}
    assert checks_by_id["payment_status_enum_valid"]["status"] == "FAIL"
    assert "SETTLED" in checks_by_id["payment_status_enum_valid"]["details"]
