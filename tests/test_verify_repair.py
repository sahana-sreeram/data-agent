"""Tests for deterministic post-repair verification and promotion.

Uses the REAL transform/validate_portfolio functions against small,
hand-crafted fixtures (no mocking of the pipeline logic itself) so these
tests exercise the actual rerun path, not a stand-in for it.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from src.verify_repair import run_verify_repair

LOANS = [{"loan_id": "L1", "customer_id": "C1", "principal_amount": 1000.0, "loan_status": "CLOSED"}]
PAYMENTS = [
    {"payment_id": "P1", "loan_id": "L1", "amount_paid": 500.0, "payment_status": "PAID"},
    {"payment_id": "P2", "loan_id": "L1", "amount_paid": 500.0, "payment_status": "SETTLED"},
]
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


def _setup(tmp_path: Path):
    loans_path = tmp_path / "loans.json"
    payments_path = tmp_path / "payments.json"
    stale_rules_path = tmp_path / "stale_business_rules.json"
    adopted_rules_path = tmp_path / "adopted_business_rules.json"
    pipeline_config_path = tmp_path / "pipeline_config.json"
    diagnosis_path = tmp_path / "diagnosis.json"
    validation_results_path = tmp_path / "validation_results.json"
    summary_path = tmp_path / "portfolio_summary.json"
    validation_rules_path = tmp_path / "validation_rules.json"

    loans_path.write_text(json.dumps(LOANS))
    payments_path.write_text(json.dumps(PAYMENTS))
    stale_rules_path.write_text(json.dumps({"successful_payment_statuses": ["PAID"], "valid_payment_statuses": ["PAID", "SETTLED"], "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"]}))
    adopted_rules_path.write_text(json.dumps({"successful_payment_statuses": ["PAID", "SETTLED"], "valid_payment_statuses": ["PAID", "SETTLED"], "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"]}))
    validation_rules_path.write_text(json.dumps(VALIDATION_RULES))

    # Stale execution config (pre-repair): points at the old rules.
    pipeline_config_path.write_text(json.dumps({"business_rules_file": str(stale_rules_path)}))

    # Stale summary, computed under the OLD rules: only P1 (500) counted.
    stale_summary = {
        "as_of_date": "2026-07-20",
        "loan_count": 1,
        "active_loan_count": 0,
        "closed_loan_count": 1,
        "defaulted_loan_count": 0,
        "payment_count": 2,
        "successful_payment_count": 1,
        "total_original_principal": 1000.0,
        "total_successful_payments": 500.0,
        "total_outstanding_balance": 500.0,
    }
    summary_path.write_text(json.dumps(stale_summary))

    validation_before = {
        "overall_status": "FAIL",
        "checks": [
            {"id": "loan_count_reconciliation", "status": "PASS", "expected": 1, "actual": 1, "difference": 0},
            {"id": "successful_payment_count_reconciliation", "status": "FAIL", "expected": 2, "actual": 1, "difference": -1},
            {"id": "total_successful_payments_reconciliation", "status": "FAIL", "expected": 1000.0, "actual": 500.0, "difference": -500.0},
            {"id": "total_outstanding_balance_reconciliation", "status": "FAIL", "expected": 0.0, "actual": 500.0, "difference": 500.0},
        ],
    }
    validation_results_path.write_text(json.dumps(validation_before))
    diagnosis_path.write_text(json.dumps({"incident_summary": "stale config"}))

    manifest = {
        "incident_id": "test",
        "diagnosis_file": str(diagnosis_path),
        "validation_results_file": str(validation_results_path),
        "portfolio_summary_file": str(summary_path),
        "pipeline_configuration_file": str(pipeline_config_path),
        "etl_function_name": "compute_portfolio_summary",
        "test_inventory": ["tests/test_transform.py"],
        "rerun": {
            "kind": "one_row_per_payment",
            "loans_file": str(loans_path),
            "payments_file": str(payments_path),
            "as_of_date": "2026-07-20",
            "validation_rules_file": str(validation_rules_path),
            "validation_business_rules_file": str(adopted_rules_path),
        },
    }
    return manifest, pipeline_config_path, adopted_rules_path


def _apply_result_for(target_file: str, workspace_dir: Path) -> dict:
    return {
        "repair_status": "APPLIED",
        "repair_type": "CONFIGURATION_CHANGE",
        "target_file": target_file,
        "changed_files": [target_file],
        "workspace_dir": str(workspace_dir),
    }


def test_correct_repair_causes_etl_and_validation_to_pass_and_promotes(tmp_path):
    manifest, pipeline_config_path, adopted_rules_path = _setup(tmp_path)

    workspace_dir = Path(tempfile.mkdtemp())
    patched_config_path = workspace_dir / str(pipeline_config_path).lstrip("/")
    patched_config_path.parent.mkdir(parents=True, exist_ok=True)
    patched_config_path.write_text(json.dumps({"business_rules_file": str(adopted_rules_path)}))

    repair_result = _apply_result_for(str(pipeline_config_path), workspace_dir)
    result = run_verify_repair(manifest, {}, repair_result)

    assert result["verification_status"] == "VERIFIED"
    assert result["validation_before"] == "FAIL"
    assert result["validation_after"] == "PASS"
    assert "successful_payment_count_reconciliation" in result["failed_checks_before"]
    assert result["failed_checks_after"] == []
    assert result["rollback_performed"] is False

    # Promotion actually happened: the real pipeline_config.json now points at adopted rules.
    assert json.loads(pipeline_config_path.read_text())["business_rules_file"] == str(adopted_rules_path)
    promoted_summary = json.loads(Path(manifest["portfolio_summary_file"]).read_text())
    assert promoted_summary["total_successful_payments"] == 1000.0
    assert promoted_summary["total_outstanding_balance"] == 0.0

    assert not workspace_dir.exists()  # cleaned up


def test_repair_that_does_not_fix_the_issue_is_not_verified_and_nothing_is_promoted(tmp_path):
    manifest, pipeline_config_path, adopted_rules_path = _setup(tmp_path)
    original_config_content = pipeline_config_path.read_text()
    original_summary_content = Path(manifest["portfolio_summary_file"]).read_text()

    # "Repair" that points at a THIRD rules file which still only recognizes PAID --
    # a plausible bad patch that doesn't actually address the root cause.
    still_broken_rules_path = tmp_path / "still_broken_rules.json"
    still_broken_rules_path.write_text(json.dumps({"successful_payment_statuses": ["PAID"], "valid_payment_statuses": ["PAID", "SETTLED"], "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"]}))

    workspace_dir = Path(tempfile.mkdtemp())
    patched_config_path = workspace_dir / str(pipeline_config_path).lstrip("/")
    patched_config_path.parent.mkdir(parents=True, exist_ok=True)
    patched_config_path.write_text(json.dumps({"business_rules_file": str(still_broken_rules_path)}))

    repair_result = _apply_result_for(str(pipeline_config_path), workspace_dir)
    result = run_verify_repair(manifest, {}, repair_result)

    assert result["verification_status"] == "NOT_VERIFIED"
    assert result["rollback_performed"] is True
    # Nothing real was touched.
    assert pipeline_config_path.read_text() == original_config_content
    assert Path(manifest["portfolio_summary_file"]).read_text() == original_summary_content
    assert not workspace_dir.exists()


def test_repair_that_changes_raw_data_is_blocked(tmp_path, monkeypatch):
    manifest, pipeline_config_path, adopted_rules_path = _setup(tmp_path)

    workspace_dir = Path(tempfile.mkdtemp())
    patched_config_path = workspace_dir / str(pipeline_config_path).lstrip("/")
    patched_config_path.parent.mkdir(parents=True, exist_ok=True)
    patched_config_path.write_text(json.dumps({"business_rules_file": str(adopted_rules_path)}))

    # Simulate raw data changing DURING the run (between the before/after
    # snapshots run_verify_repair takes internally) by making the second call
    # to _raw_data_hashes report different content.
    import src.verify_repair as verify_repair_module

    call_count = {"n": 0}
    real_fn = verify_repair_module._raw_data_hashes

    def _flaky_raw_data_hashes(manifest_arg):
        call_count["n"] += 1
        real = real_fn(manifest_arg)
        if call_count["n"] > 1:
            return {**real, "__tampered__": "different"}
        return real

    monkeypatch.setattr(verify_repair_module, "_raw_data_hashes", _flaky_raw_data_hashes)

    repair_result = _apply_result_for(str(pipeline_config_path), workspace_dir)
    result = run_verify_repair(manifest, {}, repair_result)

    assert result["raw_data_unchanged"] is False
    assert result["verification_status"] == "NOT_VERIFIED"


def test_nothing_to_verify_when_repair_was_not_applied(tmp_path):
    manifest, _, _ = _setup(tmp_path)
    repair_result = {"repair_status": "BLOCKED", "target_file": None, "workspace_dir": None}
    result = run_verify_repair(manifest, {}, repair_result)
    assert result["verification_status"] == "BLOCKED"
    assert result["tests"]["targeted"] == "NOT_RUN"


def test_protected_files_hash_check_passes_when_untouched(tmp_path):
    manifest, pipeline_config_path, adopted_rules_path = _setup(tmp_path)
    workspace_dir = Path(tempfile.mkdtemp())
    patched_config_path = workspace_dir / str(pipeline_config_path).lstrip("/")
    patched_config_path.parent.mkdir(parents=True, exist_ok=True)
    patched_config_path.write_text(json.dumps({"business_rules_file": str(adopted_rules_path)}))

    repair_result = _apply_result_for(str(pipeline_config_path), workspace_dir)
    result = run_verify_repair(manifest, {}, repair_result)
    assert result["unchanged_protected_files_verified"] is True


def test_protected_files_hash_check_detects_tampering_mid_run(tmp_path, monkeypatch):
    manifest, pipeline_config_path, adopted_rules_path = _setup(tmp_path)
    workspace_dir = Path(tempfile.mkdtemp())
    patched_config_path = workspace_dir / str(pipeline_config_path).lstrip("/")
    patched_config_path.parent.mkdir(parents=True, exist_ok=True)
    patched_config_path.write_text(json.dumps({"business_rules_file": str(adopted_rules_path)}))

    import src.verify_repair as verify_repair_module

    call_count = {"n": 0}
    real_fn = verify_repair_module._protected_file_hashes

    def _flaky_protected_file_hashes(manifest_arg):
        call_count["n"] += 1
        real = real_fn(manifest_arg)
        if call_count["n"] > 1:
            return {**real, "__tampered__": "different"}
        return real

    monkeypatch.setattr(verify_repair_module, "_protected_file_hashes", _flaky_protected_file_hashes)

    repair_result = _apply_result_for(str(pipeline_config_path), workspace_dir)
    result = run_verify_repair(manifest, {}, repair_result)

    assert result["unchanged_protected_files_verified"] is False
    assert result["verification_status"] == "NOT_VERIFIED"


def test_targeted_and_full_suite_test_results_are_recorded(tmp_path):
    manifest, pipeline_config_path, adopted_rules_path = _setup(tmp_path)
    workspace_dir = Path(tempfile.mkdtemp())
    patched_config_path = workspace_dir / str(pipeline_config_path).lstrip("/")
    patched_config_path.parent.mkdir(parents=True, exist_ok=True)
    patched_config_path.write_text(json.dumps({"business_rules_file": str(adopted_rules_path)}))

    repair_result = _apply_result_for(str(pipeline_config_path), workspace_dir)
    result = run_verify_repair(manifest, {}, repair_result)
    assert result["tests"]["targeted"] in ("PASS", "FAIL")
    assert result["tests"]["full_relevant_suite"] in ("PASS", "FAIL")
