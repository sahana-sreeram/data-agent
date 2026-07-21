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


# --- CODE_CHANGE / loan_payment_join (the incorrect_join scenario's repair kind) ---
#
# The tests above all exercise CONFIGURATION_CHANGE promotion (kind="one_row_per_payment").
# Nothing previously exercised the CODE_CHANGE rerun path end to end: dynamically loading a
# patched src/transform.py from an isolated workspace and validating with
# validate_portfolio_with_join_profile. This closes that gap deterministically (a scripted
# patched module, no live model call).

JOIN_BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "valid_payment_statuses": ["PAID", "SETTLED"],
    "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"],
}

# L2 has zero payment records -- the case the inner-join bug drops entirely.
JOIN_LOANS = [
    {"loan_id": "L1", "customer_id": "C1", "principal_amount": 1000.0, "loan_status": "ACTIVE"},
    {"loan_id": "L2", "customer_id": "C2", "principal_amount": 500.0, "loan_status": "ACTIVE"},
]
JOIN_PAYMENTS = [{"payment_id": "P1", "loan_id": "L1", "amount_paid": 1000.0, "payment_status": "PAID"}]

JOIN_VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0},
    "rules": [
        *VALIDATION_RULES["rules"],
        {
            "id": "loans_without_payment_records_present",
            "type": "informational",
            "tolerance_type": None,
            "description": "d",
        },
    ],
}

# A correct fix for the inner-join bug: how="left" + fillna(0.0), self-contained so it can
# be exec'd standalone as the patched module (mirrors the real fixed
# compute_portfolio_summary_with_payment_join in src/transform.py).
FIXED_JOIN_TRANSFORM_SOURCE = '''
import pandas as pd


def compute_portfolio_summary_with_payment_join(loans_df, payments_df, as_of_date, business_rules):
    success_statuses = business_rules["successful_payment_statuses"]
    successful_payments = (
        payments_df[payments_df["payment_status"].isin(success_statuses)]
        if not payments_df.empty
        else payments_df
    )
    payments_by_loan = (
        successful_payments.groupby("loan_id")["amount_paid"].sum()
        if not successful_payments.empty
        else pd.Series(dtype=float, name="amount_paid").rename_axis("loan_id")
    )
    portfolio = loans_df.merge(payments_by_loan.rename("total_paid"), on="loan_id", how="left")
    portfolio["total_paid"] = portfolio["total_paid"].fillna(0.0)

    total_original_principal = round(float(portfolio["principal_amount"].sum()), 2) if not portfolio.empty else 0.0
    total_successful_payments = round(float(portfolio["total_paid"].sum()), 2) if not portfolio.empty else 0.0
    total_outstanding_balance = round(total_original_principal - total_successful_payments, 2)

    def _count_status(df, col, val):
        return int((df[col] == val).sum()) if not df.empty else 0

    return {
        "as_of_date": as_of_date,
        "loan_count": int(len(portfolio)),
        "active_loan_count": _count_status(portfolio, "loan_status", "ACTIVE"),
        "closed_loan_count": _count_status(portfolio, "loan_status", "CLOSED"),
        "defaulted_loan_count": _count_status(portfolio, "loan_status", "DEFAULTED"),
        "payment_count": int(len(payments_df)),
        "successful_payment_count": int(len(successful_payments)),
        "total_original_principal": total_original_principal,
        "total_successful_payments": total_successful_payments,
        "total_outstanding_balance": total_outstanding_balance,
    }
'''


def _setup_join_scenario(tmp_path: Path) -> dict:
    loans_path = tmp_path / "loans.json"
    payments_path = tmp_path / "payments.json"
    business_rules_path = tmp_path / "business_rules.json"
    validation_rules_path = tmp_path / "validation_rules.json"
    diagnosis_path = tmp_path / "diagnosis.json"
    validation_results_path = tmp_path / "validation_results.json"
    summary_path = tmp_path / "portfolio_summary.json"

    loans_path.write_text(json.dumps(JOIN_LOANS))
    payments_path.write_text(json.dumps(JOIN_PAYMENTS))
    business_rules_path.write_text(json.dumps(JOIN_BUSINESS_RULES))
    validation_rules_path.write_text(json.dumps(JOIN_VALIDATION_RULES))
    diagnosis_path.write_text(json.dumps({"incident_summary": "inner join drops loans with no payments"}))

    # The buggy (pre-repair) ETL output: L2 silently dropped by the inner join.
    buggy_summary = {
        "as_of_date": "2026-07-20",
        "loan_count": 1,
        "active_loan_count": 1,
        "closed_loan_count": 0,
        "defaulted_loan_count": 0,
        "payment_count": 1,
        "successful_payment_count": 1,
        "total_original_principal": 1000.0,
        "total_successful_payments": 1000.0,
        "total_outstanding_balance": 0.0,
    }
    summary_path.write_text(json.dumps(buggy_summary))

    validation_before = {
        "overall_status": "FAIL",
        "checks": [
            {"id": "loan_count_reconciliation", "status": "FAIL", "expected": 2, "actual": 1, "difference": -1},
            {"id": "active_loan_count_reconciliation", "status": "FAIL", "expected": 2, "actual": 1, "difference": -1},
            {"id": "total_original_principal_reconciliation", "status": "FAIL", "expected": 1500.0, "actual": 1000.0, "difference": -500.0},
            {"id": "total_outstanding_balance_reconciliation", "status": "FAIL", "expected": 500.0, "actual": 0.0, "difference": -500.0},
            {"id": "payment_count_reconciliation", "status": "PASS", "expected": 1, "actual": 1, "difference": 0},
            {"id": "successful_payment_count_reconciliation", "status": "PASS", "expected": 1, "actual": 1, "difference": 0},
            {"id": "total_successful_payments_reconciliation", "status": "PASS", "expected": 1000.0, "actual": 1000.0, "difference": 0.0},
        ],
    }
    validation_results_path.write_text(json.dumps(validation_before))

    manifest = {
        "incident_id": "incorrect_join_test",
        "diagnosis_file": str(diagnosis_path),
        "validation_results_file": str(validation_results_path),
        "portfolio_summary_file": str(summary_path),
        "etl_function_name": "compute_portfolio_summary_with_payment_join",
        "test_inventory": ["tests/test_transform.py", "tests/test_validate_portfolio.py"],
        "rerun": {
            "kind": "loan_payment_join",
            "loans_file": str(loans_path),
            "payments_file": str(payments_path),
            "as_of_date": "2026-07-20",
            "business_rules_file": str(business_rules_path),
            "validation_rules_file": str(validation_rules_path),
        },
    }
    return manifest


def test_incorrect_join_code_change_repair_is_verified_and_promoted(tmp_path, monkeypatch):
    manifest = _setup_join_scenario(tmp_path)

    workspace_dir = Path(tempfile.mkdtemp())
    patched_transform_path = workspace_dir / "src" / "transform.py"
    patched_transform_path.parent.mkdir(parents=True, exist_ok=True)
    patched_transform_path.write_text(FIXED_JOIN_TRANSFORM_SOURCE)

    repair_result = {
        "repair_status": "APPLIED",
        "repair_type": "CODE_CHANGE",
        "target_file": "src/transform.py",
        "changed_files": ["src/transform.py"],
        "workspace_dir": str(workspace_dir),
    }

    # Redirect ONLY the final promotion copy so the real repo's src/transform.py is never
    # touched, without changing cwd (test_inventory's nested pytest run needs the real cwd --
    # tests/test_transform.py's own CLI tests default to a cwd-relative business-rules path).
    import src.verify_repair as verify_repair_module

    sandbox_transform_path = tmp_path / "sandboxed_transform.py"
    real_copy2 = verify_repair_module.shutil.copy2

    def _redirect_copy2(src, dst, *args, **kwargs):
        if Path(dst) == Path("src/transform.py"):
            dst = sandbox_transform_path
        return real_copy2(src, dst, *args, **kwargs)

    monkeypatch.setattr(verify_repair_module.shutil, "copy2", _redirect_copy2)

    result = run_verify_repair(manifest, {}, repair_result)

    assert result["verification_status"] == "VERIFIED"
    assert result["validation_before"] == "FAIL"
    assert result["validation_after"] == "PASS"
    assert set(result["failed_checks_before"]) == {
        "loan_count_reconciliation",
        "active_loan_count_reconciliation",
        "total_original_principal_reconciliation",
        "total_outstanding_balance_reconciliation",
    }
    assert result["failed_checks_after"] == []
    assert result["rollback_performed"] is False

    # Promotion happened: the (sandboxed) real transform.py and outputs were updated.
    assert sandbox_transform_path.read_text() == FIXED_JOIN_TRANSFORM_SOURCE
    promoted_summary = json.loads(Path(manifest["portfolio_summary_file"]).read_text())
    assert promoted_summary["loan_count"] == 2
    assert promoted_summary["total_original_principal"] == 1500.0
    assert promoted_summary["total_outstanding_balance"] == 500.0
    assert not workspace_dir.exists()
