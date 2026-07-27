"""End-to-end tests for the full self-healing workflow (apply + verify),
covering the three scenario shapes described in the milestone:

1. Unknown contract change -> blocked, no files touched.
2. Approved rule change with stale config -> eligible, repaired, VERIFIED.
3. Malformed model output -> rejected cleanly, no writes occur.

No live model calls -- ScriptedDiagnosisModelClient scripts everything.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.legacy.apply_repair import ApplyRepairError
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall
from src.legacy.repair_agent import SUBMIT_REPAIR_PLAN_TOOL_NAME
from src.legacy.run_self_healing import run_self_healing

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


def _setup_stale_config_scenario(tmp_path: Path) -> dict:
    loans_path = tmp_path / "loans.json"
    payments_path = tmp_path / "payments.json"
    stale_rules_path = tmp_path / "stale_business_rules.json"
    adopted_rules_path = tmp_path / "adopted_business_rules.json"
    pipeline_config_path = tmp_path / "pipeline_config.json"
    diagnosis_path = tmp_path / "diagnosis.json"
    validation_results_path = tmp_path / "validation_results.json"
    summary_path = tmp_path / "portfolio_summary.json"
    validation_rules_path = tmp_path / "validation_rules.json"
    repair_targets_path = tmp_path / "repair_targets.json"

    loans_path.write_text(json.dumps(LOANS))
    payments_path.write_text(json.dumps(PAYMENTS))
    stale_rules_path.write_text(json.dumps({"successful_payment_statuses": ["PAID"], "valid_payment_statuses": ["PAID", "SETTLED"], "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"]}))
    adopted_rules_path.write_text(json.dumps({"successful_payment_statuses": ["PAID", "SETTLED"], "valid_payment_statuses": ["PAID", "SETTLED"], "valid_loan_statuses": ["ACTIVE", "CLOSED", "DEFAULTED"]}))
    validation_rules_path.write_text(json.dumps(VALIDATION_RULES))
    pipeline_config_path.write_text(json.dumps({"business_rules_file": str(stale_rules_path)}))

    repair_targets_path.write_text(
        json.dumps(
            {
                "targets": {
                    str(pipeline_config_path): {
                        "repair_type": "CONFIGURATION_CHANGE",
                        "editable_fields": ["business_rules_file"],
                        "allowed_values": {"business_rules_file": [str(adopted_rules_path)]},
                    }
                }
            }
        )
    )

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

    diagnosis = {
        "diagnosis_status": "DIAGNOSED",
        "root_cause_category": "BUSINESS_RULE_MISMATCH",
        "confidence": "HIGH",
        "incident_summary": "stale config",
        "root_cause": "pipeline_config.json points at the old business rules file",
        "evidence": [{"source_type": "BUSINESS_RULE", "source_reference": "get_business_rules", "finding": "x", "expected": None, "actual": None}],
        "recommended_fix": {"target_file": str(pipeline_config_path), "change_summary": "point at the adopted rules", "scope": "MINIMAL"},
    }
    diagnosis_path.write_text(json.dumps(diagnosis))

    validation_before = {
        "overall_status": "FAIL",
        "checks": [
            {"id": "successful_payment_count_reconciliation", "status": "FAIL", "expected": 2, "actual": 1, "difference": -1},
            {"id": "total_successful_payments_reconciliation", "status": "FAIL", "expected": 1000.0, "actual": 500.0, "difference": -500.0},
        ],
    }
    validation_results_path.write_text(json.dumps(validation_before))

    manifest = {
        "incident_id": "stale_config_test",
        "diagnosis_file": str(diagnosis_path),
        "validation_results_file": str(validation_results_path),
        "portfolio_summary_file": str(summary_path),
        "eligibility_target_hints": [str(pipeline_config_path)],
        "business_rules_aliases": {"STALE": str(stale_rules_path), "ADOPTED": str(adopted_rules_path)},
        "pipeline_configuration_file": str(pipeline_config_path),
        "etl_function_name": "compute_portfolio_summary",
        "test_inventory": ["tests/legacy/test_transform.py", "tests/legacy/test_validate_portfolio.py"],
        "file_hash_aliases": {"PIPELINE_CONFIG": str(pipeline_config_path)},
        "rerun": {
            "kind": "one_row_per_payment",
            "loans_file": str(loans_path),
            "payments_file": str(payments_path),
            "as_of_date": "2026-07-20",
            "validation_rules_file": str(validation_rules_path),
            "validation_business_rules_file": str(adopted_rules_path),
        },
    }
    return {"manifest": manifest, "repair_targets_file": str(repair_targets_path), "pipeline_config_path": pipeline_config_path, "adopted_rules_path": adopted_rules_path}


def _valid_config_submission(target_file: str, new_value: str) -> dict:
    return {
        "repair_decision": "PROPOSE_REPAIR",
        "repair_type": "CONFIGURATION_CHANGE",
        "incident_id": "stale_config_test",
        "diagnosis_reference": "stale config",
        "root_cause_addressed": "stale pointer",
        "target_file": target_file,
        "target_symbol_or_setting": "business_rules_file",
        "current_behavior": "points at stale rules",
        "proposed_behavior": "points at adopted rules",
        "change_description": "update the pointer to the adopted rules file",
        "patch": {"format": "STRUCTURED_CONFIG_EDIT", "content": {"operations": [{"field": "business_rules_file", "value": new_value}]}},
        "files_expected_to_change": [target_file],
        "files_expected_not_to_change": [],
        "verification_steps": ["rerun ETL", "rerun validation"],
        "rollback_description": "restore the stale pointer",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": ["get_business_rules"],
    }


def test_unknown_contract_scenario_is_blocked_with_no_files_changed(tmp_path):
    setup = _setup_stale_config_scenario(tmp_path)
    manifest = setup["manifest"]
    diagnosis_path = Path(manifest["diagnosis_file"])
    diagnosis = json.loads(diagnosis_path.read_text())
    diagnosis["root_cause_category"] = "SOURCE_CONTRACT_CHANGE"  # unknown/unapproved contract change
    diagnosis_path.write_text(json.dumps(diagnosis))

    config_before = Path(manifest["pipeline_configuration_file"]).read_text()
    summary_before = Path(manifest["portfolio_summary_file"]).read_text()

    def factory():
        raise AssertionError("model must not be called for an incident requiring human review")

    outcome = run_self_healing(manifest, factory, repair_targets_file=setup["repair_targets_file"], output_dir=tmp_path)

    assert outcome["repair_plan"]["repair_decision"] == "HUMAN_REVIEW_REQUIRED"
    assert outcome["repair_result"]["repair_status"] == "BLOCKED"
    assert outcome["repair_verification"]["verification_status"] == "BLOCKED"

    # Nothing changed.
    assert Path(manifest["pipeline_configuration_file"]).read_text() == config_before
    assert Path(manifest["portfolio_summary_file"]).read_text() == summary_before


def test_approved_rule_scenario_is_repaired_and_verified(tmp_path):
    setup = _setup_stale_config_scenario(tmp_path)
    manifest = setup["manifest"]
    submission = _valid_config_submission(manifest["pipeline_configuration_file"], str(setup["adopted_rules_path"]))
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_allowed_repair_targets", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_pipeline_configuration", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="3", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=submission)]),
    ]

    outcome = run_self_healing(
        manifest,
        lambda: ScriptedDiagnosisModelClient(responses),
        repair_targets_file=setup["repair_targets_file"],
        output_dir=tmp_path,
    )

    assert outcome["repair_result"]["repair_status"] == "APPLIED"
    assert outcome["repair_verification"]["verification_status"] == "VERIFIED"
    assert outcome["repair_verification"]["validation_before"] == "FAIL"
    assert outcome["repair_verification"]["validation_after"] == "PASS"

    # Promotion actually happened.
    promoted_config = json.loads(Path(manifest["pipeline_configuration_file"]).read_text())
    assert promoted_config["business_rules_file"] == str(setup["adopted_rules_path"])
    promoted_summary = json.loads(Path(manifest["portfolio_summary_file"]).read_text())
    assert promoted_summary["total_successful_payments"] == 1000.0

    # Output artifacts were written.
    assert (tmp_path / "repair_plan.json").exists()
    assert (tmp_path / "repair_result.json").exists()
    assert (tmp_path / "repair_verification.json").exists()


def test_malformed_model_output_is_rejected_cleanly_with_no_writes(tmp_path):
    setup = _setup_stale_config_scenario(tmp_path)
    manifest = setup["manifest"]
    config_before = Path(manifest["pipeline_configuration_file"]).read_text()

    bad_submission = {"repair_decision": "NOT_A_REAL_DECISION"}
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=bad_submission)])]

    with pytest.raises(ApplyRepairError):
        run_self_healing(
            manifest,
            lambda: ScriptedDiagnosisModelClient(responses),
            repair_targets_file=setup["repair_targets_file"],
            output_dir=tmp_path,
        )

    # No repair artifacts were written, and the real config is untouched.
    assert not (tmp_path / "repair_result.json").exists()
    assert Path(manifest["pipeline_configuration_file"]).read_text() == config_before
