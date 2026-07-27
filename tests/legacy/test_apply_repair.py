"""Tests for deterministic repair application: eligibility gate, policy
validation, and isolated-workspace patch application.

No live model calls -- ScriptedDiagnosisModelClient scripts the repair
agent's tool calls and final plan.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.legacy.apply_repair import (
    ApplyRepairError,
    PatchApplyError,
    apply_structured_config_edit,
    apply_unified_diff,
    run_apply_repair,
)
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall
from src.legacy.repair_agent import SUBMIT_REPAIR_PLAN_TOOL_NAME

def _diagnosis_for(pipeline_config_path: str) -> dict:
    return {
        "diagnosis_status": "DIAGNOSED",
        "root_cause_category": "BUSINESS_RULE_MISMATCH",
        "confidence": "HIGH",
        "incident_summary": "stale config",
        "root_cause": "pipeline_config.json points at the old business rules file",
        "evidence": [{"source_type": "BUSINESS_RULE", "source_reference": "get_business_rules", "finding": "x", "expected": None, "actual": None}],
        "recommended_fix": {"target_file": pipeline_config_path, "change_summary": "point at the adopted rules", "scope": "MINIMAL"},
    }


def _write_manifest(tmp_path: Path, **overrides) -> dict:
    business_rules_path = tmp_path / "business_rules.json"
    adopted_rules_path = tmp_path / "adopted_business_rules.json"
    pipeline_config_path = tmp_path / "pipeline_config.json"
    diagnosis_path = tmp_path / "diagnosis.json"
    validation_results_path = tmp_path / "validation_results.json"
    summary_path = tmp_path / "portfolio_summary.json"

    business_rules_path.write_text(json.dumps({"successful_payment_statuses": ["PAID"]}))
    adopted_rules_path.write_text(json.dumps({"successful_payment_statuses": ["PAID", "SETTLED"]}))
    pipeline_config_path.write_text(json.dumps({"business_rules_file": str(business_rules_path)}))
    diagnosis_path.write_text(json.dumps(_diagnosis_for(str(pipeline_config_path))))
    validation_results_path.write_text(json.dumps({"overall_status": "FAIL", "checks": []}))
    summary_path.write_text(json.dumps({"total_original_principal": 1000.0}))

    manifest = {
        "incident_id": "test_incident",
        "diagnosis_file": str(diagnosis_path),
        "validation_results_file": str(validation_results_path),
        "portfolio_summary_file": str(summary_path),
        "eligibility_target_hints": [str(pipeline_config_path)],
        "business_rules_aliases": {"STALE": str(business_rules_path), "ADOPTED": str(adopted_rules_path)},
        "pipeline_configuration_file": str(pipeline_config_path),
        "etl_function_name": "compute_portfolio_summary",
        "test_inventory": ["tests/test_transform.py"],
        "file_hash_aliases": {"PIPELINE_CONFIG": str(pipeline_config_path)},
    }
    manifest.update(overrides)
    return manifest


def _write_repair_targets(tmp_path: Path, pipeline_config_path: str) -> Path:
    targets_path = tmp_path / "repair_targets.json"
    targets_path.write_text(
        json.dumps(
            {
                "targets": {
                    pipeline_config_path: {
                        "repair_type": "CONFIGURATION_CHANGE",
                        "editable_fields": ["business_rules_file"],
                        "allowed_values": {"business_rules_file": [str(Path(pipeline_config_path).parent / "adopted_business_rules.json")]},
                    }
                }
            }
        )
    )
    return targets_path


def _config_submission(target_file: str, new_value: str) -> dict:
    return {
        "repair_decision": "PROPOSE_REPAIR",
        "repair_type": "CONFIGURATION_CHANGE",
        "incident_id": "test_incident",
        "diagnosis_reference": "stale config",
        "root_cause_addressed": "stale pointer",
        "target_file": target_file,
        "target_symbol_or_setting": "business_rules_file",
        "current_behavior": "points at stale rules",
        "proposed_behavior": "points at adopted rules",
        "change_description": "update the pointer",
        "patch": {"format": "STRUCTURED_CONFIG_EDIT", "content": {"operations": [{"field": "business_rules_file", "value": new_value}]}},
        "files_expected_to_change": [target_file],
        "files_expected_not_to_change": [],
        "verification_steps": ["rerun ETL", "rerun validation"],
        "rollback_description": "restore the stale pointer",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": ["get_business_rules"],
    }


def test_no_incident_short_circuits_without_calling_model(tmp_path):
    manifest = _write_manifest(tmp_path)
    diagnosis_path = Path(manifest["diagnosis_file"])
    diagnosis_path.write_text(json.dumps({**_diagnosis_for(manifest["pipeline_configuration_file"]), "diagnosis_status": "NO_INCIDENT"}))

    def factory():
        raise AssertionError("model should not be called for NO_INCIDENT")

    plan_dict, result = run_apply_repair(manifest, factory, repair_targets_file=str(_write_repair_targets(tmp_path, manifest["pipeline_configuration_file"])))
    assert result["repair_status"] == "NO_REPAIR"
    assert plan_dict["repair_decision"] == "NO_SAFE_REPAIR"


def test_unknown_contract_change_is_blocked_without_calling_model(tmp_path):
    manifest = _write_manifest(tmp_path)
    diagnosis_path = Path(manifest["diagnosis_file"])
    diagnosis_path.write_text(json.dumps({**_diagnosis_for(manifest["pipeline_configuration_file"]), "root_cause_category": "SOURCE_CONTRACT_CHANGE"}))

    def factory():
        raise AssertionError("model should not be called when eligibility blocks the incident")

    plan_dict, result = run_apply_repair(manifest, factory, repair_targets_file=str(_write_repair_targets(tmp_path, manifest["pipeline_configuration_file"])))
    assert result["repair_status"] == "BLOCKED"
    assert plan_dict["repair_decision"] == "HUMAN_REVIEW_REQUIRED"


def test_eligible_incident_applies_structured_config_edit_in_isolation(tmp_path):
    manifest = _write_manifest(tmp_path)
    targets_path = _write_repair_targets(tmp_path, manifest["pipeline_configuration_file"])
    adopted_path = str(tmp_path / "adopted_business_rules.json")
    submission = _config_submission(manifest["pipeline_configuration_file"], adopted_path)
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=submission)])]

    original_content = Path(manifest["pipeline_configuration_file"]).read_text()

    plan_dict, result = run_apply_repair(
        manifest, lambda: ScriptedDiagnosisModelClient(responses), repair_targets_file=str(targets_path)
    )

    assert result["repair_status"] == "APPLIED"
    assert result["changed_files"] == [manifest["pipeline_configuration_file"]]
    assert result["original_hashes"] and result["repaired_hashes"]
    assert result["original_hashes"] != result["repaired_hashes"]

    # The REAL file must be untouched -- only the isolated workspace copy changed.
    assert Path(manifest["pipeline_configuration_file"]).read_text() == original_content

    workspace_dir = Path(result["workspace_dir"])
    patched_path = workspace_dir / manifest["pipeline_configuration_file"].lstrip("/")
    assert patched_path.exists()
    patched_content = json.loads(patched_path.read_text())
    assert patched_content["business_rules_file"] == adopted_path

    # Only the target file exists in the workspace -- nothing else was copied.
    all_files = [p for p in workspace_dir.rglob("*") if p.is_file()]
    assert len(all_files) == 1


def test_malformed_model_output_raises_apply_repair_error(tmp_path):
    manifest = _write_manifest(tmp_path)
    targets_path = _write_repair_targets(tmp_path, manifest["pipeline_configuration_file"])
    bad = {"repair_decision": "NOT_A_REAL_DECISION"}
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=bad)])]

    with pytest.raises(ApplyRepairError):
        run_apply_repair(manifest, lambda: ScriptedDiagnosisModelClient(responses), repair_targets_file=str(targets_path))


def test_patch_producing_no_change_is_blocked(tmp_path):
    manifest = _write_manifest(tmp_path)
    targets_path = _write_repair_targets(tmp_path, manifest["pipeline_configuration_file"])
    # "new" value is identical to the current one -- a no-op edit.
    current_value = json.loads(Path(manifest["pipeline_configuration_file"]).read_text())["business_rules_file"]
    targets_path.write_text(
        json.dumps(
            {
                "targets": {
                    manifest["pipeline_configuration_file"]: {
                        "repair_type": "CONFIGURATION_CHANGE",
                        "editable_fields": ["business_rules_file"],
                        "allowed_values": {"business_rules_file": [current_value]},
                    }
                }
            }
        )
    )
    submission = _config_submission(manifest["pipeline_configuration_file"], current_value)
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=submission)])]

    plan_dict, result = run_apply_repair(
        manifest, lambda: ScriptedDiagnosisModelClient(responses), repair_targets_file=str(targets_path)
    )
    assert result["repair_status"] == "BLOCKED"
    assert result["plan_policy_status"] == "FAIL"


def test_no_safe_repair_decision_yields_no_repair_status(tmp_path):
    manifest = _write_manifest(tmp_path)
    targets_path = _write_repair_targets(tmp_path, manifest["pipeline_configuration_file"])
    raw = {
        "repair_decision": "NO_SAFE_REPAIR",
        "repair_type": "NONE",
        "incident_id": "x",
        "diagnosis_reference": "x",
        "root_cause_addressed": None,
        "target_file": None,
        "target_symbol_or_setting": None,
        "current_behavior": None,
        "proposed_behavior": None,
        "change_description": "No safe automated repair is available for this incident.",
        "patch": None,
        "files_expected_to_change": [],
        "files_expected_not_to_change": [],
        "verification_steps": [],
        "rollback_description": "not applicable",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": [],
    }
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=raw)])]

    plan_dict, result = run_apply_repair(
        manifest, lambda: ScriptedDiagnosisModelClient(responses), repair_targets_file=str(targets_path)
    )
    assert result["repair_status"] == "NO_REPAIR"


# --- Direct unit tests for the patch-application primitives -----------------


def test_apply_structured_config_edit_returns_new_dict_without_mutating_original():
    original = {"a": 1, "b": 2}
    patched = apply_structured_config_edit(original, [{"field": "a", "value": 99}])
    assert patched == {"a": 99, "b": 2}
    assert original == {"a": 1, "b": 2}  # unmutated


def test_apply_unified_diff_rejects_context_mismatch():
    original = "line1\nline2\nline3\n"
    diff = "--- a/f\n+++ b/f\n@@\n line1\n-wrong line\n+new line\n line3\n"
    with pytest.raises(PatchApplyError):
        apply_unified_diff(original, diff)


def test_apply_unified_diff_rejects_diff_with_no_hunks():
    with pytest.raises(PatchApplyError):
        apply_unified_diff("line1\n", "--- a/f\n+++ b/f\n")


def test_apply_unified_diff_tolerates_the_apply_patch_envelope():
    # Confirmed live: gpt-5 (via the Responses API) sometimes wraps an otherwise well-formed
    # unified diff in OpenAI's own apply_patch envelope instead of a bare unified diff --
    # these wrapper lines carry no diff content and must be skipped, not rejected.
    original = "line1\nline2\nline3\n"
    diff = "*** Begin Patch\n*** Update File: f\n@@\n line1\n-line2\n+new line\n line3\n*** End Patch"
    assert apply_unified_diff(original, diff) == "line1\nnew line\nline3\n"
