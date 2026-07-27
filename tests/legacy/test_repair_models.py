"""Tests for the deterministic repair-eligibility gate and repair-plan schema validation.

No model calls anywhere -- everything here is pure Python logic operating
on plain dicts.
"""

from __future__ import annotations

import json

import pytest

from src.legacy.repair_models import (
    DEFAULT_ELIGIBLE_ROOT_CAUSE_CATEGORIES,
    RepairEligibility,
    RepairPlanValidationError,
    build_blocked_repair_plan,
    build_no_repair_needed_plan,
    evaluate_repair_eligibility,
    parse_repair_plan,
    repair_plan_to_dict,
)

ALLOWED_TARGET_HINTS = {"context/business_rules.json", "src/transform.py"}

ALLOWED_TARGETS_REGISTRY = {
    "data/scenarios/x/pipeline_config.json": {
        "repair_type": "CONFIGURATION_CHANGE",
        "editable_fields": ["business_rules_file"],
        "allowed_values": {"business_rules_file": ["context/business_rules.json", "data/scenarios/x/business_rules.json"]},
    },
    "src/transform.py": {
        "repair_type": "CODE_CHANGE",
    },
}

BASE_DIAGNOSIS = {
    "diagnosis_status": "DIAGNOSED",
    "root_cause_category": "BUSINESS_RULE_MISMATCH",
    "confidence": "HIGH",
    "recommended_fix": {"target_file": "context/business_rules.json", "change_summary": "x", "scope": "MINIMAL"},
    "evidence": [{"source_type": "BUSINESS_RULE", "source_reference": "get_business_rules", "finding": "x", "expected": None, "actual": None}],
}


# --- Eligibility gate -------------------------------------------------------


def test_no_incident_yields_no_repair_needed():
    diagnosis = {**BASE_DIAGNOSIS, "diagnosis_status": "NO_INCIDENT"}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.NO_REPAIR_NEEDED


def test_insufficient_evidence_yields_human_review_required():
    diagnosis = {**BASE_DIAGNOSIS, "diagnosis_status": "INSUFFICIENT_EVIDENCE"}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.HUMAN_REVIEW_REQUIRED


def test_unknown_contract_change_category_requires_human_review():
    diagnosis = {**BASE_DIAGNOSIS, "root_cause_category": "SOURCE_CONTRACT_CHANGE"}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.HUMAN_REVIEW_REQUIRED


@pytest.mark.parametrize("category", sorted(DEFAULT_ELIGIBLE_ROOT_CAUSE_CATEGORIES))
def test_eligible_categories_yield_eligible_for_repair(category):
    diagnosis = {**BASE_DIAGNOSIS, "root_cause_category": category}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.ELIGIBLE_FOR_REPAIR


def test_low_confidence_blocks_automated_repair():
    diagnosis = {**BASE_DIAGNOSIS, "confidence": "MEDIUM"}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.HUMAN_REVIEW_REQUIRED


def test_missing_recommended_fix_requires_human_review():
    diagnosis = {**BASE_DIAGNOSIS, "recommended_fix": None}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.HUMAN_REVIEW_REQUIRED


def test_target_outside_allowlist_requires_human_review():
    diagnosis = {**BASE_DIAGNOSIS, "recommended_fix": {"target_file": "/etc/passwd", "change_summary": "x", "scope": "MINIMAL"}}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.HUMAN_REVIEW_REQUIRED


def test_ungrounded_diagnosis_evidence_is_invalid():
    diagnosis = {**BASE_DIAGNOSIS, "evidence": []}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.INVALID_DIAGNOSIS


def test_missing_required_keys_is_invalid_diagnosis():
    result = evaluate_repair_eligibility({"diagnosis_status": "DIAGNOSED"}, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.INVALID_DIAGNOSIS


def test_unknown_diagnosis_status_is_invalid():
    diagnosis = {**BASE_DIAGNOSIS, "diagnosis_status": "SOMETHING_ELSE"}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.INVALID_DIAGNOSIS


def test_unknown_confidence_is_invalid():
    diagnosis = {**BASE_DIAGNOSIS, "confidence": "VERY_SURE"}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS)
    assert result.decision == RepairEligibility.INVALID_DIAGNOSIS


def test_confidence_threshold_is_configurable():
    diagnosis = {**BASE_DIAGNOSIS, "confidence": "MEDIUM"}
    result = evaluate_repair_eligibility(diagnosis, allowed_target_files=ALLOWED_TARGET_HINTS, confidence_threshold="MEDIUM")
    assert result.decision == RepairEligibility.ELIGIBLE_FOR_REPAIR


# --- Repair plan schema/grounding validation --------------------------------


def _valid_config_plan(**overrides) -> dict:
    plan = {
        "repair_decision": "PROPOSE_REPAIR",
        "repair_type": "CONFIGURATION_CHANGE",
        "incident_id": "x",
        "diagnosis_reference": "x",
        "root_cause_addressed": "stale config",
        "target_file": "data/scenarios/x/pipeline_config.json",
        "target_symbol_or_setting": "business_rules_file",
        "current_behavior": "points at the old rules file",
        "proposed_behavior": "points at the adopted rules file",
        "change_description": "update the pointer",
        "patch": {
            "format": "STRUCTURED_CONFIG_EDIT",
            "content": {"operations": [{"field": "business_rules_file", "value": "data/scenarios/x/business_rules.json"}]},
        },
        "files_expected_to_change": ["data/scenarios/x/pipeline_config.json"],
        "files_expected_not_to_change": ["data/raw/loans.json"],
        "verification_steps": ["rerun ETL", "rerun validation"],
        "rollback_description": "restore the original pointer",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": ["get_business_rules"],
    }
    plan.update(overrides)
    return plan


def _valid_code_plan(**overrides) -> dict:
    diff = "--- a/src/transform.py\n+++ b/src/transform.py\n@@ -1,2 +1,2 @@\n-old\n+new\n"
    plan = {
        "repair_decision": "PROPOSE_REPAIR",
        "repair_type": "CODE_CHANGE",
        "incident_id": "x",
        "diagnosis_reference": "x",
        "root_cause_addressed": "grain mismatch",
        "target_file": "src/transform.py",
        "target_symbol_or_setting": "compute_portfolio_summary_from_payment_events",
        "current_behavior": "counts every SETTLED row",
        "proposed_behavior": "counts one SETTLED per payment_id",
        "change_description": "dedupe before aggregating",
        "patch": {"format": "UNIFIED_DIFF", "content": diff},
        "files_expected_to_change": ["src/transform.py"],
        "files_expected_not_to_change": ["data/scenarios/x/business_rules.json"],
        "verification_steps": ["rerun ETL", "rerun validation"],
        "rollback_description": "discard the patched copy",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": ["get_business_rules"],
    }
    plan.update(overrides)
    return plan


def test_valid_configuration_change_plan_parses():
    plan = parse_repair_plan(_valid_config_plan(), diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)
    assert plan.repair_type.value == "CONFIGURATION_CHANGE"
    assert plan.target_file == "data/scenarios/x/pipeline_config.json"


def test_valid_code_change_plan_parses():
    plan = parse_repair_plan(_valid_code_plan(), diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)
    assert plan.repair_type.value == "CODE_CHANGE"
    assert plan.target_file == "src/transform.py"


def test_arbitrary_target_path_is_rejected():
    bad = _valid_config_plan(target_file="/etc/passwd", files_expected_to_change=["/etc/passwd"])
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_prohibited_target_is_rejected_even_if_hypothetically_registered():
    registry_with_prohibited = {**ALLOWED_TARGETS_REGISTRY, "data/raw/loans.json": {"repair_type": "CONFIGURATION_CHANGE"}}
    bad = _valid_config_plan(target_file="data/raw/loans.json", files_expected_to_change=["data/raw/loans.json"])
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=registry_with_prohibited)


def test_multiple_targets_is_rejected():
    bad = _valid_config_plan(files_expected_to_change=["data/scenarios/x/pipeline_config.json", "src/transform.py"])
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_repair_type_mismatch_with_registered_target_is_rejected():
    bad = _valid_config_plan(repair_type="CODE_CHANGE")
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_patch_format_must_match_repair_type():
    bad = _valid_config_plan(patch={"format": "UNIFIED_DIFF", "content": "not a real diff"})
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_structured_edit_disallowed_field_is_rejected():
    bad = _valid_config_plan(
        patch={"format": "STRUCTURED_CONFIG_EDIT", "content": {"operations": [{"field": "not_editable", "value": "x"}]}}
    )
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_structured_edit_disallowed_value_is_rejected():
    bad = _valid_config_plan(
        patch={
            "format": "STRUCTURED_CONFIG_EDIT",
            "content": {"operations": [{"field": "business_rules_file", "value": "/etc/passwd"}]},
        }
    )
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_malformed_patch_missing_content_key_is_rejected():
    bad = _valid_config_plan(patch={"format": "STRUCTURED_CONFIG_EDIT"})
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_oversized_unified_diff_is_rejected():
    huge_diff = "--- a/src/transform.py\n+++ b/src/transform.py\n" + ("+x\n" * 20_000)
    bad = _valid_code_plan(patch={"format": "UNIFIED_DIFF", "content": huge_diff})
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_invented_evidence_reference_is_rejected():
    bad = _valid_config_plan(evidence_references=["some_tool_never_in_diagnosis"])
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_empty_evidence_references_is_rejected_for_proposed_repair():
    bad = _valid_config_plan(evidence_references=[])
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_empty_verification_steps_is_rejected_for_proposed_repair():
    bad = _valid_config_plan(verification_steps=[])
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_human_review_decision_with_non_null_target_is_rejected():
    bad = _valid_config_plan(repair_decision="HUMAN_REVIEW_REQUIRED")
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_no_safe_repair_decision_parses_with_null_target():
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
        "change_description": "No safe automated repair is available.",
        "patch": None,
        "files_expected_to_change": [],
        "files_expected_not_to_change": [],
        "verification_steps": [],
        "rollback_description": "not applicable",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": [],
    }
    plan = parse_repair_plan(raw, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)
    assert plan.repair_decision.value == "NO_SAFE_REPAIR"
    assert plan.target_file is None


def test_invalid_enum_value_is_rejected():
    bad = _valid_config_plan(risk_level="EXTREME")
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


def test_missing_required_key_is_rejected():
    bad = _valid_config_plan()
    del bad["risk_level"]
    with pytest.raises(RepairPlanValidationError):
        parse_repair_plan(bad, diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)


# --- Deterministic constructors + serialization -----------------------------


def test_build_blocked_repair_plan_is_json_serializable():
    plan = build_blocked_repair_plan("needs human review", incident_id="x", diagnosis_reference="y")
    data = repair_plan_to_dict(plan)
    json.dumps(data)  # must not raise
    assert data["repair_decision"] == "HUMAN_REVIEW_REQUIRED"
    assert data["target_file"] is None


def test_build_no_repair_needed_plan_is_json_serializable():
    plan = build_no_repair_needed_plan(incident_id="x", diagnosis_reference="y")
    data = repair_plan_to_dict(plan)
    json.dumps(data)
    assert data["repair_decision"] == "NO_SAFE_REPAIR"


def test_repair_plan_to_dict_round_trips_a_proposed_repair():
    plan = parse_repair_plan(_valid_code_plan(), diagnosis=BASE_DIAGNOSIS, allowed_targets=ALLOWED_TARGETS_REGISTRY)
    data = repair_plan_to_dict(plan)
    json.dumps(data)
    assert data["patch"]["format"] == "UNIFIED_DIFF"
    assert isinstance(data["patch"]["content"], str)
