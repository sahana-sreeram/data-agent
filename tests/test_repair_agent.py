"""Tests for the repair agent's tool-calling planning loop, using a scripted fake model client.

No live model calls anywhere in this file.
"""

from __future__ import annotations

import pytest

from src.model_client import ModelClientError, ModelResponse, ScriptedDiagnosisModelClient, ToolCall
from src.repair_agent import SUBMIT_REPAIR_PLAN_TOOL_NAME, RepairAgentError, run_repair_planning
from src.repair_models import RepairPlanValidationError
from src.repair_tools import RepairTools

DIAGNOSIS = {
    "diagnosis_status": "DIAGNOSED",
    "root_cause_category": "ETL_LOGIC",
    "confidence": "HIGH",
    "root_cause": "compute_portfolio_summary_from_payment_events double-counts replayed SETTLED events.",
    "evidence": [
        {"source_type": "ETL_SOURCE", "source_reference": "get_relevant_etl_source", "finding": "no dedup by payment_id", "expected": None, "actual": None},
        {"source_type": "RAW_DATA", "source_reference": "get_duplicate_payment_id_counts", "finding": "31 duplicated payment_ids", "expected": None, "actual": None},
    ],
    "recommended_fix": {"target_file": "src/transform.py", "change_summary": "dedupe by payment_id", "scope": "MINIMAL"},
}
VALIDATION_RESULTS = {"overall_status": "FAIL", "checks": [{"id": "total_successful_payments_reconciliation", "status": "FAIL"}]}
ALLOWED_TARGETS = {
    "src/transform.py": {"repair_type": "CODE_CHANGE"},
}
STARTING_CONTEXT = {"diagnosis_status": "DIAGNOSED", "root_cause_category": "ETL_LOGIC", "confidence": "HIGH"}


@pytest.fixture()
def tools():
    return RepairTools(
        diagnosis=DIAGNOSIS,
        validation_results=VALIDATION_RESULTS,
        business_rules_by_alias={"CURRENT": {"payment_event_rules": {"successful_terminal_event": "SETTLED"}}},
        lineage={"datasets": {"processed.portfolio_summary": {"path": "x", "depends_on": []}}},
        pipeline_configuration=None,
        allowed_repair_targets=ALLOWED_TARGETS,
        test_inventory=["tests/test_transform.py"],
        etl_function_name="compute_portfolio_summary_from_payment_events",
        file_hash_paths={},
    )


def _run(tools, client, **overrides):
    kwargs = dict(diagnosis=DIAGNOSIS, allowed_targets=ALLOWED_TARGETS)
    kwargs.update(overrides)
    return run_repair_planning(STARTING_CONTEXT, tools, client, **kwargs)


def _valid_code_submission(**overrides) -> dict:
    diff = "--- a/src/transform.py\n+++ b/src/transform.py\n@@\n-old\n+new\n"
    payload = {
        "repair_decision": "PROPOSE_REPAIR",
        "repair_type": "CODE_CHANGE",
        "incident_id": "payment_events_cardinality",
        "diagnosis_reference": "grain mismatch",
        "root_cause_addressed": "double counting of replayed SETTLED events",
        "target_file": "src/transform.py",
        "target_symbol_or_setting": "compute_portfolio_summary_from_payment_events",
        "current_behavior": "sums every SETTLED row",
        "proposed_behavior": "sums one SETTLED per payment_id",
        "change_description": "dedupe SETTLED events by payment_id before aggregating",
        "patch": {"format": "UNIFIED_DIFF", "content": diff},
        "files_expected_to_change": ["src/transform.py"],
        "files_expected_not_to_change": ["data/scenarios/payment_events_cardinality/business_rules.json"],
        "verification_steps": ["run tests", "rerun ETL", "rerun validation"],
        "rollback_description": "discard the patched workspace copy",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": ["get_relevant_etl_source"],
    }
    payload.update(overrides)
    return payload


def test_agent_dispatches_planning_tools_then_returns_plan(tools):
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_allowed_repair_targets", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_relevant_etl_source", arguments={"metric_name": "total_successful_payments"})]),
        ModelResponse(tool_calls=[ToolCall(id="3", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=_valid_code_submission())]),
    ]
    plan = _run(tools, ScriptedDiagnosisModelClient(responses))
    assert plan.repair_decision.value == "PROPOSE_REPAIR"
    assert plan.target_file == "src/transform.py"


def test_agent_rejects_target_outside_allowed_targets(tools):
    bad = _valid_code_submission(target_file="/etc/passwd", files_expected_to_change=["/etc/passwd"])
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=bad)])]
    with pytest.raises(RepairPlanValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_agent_rejects_evidence_reference_not_in_diagnosis(tools):
    bad = _valid_code_submission(evidence_references=["some_tool_never_in_diagnosis"])
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=bad)])]
    with pytest.raises(RepairPlanValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_human_review_required_decision_is_accepted(tools):
    raw = {
        "repair_decision": "HUMAN_REVIEW_REQUIRED",
        "repair_type": "NONE",
        "incident_id": "x",
        "diagnosis_reference": "x",
        "root_cause_addressed": None,
        "target_file": None,
        "target_symbol_or_setting": None,
        "current_behavior": None,
        "proposed_behavior": None,
        "change_description": "Evidence is contradictory; a human should confirm the fix.",
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
    plan = _run(tools, ScriptedDiagnosisModelClient(responses))
    assert plan.repair_decision.value == "HUMAN_REVIEW_REQUIRED"


def test_agent_raises_after_max_turns_without_submission(tools):
    responses = [
        ModelResponse(tool_calls=[ToolCall(id=str(i), name="get_allowed_repair_targets", arguments={})]) for i in range(3)
    ]
    with pytest.raises(RepairAgentError):
        _run(tools, ScriptedDiagnosisModelClient(responses), max_turns=3)


def test_model_client_error_propagates_when_scripted_responses_exhausted(tools):
    with pytest.raises(ModelClientError):
        _run(tools, ScriptedDiagnosisModelClient([]))
