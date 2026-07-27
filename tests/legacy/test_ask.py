"""Tests for src/ask.py -- the business Q&A CLI that closes the loop:
answer directly when data is trustworthy, auto-heal via the existing
diagnose/repair/verify machinery when it isn't, and never fabricate a
confident answer when it can't be verified or repaired.

diagnose_incident.run_diagnose_incident and run_self_healing.run_self_healing
are monkeypatched at their src.ask call sites -- they have their own
comprehensive test suites (test_diagnose_incident.py, test_apply_repair.py,
test_verify_repair.py); these tests are only about src.ask's own
orchestration decisions.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from src.legacy.answer_models import AnswerStatus
from src.legacy.ask import AskError, answer_question
from src.legacy.business_agent import SUBMIT_ANSWER_TOOL_NAME
from src.legacy.diagnose_incident import DiagnoseIncidentError
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall

PORTFOLIO_SUMMARY = {"total_outstanding_balance": 997522.36}
DATA_DICTIONARY = {"portfolio_summary": {"fields": {"total_outstanding_balance": {"type": "float"}}}}
BUSINESS_RULES = {"successful_payment_statuses": ["PAID"]}
QUESTION = "What is the total outstanding loan balance?"


def _valid_submission() -> dict:
    return {
        "answer_status": "ANSWERED",
        "question": QUESTION,
        "answer_summary": "The total outstanding loan balance is 997522.36.",
        "as_of_date": None,
        "cited_metrics": [
            {"metric_name": "total_outstanding_balance", "value": 997522.36, "source_reference": "get_portfolio_summary"}
        ],
        "caveats": [],
    }


def _answering_factory():
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_portfolio_summary", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_ANSWER_TOOL_NAME, arguments=_valid_submission())]),
    ]
    return ScriptedDiagnosisModelClient(responses)


def _poison_factory():
    def factory():
        raise AssertionError("this model client factory should not have been used")

    return factory


def _write_manifest(tmp_path: Path, *, validation_status: str) -> dict:
    scenario_dir = tmp_path / "scenario"
    scenario_dir.mkdir()

    validation_results_file = scenario_dir / "validation_results.json"
    validation_results_file.write_text(json.dumps({"overall_status": validation_status, "checks": []}))

    portfolio_summary_file = scenario_dir / "portfolio_summary.json"
    portfolio_summary_file.write_text(json.dumps(PORTFOLIO_SUMMARY))

    business_rules_file = scenario_dir / "business_rules.json"
    business_rules_file.write_text(json.dumps(BUSINESS_RULES))

    data_dictionary_file = tmp_path / "data_dictionary.json"
    data_dictionary_file.write_text(json.dumps(DATA_DICTIONARY))

    return {
        "diagnosis_file": str(scenario_dir / "diagnosis.json"),
        "validation_results_file": str(validation_results_file),
        "portfolio_summary_file": str(portfolio_summary_file),
        "pipeline_run_file": str(scenario_dir / "pipeline_run.json"),
        "lineage_file": str(tmp_path / "lineage.json"),
        "data_dictionary_file": str(data_dictionary_file),
        "etl_function_name": "compute_portfolio_summary",
        "rerun": {
            "kind": "one_row_per_payment",
            "loans_file": str(tmp_path / "loans.json"),
            "payments_file": str(tmp_path / "payments.json"),
            "as_of_date": "2026-07-20",
            "validation_rules_file": str(tmp_path / "validation_rules.json"),
            "validation_business_rules_file": str(business_rules_file),
        },
    }, scenario_dir, validation_results_file, portfolio_summary_file


def test_pass_validation_answers_directly_without_touching_diagnose_or_repair(tmp_path, monkeypatch):
    manifest, scenario_dir, _, _ = _write_manifest(tmp_path, validation_status="PASS")

    monkeypatch.setattr("src.legacy.ask.run_diagnose_incident", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not diagnose")))
    monkeypatch.setattr("src.legacy.ask.run_self_healing", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not self-heal")))

    result = answer_question(
        QUESTION,
        manifest,
        diagnosis_model_client_factory=_poison_factory(),
        repair_model_client_factory=_poison_factory(),
        answer_model_client_factory=_answering_factory,
    )

    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["answer"]["cited_metrics"][0]["value"] == 997522.36
    assert result["self_healing"] is None
    assert json.loads((scenario_dir / "answer.json").read_text()) == result


def test_fail_validation_heals_successfully_then_answers(tmp_path, monkeypatch):
    manifest, scenario_dir, validation_results_file, portfolio_summary_file = _write_manifest(tmp_path, validation_status="FAIL")

    def fake_diagnose(args, factory):
        return {"diagnosis_status": "DIAGNOSED", "root_cause_category": "DUPLICATION", "incident_summary": "duplicated rows"}

    def fake_self_heal(manifest_arg, factory, *, repair_targets_file, confidence_threshold, output_dir):
        # Simulate a real repair: the pipeline is rerun and now passes.
        validation_results_file.write_text(json.dumps({"overall_status": "PASS", "checks": []}))
        portfolio_summary_file.write_text(json.dumps(PORTFOLIO_SUMMARY))
        return {
            "repair_plan": {},
            "repair_result": {"repair_status": "APPLIED"},
            "repair_verification": {"verification_status": "VERIFIED"},
        }

    monkeypatch.setattr("src.legacy.ask.run_diagnose_incident", fake_diagnose)
    monkeypatch.setattr("src.legacy.ask.run_self_healing", fake_self_heal)

    result = answer_question(
        QUESTION,
        manifest,
        diagnosis_model_client_factory=_poison_factory(),
        repair_model_client_factory=_poison_factory(),
        answer_model_client_factory=_answering_factory,
    )

    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["self_healing"]["repair_status"] == "APPLIED"
    assert result["self_healing"]["verification_status"] == "VERIFIED"


def test_fail_validation_blocked_repair_returns_unreliable_data_without_answering(tmp_path, monkeypatch):
    manifest, scenario_dir, _, _ = _write_manifest(tmp_path, validation_status="FAIL")

    def fake_diagnose(args, factory):
        return {"diagnosis_status": "DIAGNOSED", "root_cause_category": "UNKNOWN", "incident_summary": "needs a human"}

    def fake_self_heal(manifest_arg, factory, *, repair_targets_file, confidence_threshold, output_dir):
        return {
            "repair_plan": {},
            "repair_result": {"repair_status": "BLOCKED"},
            "repair_verification": {"verification_status": "BLOCKED"},
        }

    monkeypatch.setattr("src.legacy.ask.run_diagnose_incident", fake_diagnose)
    monkeypatch.setattr("src.legacy.ask.run_self_healing", fake_self_heal)

    result = answer_question(
        QUESTION,
        manifest,
        diagnosis_model_client_factory=_poison_factory(),
        repair_model_client_factory=_poison_factory(),
        answer_model_client_factory=_poison_factory(),  # must never be reached
    )

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"
    assert result["answer"]["cited_metrics"] == []
    assert result["self_healing"]["verification_status"] == "BLOCKED"


def test_diagnose_incident_error_degrades_to_unreliable_data(tmp_path, monkeypatch):
    manifest, scenario_dir, _, _ = _write_manifest(tmp_path, validation_status="FAIL")

    def fake_diagnose(args, factory):
        raise DiagnoseIncidentError("model request failed")

    monkeypatch.setattr("src.legacy.ask.run_diagnose_incident", fake_diagnose)
    monkeypatch.setattr("src.legacy.ask.run_self_healing", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not reach repair")))

    result = answer_question(
        QUESTION,
        manifest,
        diagnosis_model_client_factory=_poison_factory(),
        repair_model_client_factory=_poison_factory(),
        answer_model_client_factory=_poison_factory(),
    )

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"
    assert "model request failed" in result["answer"]["caveats"][0]


def test_business_agent_failure_degrades_to_unreliable_data_instead_of_crashing(tmp_path, monkeypatch):
    manifest, scenario_dir, _, _ = _write_manifest(tmp_path, validation_status="PASS")

    # Model never submits -- exhausts max turns inside run_business_qa.
    responses = [ModelResponse(tool_calls=[ToolCall(id=str(i), name="get_portfolio_summary", arguments={})]) for i in range(6)]
    factory = lambda: ScriptedDiagnosisModelClient(responses)

    result = answer_question(
        QUESTION,
        manifest,
        diagnosis_model_client_factory=_poison_factory(),
        repair_model_client_factory=_poison_factory(),
        answer_model_client_factory=factory,
    )

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"


def test_ungrounded_model_answer_degrades_to_unreliable_data(tmp_path, monkeypatch):
    manifest, scenario_dir, _, _ = _write_manifest(tmp_path, validation_status="PASS")

    bad_submission = _valid_submission()
    bad_submission["cited_metrics"][0]["value"] = 1.0  # does not match the trusted 997522.36
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_portfolio_summary", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_ANSWER_TOOL_NAME, arguments=bad_submission)]),
    ]
    factory = lambda: ScriptedDiagnosisModelClient(responses)

    result = answer_question(
        QUESTION,
        manifest,
        diagnosis_model_client_factory=_poison_factory(),
        repair_model_client_factory=_poison_factory(),
        answer_model_client_factory=factory,
    )

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"


def test_missing_portfolio_summary_file_is_an_ask_error(tmp_path, monkeypatch):
    manifest, scenario_dir, _, portfolio_summary_file = _write_manifest(tmp_path, validation_status="PASS")
    portfolio_summary_file.unlink()

    with pytest.raises(AskError):
        answer_question(
            QUESTION,
            manifest,
            diagnosis_model_client_factory=_poison_factory(),
            repair_model_client_factory=_poison_factory(),
            answer_model_client_factory=_poison_factory(),
        )


def test_missing_validation_results_file_is_an_ask_error(tmp_path):
    manifest, scenario_dir, validation_results_file, _ = _write_manifest(tmp_path, validation_status="PASS")
    validation_results_file.unlink()

    with pytest.raises(AskError):
        answer_question(
            QUESTION,
            manifest,
            diagnosis_model_client_factory=_poison_factory(),
            repair_model_client_factory=_poison_factory(),
            answer_model_client_factory=_poison_factory(),
        )


def test_no_subprocess_usage_in_ask_module():
    tree = ast.parse(Path("src/legacy/ask.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(alias.name == "subprocess" for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.module != "subprocess"
