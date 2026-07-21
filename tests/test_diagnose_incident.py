"""Tests for the diagnose_incident CLI/orchestration layer.

Covers: clean-scenario NO_INCIDENT (no model call), failed-scenario agent
run, malformed-output rejection, missing-artifact handling, and safety
(source/raw data untouched, no subprocess usage anywhere in these modules).
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from src.diagnose_incident import DiagnoseIncidentError, parse_args, run_diagnose_incident
from src.diagnosis_agent import SUBMIT_DIAGNOSIS_TOOL_NAME
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall

LOANS = [{"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"}]
CLEAN_PAYMENTS = [{"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 1000.0, "payment_status": "PAID"}]
DIRTY_PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 800.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000001", "amount_paid": 200.0, "payment_status": "SETTLED"},
]
BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "valid_payment_statuses": ["PAID", "MISSED", "SCHEDULED", "LATE", "FAILED"],
}
VALIDATION_RULES = {"tolerance": {"currency": 0.01, "count": 0}, "rules": []}
LINEAGE = {"datasets": {"processed.portfolio_summary": {"path": "x", "depends_on": []}}}
DATA_DICTIONARY = {"portfolio_summary": {"fields": {"total_successful_payments": {"type": "float"}}}}

PASS_VALIDATION_RESULTS = {"overall_status": "PASS", "checks": []}
FAIL_VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [
        {
            "id": "payment_status_enum_valid",
            "status": "FAIL",
            "description": "d",
            "expected": ["PAID"],
            "actual": ["PAID", "SETTLED"],
            "difference": None,
            "details": "unexpected values found: ['SETTLED']",
        }
    ],
}


def _write_common_fixtures(tmp_path: Path, payments: list, validation_results: dict, summary: dict) -> dict:
    paths = {
        "loans_file": tmp_path / "loans.json",
        "payments_file": tmp_path / "payments.json",
        "summary_file": tmp_path / "summary.json",
        "validation_results_file": tmp_path / "validation_results.json",
        "business_rules_file": tmp_path / "business_rules.json",
        "validation_rules_file": tmp_path / "validation_rules.json",
        "lineage_file": tmp_path / "lineage.json",
        "data_dictionary_file": tmp_path / "data_dictionary.json",
        "pipeline_run_file": tmp_path / "pipeline_run.json",  # deliberately not written -> "not available"
        "output_dir": tmp_path / "processed",
    }
    paths["loans_file"].write_text(json.dumps(LOANS))
    paths["payments_file"].write_text(json.dumps(payments))
    paths["summary_file"].write_text(json.dumps(summary))
    paths["validation_results_file"].write_text(json.dumps(validation_results))
    paths["business_rules_file"].write_text(json.dumps(BUSINESS_RULES))
    paths["validation_rules_file"].write_text(json.dumps(VALIDATION_RULES))
    paths["lineage_file"].write_text(json.dumps(LINEAGE))
    paths["data_dictionary_file"].write_text(json.dumps(DATA_DICTIONARY))
    return paths


def _build_args(paths: dict):
    return parse_args(
        [
            "--loans-file", str(paths["loans_file"]),
            "--payments-file", str(paths["payments_file"]),
            "--summary-file", str(paths["summary_file"]),
            "--validation-results-file", str(paths["validation_results_file"]),
            "--business-rules-file", str(paths["business_rules_file"]),
            "--validation-rules-file", str(paths["validation_rules_file"]),
            "--lineage-file", str(paths["lineage_file"]),
            "--data-dictionary-file", str(paths["data_dictionary_file"]),
            "--pipeline-run-file", str(paths["pipeline_run_file"]),
            "--output-dir", str(paths["output_dir"]),
        ]
    )


def _diagnosed_submission() -> dict:
    return {
        "diagnosis_status": "DIAGNOSED",
        "incident_summary": "SETTLED payments are excluded from successful-payment totals.",
        "affected_metrics": ["total_successful_payments"],
        "root_cause_category": "SOURCE_CONTRACT_CHANGE",
        "initiating_event": None,
        "root_cause": "SETTLED is not recognized by business_rules.json.",
        "reasoning_summary": "SETTLED behaves like PAID but is excluded by the current rule.",
        "evidence": [
            {
                "source_type": "RAW_DATA",
                "source_reference": "get_payment_status_counts",
                "finding": "SETTLED present with a nonzero count.",
                "expected": "no SETTLED",
                "actual": "1 SETTLED",
            }
        ],
        "recommended_fix": {
            "target_file": "context/business_rules.json",
            "change_summary": "Add SETTLED if it represents a received payment.",
            "scope": "MINIMAL",
        },
        "confidence": "MEDIUM",
        "uncertainties": [],
        "additional_evidence_needed": [],
    }


def test_clean_validation_returns_no_incident_without_calling_model(tmp_path):
    summary = {"total_original_principal": 1000.0, "total_successful_payments": 1000.0, "total_outstanding_balance": 0.0}
    paths = _write_common_fixtures(tmp_path, CLEAN_PAYMENTS, PASS_VALIDATION_RESULTS, summary)
    args = _build_args(paths)

    def factory():
        raise AssertionError("model client factory should not be called for a clean validation run")

    result = run_diagnose_incident(args, factory)

    assert result["diagnosis_status"] == "NO_INCIDENT"
    written = json.loads((paths["output_dir"] / "diagnosis.json").read_text())
    assert written["diagnosis_status"] == "NO_INCIDENT"


def test_failed_validation_runs_agent_and_writes_diagnosis(tmp_path):
    summary = {"total_original_principal": 1000.0, "total_successful_payments": 800.0, "total_outstanding_balance": 200.0}
    paths = _write_common_fixtures(tmp_path, DIRTY_PAYMENTS, FAIL_VALIDATION_RESULTS, summary)
    args = _build_args(paths)

    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_payment_status_counts", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=_diagnosed_submission())]),
    ]

    result = run_diagnose_incident(args, lambda: ScriptedDiagnosisModelClient(responses))

    assert result["diagnosis_status"] == "DIAGNOSED"
    assert "total_successful_payments" in result["affected_metrics"]
    assert len(result["evidence"]) == 1
    written = json.loads((paths["output_dir"] / "diagnosis.json").read_text())
    assert written["diagnosis_status"] == "DIAGNOSED"


def test_malformed_model_output_is_a_controlled_failure_and_writes_nothing(tmp_path):
    summary = {"total_original_principal": 1000.0, "total_successful_payments": 800.0, "total_outstanding_balance": 200.0}
    paths = _write_common_fixtures(tmp_path, DIRTY_PAYMENTS, FAIL_VALIDATION_RESULTS, summary)
    args = _build_args(paths)

    bad_submission = {"diagnosis_status": "NOT_A_REAL_STATUS"}
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad_submission)])]

    with pytest.raises(DiagnoseIncidentError):
        run_diagnose_incident(args, lambda: ScriptedDiagnosisModelClient(responses))

    assert not (paths["output_dir"] / "diagnosis.json").exists()


def test_missing_validation_results_file_is_a_controlled_failure(tmp_path):
    summary = {"total_original_principal": 0.0, "total_successful_payments": 0.0, "total_outstanding_balance": 0.0}
    paths = _write_common_fixtures(tmp_path, CLEAN_PAYMENTS, PASS_VALIDATION_RESULTS, summary)
    paths["validation_results_file"].unlink()
    args = _build_args(paths)

    with pytest.raises(DiagnoseIncidentError):
        run_diagnose_incident(args, lambda: ScriptedDiagnosisModelClient([]))


def test_missing_business_rules_file_during_failed_run_is_a_controlled_failure(tmp_path):
    summary = {"total_original_principal": 1000.0, "total_successful_payments": 800.0, "total_outstanding_balance": 200.0}
    paths = _write_common_fixtures(tmp_path, DIRTY_PAYMENTS, FAIL_VALIDATION_RESULTS, summary)
    paths["business_rules_file"].unlink()
    args = _build_args(paths)

    with pytest.raises(DiagnoseIncidentError):
        run_diagnose_incident(args, lambda: ScriptedDiagnosisModelClient([]))


def test_safety_source_and_raw_files_untouched_by_full_diagnosis_run(tmp_path):
    transform_path = Path("src/transform.py")
    raw_loans_path = Path("data/raw/loans.json")
    before_transform = transform_path.read_bytes()
    before_loans = raw_loans_path.read_bytes()

    summary = {"total_original_principal": 1000.0, "total_successful_payments": 800.0, "total_outstanding_balance": 200.0}
    paths = _write_common_fixtures(tmp_path, DIRTY_PAYMENTS, FAIL_VALIDATION_RESULTS, summary)
    args = _build_args(paths)

    insufficient = {
        "diagnosis_status": "INSUFFICIENT_EVIDENCE",
        "incident_summary": "x",
        "affected_metrics": [],
        "root_cause_category": "UNKNOWN",
        "initiating_event": None,
        "root_cause": "x",
        "reasoning_summary": "x",
        "evidence": [],
        "recommended_fix": None,
        "confidence": "LOW",
        "uncertainties": [],
        "additional_evidence_needed": ["need more data"],
    }
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=insufficient)])]

    run_diagnose_incident(args, lambda: ScriptedDiagnosisModelClient(responses))

    assert transform_path.read_bytes() == before_transform
    assert raw_loans_path.read_bytes() == before_loans


def test_no_subprocess_usage_anywhere_in_diagnosis_modules():
    diagnosis_modules = [
        "src/diagnostic_tools.py",
        "src/diagnosis_agent.py",
        "src/diagnose_incident.py",
        "src/model_client.py",
        "src/diagnosis_models.py",
    ]
    for module_path in diagnosis_modules:
        tree = ast.parse(Path(module_path).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(alias.name == "subprocess" for alias in node.names), module_path
            if isinstance(node, ast.ImportFrom):
                assert node.module != "subprocess", module_path
