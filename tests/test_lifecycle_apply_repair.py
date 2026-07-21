"""Tests for deterministic repair application for the loan_portfolio lifecycle pipeline:
eligibility gate, policy validation, and isolated-workspace patch application. Parallel to
tests/test_apply_repair.py. No live model calls -- ScriptedDiagnosisModelClient scripts the
repair agent's tool calls and final plan. No real S3 access needed -- a small fake storage
stub stands in for context reads.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.lifecycle_apply_repair import ApplyLifecycleRepairError, run_apply_lifecycle_repair
from src.lifecycle_repair_agent import SUBMIT_REPAIR_PLAN_TOOL_NAME
from src.lifecycle_repair_tools import ETL_SOURCE_FILE
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall

VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [{"id": "loan_count_reconciliation", "status": "FAIL", "expected": 162, "actual": 160}],
}

# Blank context lines must be a single space (the diff context marker) rather than a truly
# empty string -- apply_unified_diff treats a truly empty diff line as a no-op filler, not a
# blank-line context assertion, so it must not be skipped here or the hunk's context lines
# won't be contiguous with the real file's blank lines.
REAL_DIFF = "\n".join(
    [
        "--- a/src/etl_spark_loan_portfolio.py",
        "+++ b/src/etl_spark_loan_portfolio.py",
        "@@",
        " from src.storage import S3Storage",
        " ",
        '-DEFAULT_AS_OF_DATE = "2026-07-20"',
        '+DEFAULT_AS_OF_DATE = "2026-07-20"  # patched',
        " ",
        " ",
        " def compute_loan_portfolio(spark: SparkSession, business_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE) -> DataFrame:",
        "",
    ]
)


class _FakeStorage:
    """Stands in for S3Storage in these tests -- only read_json is ever called by
    build_lifecycle_repair_tools, so that's all this needs to implement."""

    def read_json(self, path: str) -> dict:
        return {
            "context/business_rules.json": {"successful_payment_statuses": ["PAID"]},
            "context/lineage.json": {"datasets": {"curated.loan_portfolio": {"path": "x", "depends_on": []}}},
            "context/metrics/loan_portfolio.json": {"metrics": {"loan_count": {"formula": "count(loans)"}}},
        }[path]


def _diagnosis(**overrides) -> dict:
    base = {
        "diagnosis_status": "DIAGNOSED",
        "root_cause_category": "ETL_LOGIC",
        "confidence": "HIGH",
        "incident_summary": "inner join drops zero-net-payment loans",
        "root_cause": "compute_loan_portfolio uses how='inner' instead of how='left'",
        "evidence": [
            {"source_type": "ETL_SOURCE", "source_reference": "get_relevant_etl_source", "finding": "x", "expected": None, "actual": None}
        ],
        "recommended_fix": {"target_file": ETL_SOURCE_FILE, "change_summary": "use how='left'", "scope": "MINIMAL"},
    }
    base.update(overrides)
    return base


def _repair_targets_file(tmp_path: Path) -> Path:
    path = tmp_path / "repair_targets.json"
    path.write_text(
        json.dumps({"targets": {ETL_SOURCE_FILE: {"repair_type": "CODE_CHANGE", "editable_symbols": ["compute_loan_portfolio"]}}})
    )
    return path


def _code_submission(diff: str = REAL_DIFF) -> dict:
    return {
        "repair_decision": "PROPOSE_REPAIR",
        "repair_type": "CODE_CHANGE",
        "incident_id": "loan_portfolio",
        "diagnosis_reference": "inner join drops zero-net-payment loans",
        "root_cause_addressed": "inner join drops zero-net-payment loans",
        "target_file": ETL_SOURCE_FILE,
        "target_symbol_or_setting": "compute_loan_portfolio",
        "current_behavior": "drops loans with no net PAID/REVERSED payment",
        "proposed_behavior": "keeps all loans, treating missing net_paid as 0.0",
        "change_description": "annotate the constant as a minimal, real, applicable diff",
        "patch": {"format": "UNIFIED_DIFF", "content": diff},
        "files_expected_to_change": [ETL_SOURCE_FILE],
        "files_expected_not_to_change": [],
        "verification_steps": ["rerun ETL", "rerun validation"],
        "rollback_description": "revert the diff",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": ["get_relevant_etl_source"],
    }


def test_no_incident_short_circuits_without_calling_model(tmp_path):
    diagnosis = _diagnosis(diagnosis_status="NO_INCIDENT")

    def factory():
        raise AssertionError("model should not be called for NO_INCIDENT")

    plan_dict, result = run_apply_lifecycle_repair(
        _FakeStorage(), diagnosis, VALIDATION_RESULTS, factory, repair_targets_file=str(_repair_targets_file(tmp_path))
    )
    assert result["repair_status"] == "NO_REPAIR"
    assert plan_dict["repair_decision"] == "NO_SAFE_REPAIR"


def test_ineligible_root_cause_is_blocked_without_calling_model(tmp_path):
    diagnosis = _diagnosis(root_cause_category="SOURCE_CONTRACT_CHANGE")

    def factory():
        raise AssertionError("model should not be called when eligibility blocks the incident")

    plan_dict, result = run_apply_lifecycle_repair(
        _FakeStorage(), diagnosis, VALIDATION_RESULTS, factory, repair_targets_file=str(_repair_targets_file(tmp_path))
    )
    assert result["repair_status"] == "BLOCKED"
    assert plan_dict["repair_decision"] == "HUMAN_REVIEW_REQUIRED"


def test_eligible_incident_applies_code_change_in_isolation(tmp_path):
    diagnosis = _diagnosis()
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=_code_submission())])]
    original_content = Path(ETL_SOURCE_FILE).read_text()

    plan_dict, result = run_apply_lifecycle_repair(
        _FakeStorage(),
        diagnosis,
        VALIDATION_RESULTS,
        lambda: ScriptedDiagnosisModelClient(responses),
        repair_targets_file=str(_repair_targets_file(tmp_path)),
    )

    assert result["repair_status"] == "APPLIED"
    assert result["changed_files"] == [ETL_SOURCE_FILE]
    assert result["original_hashes"] != result["repaired_hashes"]

    # The REAL file must be untouched -- only the isolated workspace copy changed.
    assert Path(ETL_SOURCE_FILE).read_text() == original_content

    workspace_dir = Path(result["workspace_dir"])
    patched_path = workspace_dir / ETL_SOURCE_FILE
    assert patched_path.exists()
    assert '# patched' in patched_path.read_text()

    all_files = [p for p in workspace_dir.rglob("*") if p.is_file()]
    assert len(all_files) == 1


def test_malformed_model_output_raises_apply_lifecycle_repair_error(tmp_path):
    diagnosis = _diagnosis()
    bad = {"repair_decision": "NOT_A_REAL_DECISION"}
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=bad)])]

    with pytest.raises(ApplyLifecycleRepairError):
        run_apply_lifecycle_repair(
            _FakeStorage(),
            diagnosis,
            VALIDATION_RESULTS,
            lambda: ScriptedDiagnosisModelClient(responses),
            repair_targets_file=str(_repair_targets_file(tmp_path)),
        )


def test_patch_producing_no_change_is_blocked(tmp_path):
    diagnosis = _diagnosis()
    original_content = Path(ETL_SOURCE_FILE).read_text()
    # A diff whose + line is identical to its - line -- applies cleanly but is a no-op.
    no_op_diff = "\n".join(
        [
            "--- a/src/etl_spark_loan_portfolio.py",
            "+++ b/src/etl_spark_loan_portfolio.py",
            "@@",
            " from src.storage import S3Storage",
            " ",
            '-DEFAULT_AS_OF_DATE = "2026-07-20"',
            '+DEFAULT_AS_OF_DATE = "2026-07-20"',
            " ",
            " ",
            " def compute_loan_portfolio(spark: SparkSession, business_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE) -> DataFrame:",
            "",
        ]
    )
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=_code_submission(no_op_diff))])]

    plan_dict, result = run_apply_lifecycle_repair(
        _FakeStorage(),
        diagnosis,
        VALIDATION_RESULTS,
        lambda: ScriptedDiagnosisModelClient(responses),
        repair_targets_file=str(_repair_targets_file(tmp_path)),
    )
    assert result["repair_status"] == "BLOCKED"
    assert result["plan_policy_status"] == "FAIL"
    assert Path(ETL_SOURCE_FILE).read_text() == original_content


def test_no_safe_repair_decision_yields_no_repair_status(tmp_path):
    diagnosis = _diagnosis()
    raw = {
        "repair_decision": "NO_SAFE_REPAIR",
        "repair_type": "NONE",
        "incident_id": "loan_portfolio",
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

    plan_dict, result = run_apply_lifecycle_repair(
        _FakeStorage(),
        diagnosis,
        VALIDATION_RESULTS,
        lambda: ScriptedDiagnosisModelClient(responses),
        repair_targets_file=str(_repair_targets_file(tmp_path)),
    )
    assert result["repair_status"] == "NO_REPAIR"
