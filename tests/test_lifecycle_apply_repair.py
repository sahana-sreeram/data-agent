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
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.lifecycle_repair_agent import SUBMIT_REPAIR_PLAN_TOOL_NAME
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall
from src.repair_models import RepairEligibility, evaluate_repair_eligibility

PIPELINE_NAME = "loan_portfolio"
ETL_SOURCE_FILE = PIPELINE_REGISTRY[PIPELINE_NAME].etl_source_file

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
    """Stands in for S3Storage in these tests -- only read_json/exists are ever called by
    build_lifecycle_repair_tools, so that's all this needs to implement."""

    def read_json(self, path: str) -> dict:
        return {
            "context/business_rules.json": {"successful_payment_statuses": ["PAID"]},
            "context/lineage.json": {"datasets": {"curated.loan_portfolio": {"path": "x", "depends_on": []}}},
            "context/metrics/loan_portfolio.json": {"metrics": {"loan_count": {"formula": "count(loans)"}}},
        }[path]

    def exists(self, path: str) -> bool:
        # No pipeline_configuration_file backing in this fake -- get_pipeline_configuration
        # reports unavailable, exactly like any pipeline without one registered.
        return False


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
        PIPELINE_NAME,
        _FakeStorage(), diagnosis, VALIDATION_RESULTS, factory, repair_targets_file=str(_repair_targets_file(tmp_path))
    )
    assert result["repair_status"] == "NO_REPAIR"
    assert plan_dict["repair_decision"] == "NO_SAFE_REPAIR"


def test_ineligible_root_cause_is_blocked_without_calling_model(tmp_path):
    diagnosis = _diagnosis(root_cause_category="SOURCE_CONTRACT_CHANGE")

    def factory():
        raise AssertionError("model should not be called when eligibility blocks the incident")

    plan_dict, result = run_apply_lifecycle_repair(
        PIPELINE_NAME,
        _FakeStorage(), diagnosis, VALIDATION_RESULTS, factory, repair_targets_file=str(_repair_targets_file(tmp_path))
    )
    assert result["repair_status"] == "BLOCKED"
    assert plan_dict["repair_decision"] == "HUMAN_REVIEW_REQUIRED"


def test_human_approved_categories_unlocks_a_normally_refused_category(tmp_path):
    """Empty by default (the test above proves that): SOURCE_CONTRACT_CHANGE stays BLOCKED.
    Explicitly approving it for this one call lets it reach the repair model instead --
    proving the override is real, not just documented, while every other call site (which
    never passes it) is completely unaffected."""
    diagnosis = _diagnosis(root_cause_category="SOURCE_CONTRACT_CHANGE")
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=_code_submission())])]

    plan_dict, result = run_apply_lifecycle_repair(
        PIPELINE_NAME,
        _FakeStorage(),
        diagnosis,
        VALIDATION_RESULTS,
        lambda: ScriptedDiagnosisModelClient(responses),
        repair_targets_file=str(_repair_targets_file(tmp_path)),
        human_approved_categories=frozenset({"SOURCE_CONTRACT_CHANGE"}),
    )
    assert plan_dict["repair_decision"] == "PROPOSE_REPAIR"
    assert result["repair_status"] == "APPLIED"


def test_configuration_change_target_applies_in_isolation(tmp_path):
    """loan_portfolio's registered CONFIGURATION_CHANGE target (context/pipeline_rules/
    loan_portfolio.json, see context/repair_targets.json) lets a SOURCE_CONTRACT_CHANGE
    incident -- once human-approved -- repoint which already-approved business-rules file it
    reads, WITHOUT ever touching the ETL source or the shared business_rules.json. Uses the
    REAL repair_targets.json (default repair_targets_file) since this target is real,
    checked-in registry data, not a test fixture."""
    target_file = "context/pipeline_rules/loan_portfolio.json"
    # Whichever of the two registered allowed_values this pipeline's real, currently-checked-in
    # pointer does NOT already say -- this file is a genuine, mutable CONFIGURATION_CHANGE
    # repair target (see context/repair_targets.json), so a previously-accepted repair may
    # have already flipped it for real; hardcoding one direction would make this test flaky
    # relative to the repo's actual current state instead of testing the real behavior.
    original_content = Path(target_file).read_text()
    current_value = json.loads(original_content)["business_rules_file"]
    allowed_values = ("context/business_rules.json", "context/business_rules_demo.json")
    new_value = next(v for v in allowed_values if v != current_value)

    diagnosis = _diagnosis(
        root_cause_category="SOURCE_CONTRACT_CHANGE",
        recommended_fix={"target_file": target_file, "change_summary": f"point at {new_value}", "scope": "MINIMAL"},
    )
    submission = {
        "repair_decision": "PROPOSE_REPAIR",
        "repair_type": "CONFIGURATION_CHANGE",
        "incident_id": PIPELINE_NAME,
        "diagnosis_reference": "x",
        "root_cause_addressed": "x",
        "target_file": target_file,
        "target_symbol_or_setting": "business_rules_file",
        "current_behavior": f"reads {current_value}",
        "proposed_behavior": f"reads {new_value}",
        "change_description": f"point business_rules_file at {new_value}",
        "patch": {
            "format": "STRUCTURED_CONFIG_EDIT",
            "content": {"operations": [{"field": "business_rules_file", "value": new_value}]},
        },
        "files_expected_to_change": [target_file],
        "files_expected_not_to_change": ["context/business_rules.json"],
        "verification_steps": ["rerun loan_portfolio ETL", "rerun validate_loan_portfolio"],
        "rollback_description": "revert the pointer",
        "risk_level": "LOW",
        "assumptions": [],
        "evidence_references": ["get_relevant_etl_source"],
    }
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=submission)])]

    plan_dict, result = run_apply_lifecycle_repair(
        PIPELINE_NAME,
        _FakeStorage(),
        diagnosis,
        VALIDATION_RESULTS,
        lambda: ScriptedDiagnosisModelClient(responses),
        human_approved_categories=frozenset({"SOURCE_CONTRACT_CHANGE"}),
    )

    assert result["repair_status"] == "APPLIED"
    assert result["target_file"] == target_file
    assert Path(target_file).read_text() == original_content  # the real file is untouched

    workspace_dir = Path(result["workspace_dir"])
    patched = json.loads((workspace_dir / target_file).read_text())
    assert patched["business_rules_file"] == new_value


def test_eligible_incident_applies_code_change_in_isolation(tmp_path):
    diagnosis = _diagnosis()
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=_code_submission())])]
    original_content = Path(ETL_SOURCE_FILE).read_text()

    plan_dict, result = run_apply_lifecycle_repair(
        PIPELINE_NAME,
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
            PIPELINE_NAME,
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
        PIPELINE_NAME,
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
        PIPELINE_NAME,
        _FakeStorage(),
        diagnosis,
        VALIDATION_RESULTS,
        lambda: ScriptedDiagnosisModelClient(responses),
        repair_targets_file=str(_repair_targets_file(tmp_path)),
    )
    assert result["repair_status"] == "NO_REPAIR"


# --- Eligibility/allowlist consistency across every registered pipeline -------------------


@pytest.mark.parametrize("pipeline_name", list(PIPELINE_REGISTRY))
def test_every_pipelines_etl_source_is_eligible_and_registered_as_a_repair_target(pipeline_name):
    spec = PIPELINE_REGISTRY[pipeline_name]
    diagnosis = {
        "diagnosis_status": "DIAGNOSED",
        "root_cause_category": "ETL_LOGIC",
        "confidence": "HIGH",
        "evidence": [{"source_type": "ETL_SOURCE", "source_reference": "get_relevant_etl_source"}],
        "recommended_fix": {"target_file": spec.etl_source_file, "change_summary": "x", "scope": "MINIMAL"},
    }
    decision = evaluate_repair_eligibility(diagnosis, allowed_target_files={spec.etl_source_file})
    assert decision.decision == RepairEligibility.ELIGIBLE_FOR_REPAIR

    real_targets = json.loads(Path("context/repair_targets.json").read_text())["targets"]
    assert spec.etl_source_file in real_targets
    assert real_targets[spec.etl_source_file]["repair_type"] == "CODE_CHANGE"
    assert set(real_targets[spec.etl_source_file].get("editable_symbols", [])) == set(spec.etl_function_names)
    assert Path(spec.etl_source_file).exists()
