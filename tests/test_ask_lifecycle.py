"""Tests for src/ask_lifecycle.py -- the CLI/orchestration layer tying the curated
lifecycle data, pipeline_run.json health check, question-lineage-aware self-healing, and
the Q&A agent together.

Since the QA loop now always runs once (even when some pipeline is unhealthy, to discover
which pipeline(s) the question actually needed), most tests here monkeypatch _load_tools
to return a small, fixed LifecycleBusinessTools instead of building full fake curated
Parquet data for a stub storage -- the tool-loading plumbing itself is already covered by
tests/test_lifecycle_business_tools.py. Only the "answers normally against real data" test
uses real S3 data end-to-end.
"""

from __future__ import annotations

import pytest

import src.ask_lifecycle as ask_lifecycle_module
from src.ask_lifecycle import AskLifecycleError, answer_lifecycle_question
from src.lifecycle_business_agent import SUBMIT_ANSWER_TOOL_NAME
from src.lifecycle_business_tools import LifecycleBusinessTools
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall

_REAL_LOAD_TOOLS = ask_lifecycle_module._load_tools  # captured before any test monkeypatches it

CONTEXT_STUBS = {
    "context/business_rules.json": {},
    "context/metrics/loan_portfolio.json": {"metrics": {"total_outstanding_principal": {}}},
    "context/metrics/campaign_funnel.json": {"metrics": {}},
    "context/metrics/underwriting_performance.json": {"metrics": {}},
    "context/metrics/payment_performance.json": {"metrics": {}},
    "context/metrics/delinquency_default.json": {"metrics": {"default_rate": {}}},
}


class _StubStorage:
    """A minimal fake satisfying the read_json/exists/write_json surface
    answer_lifecycle_question needs, entirely in-memory -- no real S3 involved. Callers that
    reach _load_tools should monkeypatch it (see FIXED_TOOLS below) rather than extend this
    with real read_parquet fixtures.
    """

    def __init__(self, pipeline_run: dict, extra_json: dict | None = None) -> None:
        self._json_objects = {"curated/pipeline_run.json": pipeline_run, **CONTEXT_STUBS, **(extra_json or {})}
        self.written: dict = {}

    def exists(self, path: str) -> bool:
        return path in self._json_objects

    def read_json(self, path: str):
        return self._json_objects[path]

    def write_json(self, path: str, value) -> None:
        self.written[path] = value
        self._json_objects[path] = value


FIXED_TOOLS = LifecycleBusinessTools(
    loan_portfolio={"total_outstanding_principal": 1234.0, "as_of_date": "2026-07-20"},
    campaign_funnel=[{"campaign_id": "CMP1", "loans_funded": 5}],
    underwriting_performance=[{"breakdown_type": "risk_segment", "breakdown_value": "LOW", "approval_rate": 0.9}],
    underwriting_rejections={"LOW_CREDIT_SCORE": 3},
    payment_performance={"collection_rate": 0.95},
    delinquency_default=[{"breakdown_value": "ALL", "default_rate": 0.05}, {"breakdown_value": "HIGH", "default_rate": 0.1}],
    business_rules={},
    metrics_by_pipeline={},
)


def _submission(question: str, metric_name: str, value, source_reference: str) -> dict:
    return {
        "answer_status": "ANSWERED",
        "question": question,
        "answer_summary": f"{metric_name} is {value}.",
        "as_of_date": "2026-07-20",
        "cited_metrics": [
            {"metric_name": metric_name, "value": value, "source_reference": source_reference, "row_identifier": None}
        ],
        "caveats": [],
    }


def _loan_portfolio_responses(question: str = "What is the total outstanding principal?") -> list:
    return [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_loan_portfolio_summary", arguments={})]),
        ModelResponse(
            tool_calls=[
                ToolCall(
                    id="2", name=SUBMIT_ANSWER_TOOL_NAME,
                    arguments=_submission(question, "total_outstanding_principal", 1234.0, "get_loan_portfolio_summary"),
                )
            ]
        ),
    ]


def _compound_responses(question: str) -> list:
    """Calls both get_loan_portfolio_summary AND get_delinquency_default."""
    return [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_loan_portfolio_summary", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_delinquency_default", arguments={})]),
        ModelResponse(
            tool_calls=[
                ToolCall(
                    id="3", name=SUBMIT_ANSWER_TOOL_NAME,
                    arguments=_submission(question, "total_outstanding_principal", 1234.0, "get_loan_portfolio_summary"),
                )
            ]
        ),
    ]


@pytest.fixture(autouse=True)
def _fixed_tools(monkeypatch):
    import src.ask_lifecycle as ask_lifecycle_module

    monkeypatch.setattr(ask_lifecycle_module, "_load_tools", lambda storage, business_rules, metrics_by_pipeline: FIXED_TOOLS)


def test_missing_pipeline_run_file_raises_ask_lifecycle_error():
    storage = _StubStorage.__new__(_StubStorage)
    storage._json_objects = {}
    storage.written = {}

    def _factory():
        raise AssertionError("no model call should happen when pipeline_run.json is missing")

    with pytest.raises(AskLifecycleError):
        answer_lifecycle_question("anything", storage, _factory)


def test_fully_healthy_answers_normally_with_no_self_heal():
    storage = _StubStorage({"overall_status": "SUCCESS", "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"}}})
    factory = lambda: ScriptedDiagnosisModelClient(_loan_portfolio_responses())

    result = answer_lifecycle_question("What is the total outstanding principal?", storage, factory)

    assert result["question"] == "What is the total outstanding principal?"
    assert result["relevant_pipelines"] == ["loan_portfolio"]
    assert result["validation_failures"] == {}
    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["self_heal"] is None
    assert result["corrected_answer"] is None
    assert "curated/lifecycle_answer.json" in storage.written


def test_unrelated_pipeline_failure_does_not_block_or_trigger_heal():
    storage = _StubStorage(
        {
            "overall_status": "FAILURE",
            "pipelines": {
                "loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"},
                "campaign_funnel": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
            },
        }
    )
    factory = lambda: ScriptedDiagnosisModelClient(_loan_portfolio_responses())

    import src.ask_lifecycle as ask_lifecycle_module

    def _unexpected_heal(*args, **kwargs):
        raise AssertionError("self-heal should not be attempted for a pipeline the question never touched")

    monkeypatch_target = ask_lifecycle_module._attempt_self_heal
    ask_lifecycle_module._attempt_self_heal = _unexpected_heal
    try:
        result = answer_lifecycle_question("What is the total outstanding principal?", storage, factory)
    finally:
        ask_lifecycle_module._attempt_self_heal = monkeypatch_target

    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["self_heal"] is None


def test_relevant_pipeline_failure_heals_and_reanswers_when_verified():
    storage = _StubStorage(
        {"overall_status": "FAILURE", "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "FAIL"}}}
    )
    factory = lambda: ScriptedDiagnosisModelClient(_loan_portfolio_responses())

    import src.ask_lifecycle as ask_lifecycle_module

    heal_calls = []

    def _fake_heal(pipeline_name, storage, model_client_factory):
        heal_calls.append(pipeline_name)
        storage.write_json(
            "curated/pipeline_run.json",
            {"overall_status": "SUCCESS", "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"}}},
        )
        return {
            "run_id": "run1",
            "diagnosis": {"root_cause_category": "ETL_LOGIC", "root_cause": "inner join drops loans"},
            "repair_plan": {"patch": {"format": "UNIFIED_DIFF", "content": "--- a\n+++ b\n"}},
            "repair_result": {"repair_status": "APPLIED"},
            "repair_verification": {
                "verification_status": "VERIFIED",
                "summary": "Fixed and promoted.",
                "failed_checks_before": ["loan_count_reconciliation"],
            },
        }

    original = ask_lifecycle_module._attempt_self_heal
    ask_lifecycle_module._attempt_self_heal = _fake_heal
    try:
        result = answer_lifecycle_question("What is the total outstanding principal?", storage, factory)
    finally:
        ask_lifecycle_module._attempt_self_heal = original

    assert heal_calls == ["loan_portfolio"]
    assert result["relevant_pipelines"] == ["loan_portfolio"]
    assert result["validation_failures"] == {"loan_portfolio": ["loan_count_reconciliation"]}
    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["self_heal"]["loan_portfolio"]["repair_verification"]["summary"] == "Fixed and promoted."
    assert result["self_heal"]["loan_portfolio"]["diagnosis"]["root_cause_category"] == "ETL_LOGIC"
    assert result["corrected_answer"]["answer_status"] == "ANSWERED"


def test_relevant_pipeline_failure_not_verified_refuses_with_summary():
    storage = _StubStorage(
        {"overall_status": "FAILURE", "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "FAIL"}}}
    )
    factory = lambda: ScriptedDiagnosisModelClient(_loan_portfolio_responses())

    import src.ask_lifecycle as ask_lifecycle_module

    def _fake_heal(pipeline_name, storage, model_client_factory):
        return {
            "run_id": "run1",
            "diagnosis": {"root_cause_category": "ETL_LOGIC", "root_cause": "x"},
            "repair_plan": None,
            "repair_result": {"repair_status": "BLOCKED"},
            "repair_verification": {"verification_status": "NOT_VERIFIED", "summary": "Repository left untouched.", "failed_checks_before": []},
        }

    original = ask_lifecycle_module._attempt_self_heal
    ask_lifecycle_module._attempt_self_heal = _fake_heal
    try:
        result = answer_lifecycle_question("What is the total outstanding principal?", storage, factory)
    finally:
        ask_lifecycle_module._attempt_self_heal = original

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"
    assert "loan_portfolio" in result["answer"]["caveats"][0]
    assert "Repository left untouched." in result["answer"]["caveats"][0]
    assert result["self_heal"]["loan_portfolio"]["repair_verification"]["summary"] == "Repository left untouched."
    assert result["corrected_answer"] is None


# --- Multiple simultaneous failures --------------------------------------------------------


def test_two_simultaneous_failures_only_the_relevant_one_heals():
    storage = _StubStorage(
        {
            "overall_status": "FAILURE",
            "pipelines": {
                "loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
                "delinquency_default": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
            },
        }
    )
    # This question only calls get_loan_portfolio_summary -- delinquency_default is
    # irrelevant even though it's also currently broken.
    factory = lambda: ScriptedDiagnosisModelClient(_loan_portfolio_responses())

    import src.ask_lifecycle as ask_lifecycle_module

    heal_calls = []

    def _fake_heal(pipeline_name, storage, model_client_factory):
        heal_calls.append(pipeline_name)
        storage.write_json(
            "curated/pipeline_run.json",
            {
                "overall_status": "FAILURE",
                "pipelines": {
                    "loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"},
                    "delinquency_default": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
                },
            },
        )
        return {
            "run_id": "run1",
            "diagnosis": {"root_cause_category": "ETL_LOGIC"},
            "repair_plan": None,
            "repair_result": {"repair_status": "APPLIED"},
            "repair_verification": {"verification_status": "VERIFIED", "summary": "Fixed loan_portfolio.", "failed_checks_before": []},
        }

    original = ask_lifecycle_module._attempt_self_heal
    ask_lifecycle_module._attempt_self_heal = _fake_heal
    try:
        result = answer_lifecycle_question("What is the total outstanding principal?", storage, factory)
    finally:
        ask_lifecycle_module._attempt_self_heal = original

    assert heal_calls == ["loan_portfolio"]
    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["self_heal"]["loan_portfolio"]["repair_verification"]["summary"] == "Fixed loan_portfolio."
    assert result["corrected_answer"]["answer_status"] == "ANSWERED"


def test_two_simultaneous_failures_both_relevant_one_verifies_one_does_not():
    storage = _StubStorage(
        {
            "overall_status": "FAILURE",
            "pipelines": {
                "loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
                "delinquency_default": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
            },
        }
    )
    question = "What is the total outstanding principal and the overall default rate?"
    factory = lambda: ScriptedDiagnosisModelClient(_compound_responses(question))

    import src.ask_lifecycle as ask_lifecycle_module

    heal_calls = []

    def _fake_heal(pipeline_name, storage, model_client_factory):
        heal_calls.append(pipeline_name)
        if pipeline_name == "loan_portfolio":
            pr = storage.read_json("curated/pipeline_run.json")
            pr["pipelines"]["loan_portfolio"] = {"etl_status": "SUCCESS", "validation_status": "PASS"}
            storage.write_json("curated/pipeline_run.json", pr)
            return {
                "run_id": "run1",
                "diagnosis": {"root_cause_category": "ETL_LOGIC"},
                "repair_plan": None,
                "repair_result": {"repair_status": "APPLIED"},
                "repair_verification": {"verification_status": "VERIFIED", "summary": "Fixed loan_portfolio.", "failed_checks_before": []},
            }
        return {
            "run_id": "run2",
            "diagnosis": {"root_cause_category": "UNKNOWN"},
            "repair_plan": None,
            "repair_result": {"repair_status": "BLOCKED"},
            "repair_verification": {
                "verification_status": "NOT_VERIFIED",
                "summary": "Could not fix delinquency_default.",
                "failed_checks_before": ["default_rate_reconciliation"],
            },
        }

    original = ask_lifecycle_module._attempt_self_heal
    ask_lifecycle_module._attempt_self_heal = _fake_heal
    try:
        result = answer_lifecycle_question(question, storage, factory)
    finally:
        ask_lifecycle_module._attempt_self_heal = original

    assert set(heal_calls) == {"loan_portfolio", "delinquency_default"}
    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"
    assert "delinquency_default" in result["answer"]["caveats"][0]
    assert "loan_portfolio" not in result["answer"]["caveats"][0].split("failed validation in:")[1].split(".")[0]
    assert result["self_heal"]["loan_portfolio"]["repair_verification"]["summary"] == "Fixed loan_portfolio."
    assert result["self_heal"]["delinquency_default"]["repair_verification"]["summary"] == "Could not fix delinquency_default."
    assert result["validation_failures"]["delinquency_default"] == ["default_rate_reconciliation"]
    assert result["corrected_answer"] is None


# --- Against real S3 data -------------------------------------------------------------------


@pytest.fixture
def real_curated_data_present(s3_storage):
    if not s3_storage.exists("curated/pipeline_run.json"):
        pytest.skip("curated lifecycle data not present in this environment")
    pipeline_run = s3_storage.read_json("curated/pipeline_run.json")
    if pipeline_run.get("overall_status") != "SUCCESS":
        pytest.skip("real curated data is not currently healthy -- run src.run_lifecycle_etl_pipelines first")


def test_answers_normally_against_real_healthy_curated_data(s3_storage, real_curated_data_present, monkeypatch):
    # This one test exercises the REAL _load_tools against real curated S3 data --
    # undo the module-wide FIXED_TOOLS monkeypatch from the autouse fixture above.
    monkeypatch.setattr(ask_lifecycle_module, "_load_tools", _REAL_LOAD_TOOLS)
    real_summary = s3_storage.read_parquet("curated/loan_portfolio.parquet").iloc[0].to_dict()

    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_loan_portfolio_summary", arguments={})]),
        ModelResponse(
            tool_calls=[
                ToolCall(
                    id="2", name=SUBMIT_ANSWER_TOOL_NAME,
                    arguments=_submission(
                        "What is the total outstanding principal?", "total_outstanding_principal",
                        real_summary["total_outstanding_principal"], "get_loan_portfolio_summary",
                    ),
                )
            ]
        ),
    ]

    result = answer_lifecycle_question(
        "What is the total outstanding principal?", s3_storage, lambda: ScriptedDiagnosisModelClient(responses)
    )

    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["answer"]["cited_metrics"][0]["value"] == real_summary["total_outstanding_principal"]
