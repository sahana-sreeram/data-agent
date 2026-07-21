"""Tests for src/ask_lifecycle.py -- the CLI/orchestration layer tying the curated
lifecycle data, pipeline_run.json health check, and the Q&A agent together.

Uses ScriptedDiagnosisModelClient (fully generic, reused unchanged from
src/model_client.py) so the "answers normally when healthy" test needs no live
API call, only real curated S3 data (skips if unreachable). The "refuses when
unhealthy" test uses a small stub storage object instead of real S3, so it never
needs to touch (or risk corrupting) the real curated/pipeline_run.json.
"""

from __future__ import annotations

import pytest

from src.ask_lifecycle import answer_lifecycle_question
from src.lifecycle_business_agent import SUBMIT_ANSWER_TOOL_NAME
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall


class _StubStorage:
    """A minimal fake satisfying the read_json/read_parquet/exists/write_json surface
    answer_lifecycle_question needs, entirely in-memory -- no real S3 involved.
    """

    def __init__(self, json_objects: dict) -> None:
        self._json_objects = dict(json_objects)
        self.written: dict = {}

    def exists(self, path: str) -> bool:
        return path in self._json_objects

    def read_json(self, path: str):
        return self._json_objects[path]

    def write_json(self, path: str, value) -> None:
        self.written[path] = value


def test_refuses_to_answer_when_a_pipeline_failed_validation():
    storage = _StubStorage(
        {
            "curated/pipeline_run.json": {
                "overall_status": "FAILURE",
                "pipelines": {
                    "loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"},
                    "campaign_funnel": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
                },
            }
        }
    )

    def _factory():
        raise AssertionError("no model call should happen when the pipeline health check fails")

    result = answer_lifecycle_question("Which campaign funded the most loans?", storage, _factory)

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"
    assert "campaign_funnel" in result["answer"]["caveats"][0]
    assert "curated/lifecycle_answer.json" in storage.written


class _HealableStorage(_StubStorage):
    """Like _StubStorage, but re-reading curated/pipeline_run.json after the first read
    returns a healed (SUCCESS) snapshot -- simulating what a VERIFIED self-heal would have
    just written to it, without touching real S3/Spark.
    """

    def __init__(self, first_pipeline_run: dict, healed_pipeline_run: dict) -> None:
        super().__init__(
            {
                "curated/pipeline_run.json": first_pipeline_run,
                "context/business_rules.json": {},
                "context/metrics/loan_portfolio.json": {"metrics": {}},
                "context/metrics/campaign_funnel.json": {"metrics": {}},
                "context/metrics/underwriting_performance.json": {"metrics": {}},
                "context/metrics/payment_performance.json": {"metrics": {}},
                "context/metrics/delinquency_default.json": {"metrics": {}},
            }
        )
        self._healed_pipeline_run = healed_pipeline_run
        self._read_count = 0

    def read_json(self, path: str):
        if path == "curated/pipeline_run.json":
            self._read_count += 1
            return self._healed_pipeline_run if self._read_count > 1 else self._json_objects[path]
        return self._json_objects[path]


def test_attempts_self_heal_only_for_loan_portfolio_not_other_pipelines():
    storage = _StubStorage(
        {
            "curated/pipeline_run.json": {
                "overall_status": "FAILURE",
                "pipelines": {
                    "loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"},
                    "campaign_funnel": {"etl_status": "SUCCESS", "validation_status": "FAIL"},
                },
            }
        }
    )

    import src.ask_lifecycle as ask_lifecycle_module

    def _unexpected_heal(*args, **kwargs):
        raise AssertionError("self-heal should not be attempted when loan_portfolio itself is healthy")

    original = ask_lifecycle_module._attempt_self_heal
    ask_lifecycle_module._attempt_self_heal = _unexpected_heal
    try:
        result = answer_lifecycle_question(
            "Which campaign funded the most loans?", storage, lambda: (_ for _ in ()).throw(AssertionError("no QA model call"))
        )
    finally:
        ask_lifecycle_module._attempt_self_heal = original

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"
    assert result["self_heal"] is None


def test_verified_self_heal_falls_through_to_answering_normally():
    import src.ask_lifecycle as ask_lifecycle_module

    storage = _HealableStorage(
        first_pipeline_run={
            "overall_status": "FAILURE",
            "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "FAIL"}},
        },
        healed_pipeline_run={
            "overall_status": "SUCCESS",
            "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"}},
        },
    )

    original_attempt = ask_lifecycle_module._attempt_self_heal
    original_load_tools = ask_lifecycle_module._load_tools
    ask_lifecycle_module._attempt_self_heal = lambda storage, factory: {
        "verification_status": "VERIFIED",
        "summary": "Fixed the inner-join bug and promoted the correction.",
    }
    reached_qa = {}

    def _fake_load_tools(storage, business_rules, metrics_by_pipeline):
        reached_qa["called"] = True
        raise SystemExit("stop before needing full curated data -- reaching here is the assertion")

    ask_lifecycle_module._load_tools = _fake_load_tools
    try:
        with pytest.raises(SystemExit):
            answer_lifecycle_question("What is the total outstanding principal?", storage, lambda: None)
    finally:
        ask_lifecycle_module._attempt_self_heal = original_attempt
        ask_lifecycle_module._load_tools = original_load_tools

    assert reached_qa.get("called") is True


def test_unverified_self_heal_still_refuses_and_cites_the_repair_summary():
    import src.ask_lifecycle as ask_lifecycle_module

    storage = _StubStorage(
        {
            "curated/pipeline_run.json": {
                "overall_status": "FAILURE",
                "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "FAIL"}},
            }
        }
    )

    original = ask_lifecycle_module._attempt_self_heal
    ask_lifecycle_module._attempt_self_heal = lambda storage, factory: {
        "verification_status": "NOT_VERIFIED",
        "summary": "One or more deterministic checks failed; repository left untouched.",
    }
    try:
        result = answer_lifecycle_question(
            "What is the total outstanding principal?", storage, lambda: (_ for _ in ()).throw(AssertionError("no QA model call"))
        )
    finally:
        ask_lifecycle_module._attempt_self_heal = original

    assert result["answer"]["answer_status"] == "UNRELIABLE_DATA"
    assert "repository left untouched" in result["answer"]["caveats"][0]
    assert result["self_heal"] == "One or more deterministic checks failed; repository left untouched."


def test_missing_pipeline_run_file_raises_ask_lifecycle_error():
    from src.ask_lifecycle import AskLifecycleError

    storage = _StubStorage({})

    def _factory():
        raise AssertionError("no model call should happen when pipeline_run.json is missing")

    with pytest.raises(AskLifecycleError):
        answer_lifecycle_question("anything", storage, _factory)


@pytest.fixture
def real_curated_data_present(s3_storage):
    if not s3_storage.exists("curated/pipeline_run.json"):
        pytest.skip("curated lifecycle data not present in this environment")
    pipeline_run = s3_storage.read_json("curated/pipeline_run.json")
    if pipeline_run.get("overall_status") != "SUCCESS":
        pytest.skip("real curated data is not currently healthy -- run src.run_lifecycle_etl_pipelines first")


def test_answers_normally_against_real_healthy_curated_data(s3_storage, real_curated_data_present):
    real_summary = s3_storage.read_parquet("curated/loan_portfolio.parquet").iloc[0].to_dict()

    submission = {
        "answer_status": "ANSWERED",
        "question": "What is the total outstanding principal?",
        "answer_summary": f"The total outstanding principal is {real_summary['total_outstanding_principal']}.",
        "as_of_date": real_summary["as_of_date"],
        "cited_metrics": [
            {
                "metric_name": "total_outstanding_principal",
                "value": real_summary["total_outstanding_principal"],
                "source_reference": "get_loan_portfolio_summary",
            }
        ],
        "caveats": [],
    }
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_loan_portfolio_summary", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_ANSWER_TOOL_NAME, arguments=submission)]),
    ]

    result = answer_lifecycle_question(
        "What is the total outstanding principal?", s3_storage, lambda: ScriptedDiagnosisModelClient(responses)
    )

    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["answer"]["cited_metrics"][0]["value"] == real_summary["total_outstanding_principal"]
