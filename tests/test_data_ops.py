"""Tests for src/data_ops.py -- the data-operations console's presentation/orchestration
layer. Against real S3 (skips cleanly if unreachable) since PIPELINE_REGISTRY and the real
FileContextStore's context/ directory are both real, global state this module reads by
design -- mocking them would test a fake registry, not this one. No live model calls:
run_incident_response's model-touching paths are exercised with ScriptedDiagnosisModelClient.
"""

from __future__ import annotations

import pytest

from src.data_ops import data_product_estate, print_estate, run_incident_response
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall


def test_data_product_estate_has_one_row_per_registered_pipeline(s3_storage):
    rows = data_product_estate(s3_storage)
    assert {row["pipeline_name"] for row in rows} == set(PIPELINE_REGISTRY)
    for row in rows:
        assert row["context_provenance"] in ("human", "human+generated", "generated", "legacy_file")
        assert isinstance(row["open_conflicts"], int)


def test_print_estate_does_not_raise(s3_storage, capsys):
    print_estate(data_product_estate(s3_storage))
    out = capsys.readouterr().out
    assert "DATA PRODUCT ESTATE" in out
    assert "loan_portfolio" in out


def test_run_incident_response_requires_exactly_one_of_question_or_pipeline(s3_storage):
    def factory():
        raise AssertionError("no model call expected")

    with pytest.raises(ValueError):
        run_incident_response(s3_storage, factory)
    with pytest.raises(ValueError):
        run_incident_response(s3_storage, factory, question="q", pipeline_name="loan_portfolio")


def test_run_incident_response_reports_no_incident_for_a_healthy_pipeline(s3_storage, capsys):
    if not s3_storage.exists("curated/pipeline_run.json"):
        pytest.skip("curated lifecycle data not present in this environment")
    pipeline_run = s3_storage.read_json("curated/pipeline_run.json")
    entry = pipeline_run.get("pipelines", {}).get("loan_portfolio", {})
    if entry.get("etl_status") != "SUCCESS" or entry.get("validation_status") != "PASS":
        pytest.skip("loan_portfolio is not currently healthy in this environment")

    def factory():
        raise AssertionError("no model call needed for a trustworthy data product")

    result = run_incident_response(s3_storage, factory, pipeline_name="loan_portfolio", mode="create_pr")
    assert result == {"pipeline_name": "loan_portfolio", "self_heal": None, "candidate_answer": None}
    assert "PASS -- no incident" in capsys.readouterr().out


def test_run_incident_response_narrates_a_grounded_healthy_question(s3_storage, capsys):
    if not s3_storage.exists("curated/loan_portfolio.parquet"):
        pytest.skip("curated lifecycle data not present in this environment")
    real_value = s3_storage.read_parquet("curated/loan_portfolio.parquet").iloc[0]["total_outstanding_principal"]

    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_loan_portfolio_summary", arguments={})]),
        ModelResponse(
            tool_calls=[
                ToolCall(
                    id="2",
                    name="submit_answer",
                    arguments={
                        "answer_status": "ANSWERED",
                        "question": "What is our total outstanding principal?",
                        "answer_summary": f"Total outstanding principal is {real_value}.",
                        "as_of_date": "2026-07-20",
                        "cited_metrics": [
                            {
                                "metric_name": "total_outstanding_principal",
                                "value": real_value,
                                "source_reference": "get_loan_portfolio_summary",
                                "row_identifier": None,
                            }
                        ],
                        "caveats": [],
                    },
                )
            ]
        ),
    ]
    client = ScriptedDiagnosisModelClient(responses)
    result = run_incident_response(
        s3_storage, lambda: client, question="What is our total outstanding principal?", mode="create_pr"
    )
    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["self_heal"] is None
    out = capsys.readouterr().out
    assert "PROVENANCE: loan_portfolio.total_outstanding_principal" in out
    assert "Metric definition:" in out
