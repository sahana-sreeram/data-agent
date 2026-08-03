"""Tests for src/lifecycle_business_agent.py's tool-calling loop. No dedicated test file
existed for this loop before -- coverage was only indirect, via tests/test_ask_lifecycle.py's
stubs. This exercises the full loop (dispatch, called_tool_calls recording, grounding)
against ScriptedDiagnosisModelClient, with particular attention to the new bounded query
tools and the row_identifier grounding requirement added in this phase.
"""

from __future__ import annotations

import pytest

from src.answer_models import AnswerValidationError
from src.lifecycle_business_agent import (
    SUBMIT_ANSWER_TOOL_NAME,
    LifecycleBusinessAgentError,
    run_lifecycle_business_qa,
)
from src.lifecycle_business_tools import LifecycleBusinessTools
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall

TOOLS = LifecycleBusinessTools(
    loan_portfolio={"total_outstanding_principal": 1234.0, "as_of_date": "2026-07-20"},
    campaign_funnel=[
        {"campaign_id": "CMP1", "channel": "EMAIL", "loans_funded": 5},
        {"campaign_id": "CMP2", "channel": "SOCIAL", "loans_funded": 2},
    ],
    underwriting_performance=[{"breakdown_type": "risk_segment", "breakdown_value": "HIGH", "approval_rate": 0.9}],
    underwriting_rejections={"LOW_CREDIT_SCORE": 3},
    payment_performance={"collection_rate": 0.95},
    delinquency_default=[{"breakdown_value": "ALL", "default_rate": 0.05}, {"breakdown_value": "HIGH", "default_rate": 0.1}],
    business_rules={},
    metrics_by_pipeline={},
)

KNOWN_METRICS = {"loans_funded", "channel", "sum_loans_funded"}


def _submission(**overrides) -> dict:
    base = {
        "answer_status": "ANSWERED",
        "question": "q",
        "answer_summary": "summary",
        "as_of_date": None,
        "cited_metrics": [],
        "caveats": [],
    }
    base.update(overrides)
    return base


def test_dispatches_a_new_bounded_query_tool_and_records_it_in_called_tool_calls():
    aggregate_call = ToolCall(
        id="1",
        name="aggregate_curated_data",
        arguments={"dataset": "campaign_funnel", "group_by": ["channel"], "metrics": [{"agg": "sum", "column": "loans_funded"}]},
    )
    submit_call = ToolCall(
        id="2",
        name=SUBMIT_ANSWER_TOOL_NAME,
        arguments=_submission(
            cited_metrics=[
                {
                    "metric_name": "sum_loans_funded",
                    "value": 5,
                    "source_reference": "aggregate_curated_data",
                    "row_identifier": {"channel": "EMAIL"},
                }
            ]
        ),
    )
    responses = [ModelResponse(tool_calls=[aggregate_call]), ModelResponse(tool_calls=[submit_call])]

    result = run_lifecycle_business_qa(
        "Total loans_funded by channel?", TOOLS, ScriptedDiagnosisModelClient(responses), known_metric_names=KNOWN_METRICS
    )

    assert result.answer.answer_status.value == "ANSWERED"
    assert result.called_tool_calls == [
        {"name": "aggregate_curated_data", "arguments": aggregate_call.arguments}
    ]
    assert result.answer.cited_metrics[0].value == 5


def test_multi_row_citation_without_row_identifier_is_rejected_end_to_end():
    aggregate_call = ToolCall(
        id="1",
        name="aggregate_curated_data",
        arguments={"dataset": "campaign_funnel", "group_by": ["channel"], "metrics": [{"agg": "sum", "column": "loans_funded"}]},
    )
    submit_call = ToolCall(
        id="2",
        name=SUBMIT_ANSWER_TOOL_NAME,
        arguments=_submission(
            cited_metrics=[
                {"metric_name": "sum_loans_funded", "value": 5, "source_reference": "aggregate_curated_data", "row_identifier": None}
            ]
        ),
    )
    responses = [ModelResponse(tool_calls=[aggregate_call]), ModelResponse(tool_calls=[submit_call])]

    # A grounding failure raises AnswerValidationError, not LifecycleBusinessAgentError --
    # run_lifecycle_business_qa doesn't wrap it (its caller, src/ask_lifecycle.py, catches
    # both together).
    with pytest.raises(AnswerValidationError):
        run_lifecycle_business_qa(
            "Total loans_funded by channel?", TOOLS, ScriptedDiagnosisModelClient(responses), known_metric_names=KNOWN_METRICS
        )


def test_join_curated_data_tool_dispatches_and_grounds_a_joined_result():
    join_call = ToolCall(
        id="1",
        name="join_curated_data",
        arguments={
            "left_dataset": "underwriting_performance",
            "right_dataset": "delinquency_default",
            "join_keys": ["breakdown_value"],
        },
    )
    submit_call = ToolCall(
        id="2",
        name=SUBMIT_ANSWER_TOOL_NAME,
        arguments=_submission(
            cited_metrics=[
                {
                    "metric_name": "approval_rate",
                    "value": 0.9,
                    "source_reference": "join_curated_data",
                    "row_identifier": {"breakdown_value": "HIGH"},
                }
            ]
        ),
    )
    responses = [ModelResponse(tool_calls=[join_call]), ModelResponse(tool_calls=[submit_call])]

    result = run_lifecycle_business_qa(
        "Compare approval and default rates for the HIGH segment.",
        TOOLS,
        ScriptedDiagnosisModelClient(responses),
        known_metric_names=KNOWN_METRICS | {"approval_rate", "breakdown_value"},
    )

    assert result.answer.answer_status.value == "ANSWERED"
    assert result.answer.cited_metrics[0].value == 0.9


def test_exhausting_max_turns_without_submit_answer_raises():
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_loan_portfolio_summary", arguments={})]) for _ in range(3)
    ]
    with pytest.raises(LifecycleBusinessAgentError):
        run_lifecycle_business_qa(
            "q", TOOLS, ScriptedDiagnosisModelClient(responses), known_metric_names=KNOWN_METRICS, max_turns=3
        )
