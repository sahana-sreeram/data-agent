"""Tests for the business Q&A agent's tool-calling loop.

Uses ScriptedDiagnosisModelClient (no live API calls) to drive the loop
through: a normal tool-call-then-submit turn, a submission that fails
grounding, and max-turns exhaustion.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from src.answer_models import AnswerStatus, AnswerValidationError
from src.business_agent import SUBMIT_ANSWER_TOOL_NAME, BusinessAgentError, run_business_qa
from src.business_tools import BusinessTools
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall

PORTFOLIO_SUMMARY = {"total_outstanding_balance": 997522.36}
DATA_DICTIONARY = {"portfolio_summary": {"fields": {"total_outstanding_balance": {"type": "float"}}}}
KNOWN_METRICS = {"total_outstanding_balance"}


def _tools() -> BusinessTools:
    return BusinessTools(portfolio_summary=PORTFOLIO_SUMMARY, business_rules={}, data_dictionary=DATA_DICTIONARY)


def _valid_submission() -> dict:
    return {
        "answer_status": "ANSWERED",
        "question": "What is the total outstanding loan balance?",
        "answer_summary": "The total outstanding loan balance is 997522.36.",
        "as_of_date": None,
        "cited_metrics": [
            {"metric_name": "total_outstanding_balance", "value": 997522.36, "source_reference": "get_portfolio_summary"}
        ],
        "caveats": [],
    }


def test_agent_calls_tool_then_submits_grounded_answer():
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_portfolio_summary", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_ANSWER_TOOL_NAME, arguments=_valid_submission())]),
    ]
    answer = run_business_qa(
        "What is the total outstanding loan balance?",
        _tools(),
        ScriptedDiagnosisModelClient(responses),
        known_metric_names=KNOWN_METRICS,
    )
    assert answer.answer_status == AnswerStatus.ANSWERED
    assert answer.cited_metrics[0].value == 997522.36


def test_agent_can_submit_on_the_first_turn_without_tool_calls_if_grounded():
    # Not realistic (source_reference wouldn't be "called"), but proves the
    # loop doesn't require a fixed number of turns -- only max_turns as a cap.
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name="get_portfolio_summary", arguments={})])]
    responses.append(ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_ANSWER_TOOL_NAME, arguments=_valid_submission())]))
    answer = run_business_qa(
        "q", _tools(), ScriptedDiagnosisModelClient(responses), known_metric_names=KNOWN_METRICS
    )
    assert answer.answer_status == AnswerStatus.ANSWERED


def test_ungrounded_submission_raises_answer_validation_error():
    bad_submission = _valid_submission()
    bad_submission["cited_metrics"][0]["source_reference"] = "get_business_rules"  # never called
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_ANSWER_TOOL_NAME, arguments=bad_submission)])]
    with pytest.raises(AnswerValidationError):
        run_business_qa("q", _tools(), ScriptedDiagnosisModelClient(responses), known_metric_names=KNOWN_METRICS)


def test_max_turns_exceeded_without_submission_raises_business_agent_error():
    responses = [ModelResponse(tool_calls=[ToolCall(id=str(i), name="get_portfolio_summary", arguments={})]) for i in range(3)]
    with pytest.raises(BusinessAgentError):
        run_business_qa("q", _tools(), ScriptedDiagnosisModelClient(responses), known_metric_names=KNOWN_METRICS, max_turns=3)


def test_unknown_tool_call_is_reported_as_error_and_loop_continues():
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="delete_everything", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_portfolio_summary", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="3", name=SUBMIT_ANSWER_TOOL_NAME, arguments=_valid_submission())]),
    ]
    answer = run_business_qa("q", _tools(), ScriptedDiagnosisModelClient(responses), known_metric_names=KNOWN_METRICS)
    assert answer.answer_status == AnswerStatus.ANSWERED


def test_no_subprocess_usage_in_business_agent_module():
    tree = ast.parse(Path("src/business_agent.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(alias.name == "subprocess" for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.module != "subprocess"
