"""Tests for the business Q&A agent's structured, grounded answer output.

Covers: happy-path parsing, every grounding rule (metric must be known,
source_reference must match an actually-called tool, value must exactly
match the trusted portfolio_summary), status/caveat consistency rules, and
the deterministic UNRELIABLE_DATA constructor that needs no model call.
"""

from __future__ import annotations

import pytest

from src.answer_models import (
    AnswerStatus,
    AnswerValidationError,
    build_unreliable_data_answer,
    business_answer_to_dict,
    parse_business_answer,
)

PORTFOLIO_SUMMARY = {"total_outstanding_balance": 997522.36, "loan_count": 10}
KNOWN_METRICS = {"total_outstanding_balance", "loan_count"}
CALLED_TOOLS = {"get_portfolio_summary"}


def _valid_raw(**overrides) -> dict:
    raw = {
        "answer_status": "ANSWERED",
        "question": "What is the total outstanding loan balance?",
        "answer_summary": "The total outstanding loan balance is 997522.36.",
        "as_of_date": "2026-07-20",
        "cited_metrics": [
            {"metric_name": "total_outstanding_balance", "value": 997522.36, "source_reference": "get_portfolio_summary"}
        ],
        "caveats": [],
    }
    raw.update(overrides)
    return raw


def _parse(raw: dict):
    return parse_business_answer(
        raw, called_tool_names=CALLED_TOOLS, known_metric_names=KNOWN_METRICS, portfolio_summary=PORTFOLIO_SUMMARY
    )


def test_valid_answer_parses():
    answer = _parse(_valid_raw())
    assert answer.answer_status == AnswerStatus.ANSWERED
    assert answer.cited_metrics[0].metric_name == "total_outstanding_balance"
    assert answer.cited_metrics[0].value == 997522.36


def test_source_reference_functions_prefix_is_normalized():
    raw = _valid_raw()
    raw["cited_metrics"][0]["source_reference"] = "functions.get_portfolio_summary"
    answer = _parse(raw)
    assert answer.cited_metrics[0].source_reference == "get_portfolio_summary"


def test_missing_required_key_is_rejected():
    raw = _valid_raw()
    del raw["answer_summary"]
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_invalid_answer_status_is_rejected():
    raw = _valid_raw(answer_status="MAYBE")
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_unknown_cited_metric_name_is_rejected():
    raw = _valid_raw()
    raw["cited_metrics"][0]["metric_name"] = "made_up_metric"
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_source_reference_not_actually_called_is_rejected():
    raw = _valid_raw()
    raw["cited_metrics"][0]["source_reference"] = "get_business_rules"
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_value_that_does_not_match_trusted_data_is_rejected():
    raw = _valid_raw()
    raw["cited_metrics"][0]["value"] = 1000000.00
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_rounded_value_is_rejected_even_though_close():
    raw = _valid_raw()
    raw["cited_metrics"][0]["value"] = 997522.4
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_answered_with_no_cited_metrics_is_rejected():
    raw = _valid_raw(cited_metrics=[])
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_unreliable_data_with_no_caveats_is_rejected():
    raw = _valid_raw(answer_status="UNRELIABLE_DATA", cited_metrics=[], caveats=[])
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_insufficient_data_with_caveats_and_no_cited_metrics_is_valid():
    raw = _valid_raw(answer_status="INSUFFICIENT_DATA", cited_metrics=[], caveats=["no metric answers this question"])
    answer = _parse(raw)
    assert answer.answer_status == AnswerStatus.INSUFFICIENT_DATA


def test_caveats_must_be_a_list_of_strings():
    raw = _valid_raw(caveats=[123])
    with pytest.raises(AnswerValidationError):
        _parse(raw)


def test_build_unreliable_data_answer_needs_no_model_call():
    answer = build_unreliable_data_answer("What is the balance?", "validation failed and repair was blocked")
    assert answer.answer_status == AnswerStatus.UNRELIABLE_DATA
    assert answer.cited_metrics == []
    assert answer.caveats == ["validation failed and repair was blocked"]


def test_business_answer_to_dict_serializes_enum_to_value():
    answer = _parse(_valid_raw())
    d = business_answer_to_dict(answer)
    assert d["answer_status"] == "ANSWERED"
    assert isinstance(d["cited_metrics"][0], dict)
