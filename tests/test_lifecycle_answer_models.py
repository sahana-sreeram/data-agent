"""Tests for src/lifecycle_answer_models.py's generalized multi-table grounding."""

from __future__ import annotations

import pytest

from src.lifecycle_answer_models import AnswerValidationError, parse_lifecycle_business_answer

KNOWN_METRICS = {"loan_count", "loans_funded", "campaign_id", "as_of_date"}

# A single-dict tool result (like get_loan_portfolio_summary).
SINGLE_DICT_RESULTS = {"get_loan_portfolio_summary": [{"loan_count": 162, "as_of_date": "2026-07-20"}]}

# A multi-row tool result (like get_campaign_funnel).
MULTI_ROW_RESULTS = {
    "get_campaign_funnel": [
        {"rows": [{"campaign_id": "CMP0001", "loans_funded": 3}, {"campaign_id": "CMP0008", "loans_funded": 23}]}
    ]
}


def _submission(metric_name, value, source_reference, status="ANSWERED"):
    return {
        "answer_status": status,
        "question": "q",
        "answer_summary": "summary",
        "as_of_date": None,
        "cited_metrics": [{"metric_name": metric_name, "value": value, "source_reference": source_reference}],
        "caveats": [] if status == "ANSWERED" else ["reason"],
    }


def test_grounds_successfully_against_a_flat_dict_result():
    result = parse_lifecycle_business_answer(
        _submission("loan_count", 162, "get_loan_portfolio_summary"),
        called_tool_names={"get_loan_portfolio_summary"},
        known_metric_names=KNOWN_METRICS,
        tool_results_by_name=SINGLE_DICT_RESULTS,
    )
    assert result.cited_metrics[0].value == 162


def test_grounds_successfully_against_a_row_in_a_multi_row_result():
    result = parse_lifecycle_business_answer(
        _submission("loans_funded", 23, "get_campaign_funnel"),
        called_tool_names={"get_campaign_funnel"},
        known_metric_names=KNOWN_METRICS,
        tool_results_by_name=MULTI_ROW_RESULTS,
    )
    assert result.cited_metrics[0].value == 23


def test_rejects_a_value_not_present_in_any_row():
    with pytest.raises(AnswerValidationError, match="was not found in any result"):
        parse_lifecycle_business_answer(
            _submission("loans_funded", 999, "get_campaign_funnel"),
            called_tool_names={"get_campaign_funnel"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=MULTI_ROW_RESULTS,
        )


def test_rejects_a_value_that_exists_but_for_a_different_metric_name():
    # 3 IS a real value in the fixture (CMP0001's loans_funded), but citing it under
    # "campaign_id" (a different field) must still fail -- the search is per-field, not
    # "does this value appear anywhere in the row."
    with pytest.raises(AnswerValidationError):
        parse_lifecycle_business_answer(
            _submission("campaign_id", 3, "get_campaign_funnel"),
            called_tool_names={"get_campaign_funnel"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=MULTI_ROW_RESULTS,
        )


def test_rejects_source_reference_for_a_tool_not_actually_called():
    with pytest.raises(AnswerValidationError, match="does not match a tool actually called"):
        parse_lifecycle_business_answer(
            _submission("loans_funded", 23, "get_campaign_funnel"),
            called_tool_names=set(),  # nothing called
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=MULTI_ROW_RESULTS,
        )


def test_rejects_unknown_metric_name():
    with pytest.raises(AnswerValidationError, match="unknown metric name"):
        parse_lifecycle_business_answer(
            _submission("totally_made_up_field", 1, "get_campaign_funnel"),
            called_tool_names={"get_campaign_funnel"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=MULTI_ROW_RESULTS,
        )


def test_answered_status_requires_at_least_one_cited_metric():
    submission = _submission("loan_count", 162, "get_loan_portfolio_summary")
    submission["cited_metrics"] = []
    with pytest.raises(AnswerValidationError, match="requires at least one cited metric"):
        parse_lifecycle_business_answer(
            submission,
            called_tool_names={"get_loan_portfolio_summary"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=SINGLE_DICT_RESULTS,
        )


def test_unreliable_data_status_requires_a_caveat():
    submission = _submission("loan_count", 162, "get_loan_portfolio_summary", status="UNRELIABLE_DATA")
    submission["caveats"] = []
    with pytest.raises(AnswerValidationError, match="requires at least one caveat"):
        parse_lifecycle_business_answer(
            submission,
            called_tool_names={"get_loan_portfolio_summary"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=SINGLE_DICT_RESULTS,
        )
