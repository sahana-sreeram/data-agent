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


def _submission(metric_name, value, source_reference, status="ANSWERED", row_identifier=None):
    return {
        "answer_status": status,
        "question": "q",
        "answer_summary": "summary",
        "as_of_date": None,
        "cited_metrics": [
            {"metric_name": metric_name, "value": value, "source_reference": source_reference, "row_identifier": row_identifier}
        ],
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
        _submission("loans_funded", 23, "get_campaign_funnel", row_identifier={"campaign_id": "CMP0008"}),
        called_tool_names={"get_campaign_funnel"},
        known_metric_names=KNOWN_METRICS,
        tool_results_by_name=MULTI_ROW_RESULTS,
    )
    assert result.cited_metrics[0].value == 23


def test_rejects_a_multi_row_citation_with_no_row_identifier_even_when_the_value_is_unique():
    # 23 is unique to CMP0008 in the fixture, but omitting row_identifier must still fail --
    # the model must always be explicit about which row it means, not rely on the value
    # happening to be unique. This is the exact gap this phase closes.
    with pytest.raises(AnswerValidationError, match="was not found"):
        parse_lifecycle_business_answer(
            _submission("loans_funded", 23, "get_campaign_funnel"),
            called_tool_names={"get_campaign_funnel"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=MULTI_ROW_RESULTS,
        )


def test_rejects_a_row_identifier_that_does_not_match_any_row():
    with pytest.raises(AnswerValidationError, match="was not found"):
        parse_lifecycle_business_answer(
            _submission("loans_funded", 23, "get_campaign_funnel", row_identifier={"campaign_id": "CMP9999"}),
            called_tool_names={"get_campaign_funnel"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=MULTI_ROW_RESULTS,
        )


def test_rejects_a_row_identifier_that_matches_the_wrong_row():
    # loans_funded=3 is real, but only for CMP0001 -- citing it against CMP0008 must fail.
    with pytest.raises(AnswerValidationError, match="was not found"):
        parse_lifecycle_business_answer(
            _submission("loans_funded", 3, "get_campaign_funnel", row_identifier={"campaign_id": "CMP0008"}),
            called_tool_names={"get_campaign_funnel"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=MULTI_ROW_RESULTS,
        )


def test_row_identifier_is_optional_for_a_single_row_result():
    single_row_results = {"get_delinquency_default": [{"rows": [{"breakdown_value": "ALL", "loan_count": 5}]}]}
    result = parse_lifecycle_business_answer(
        _submission("loan_count", 5, "get_delinquency_default"),
        called_tool_names={"get_delinquency_default"},
        known_metric_names=KNOWN_METRICS | {"loan_count"},
        tool_results_by_name=single_row_results,
    )
    assert result.cited_metrics[0].value == 5


def test_grounds_a_dynamically_named_aggregate_field_not_in_known_metric_names():
    # aggregate_curated_data's field names (e.g. "sum_loans_funded") are computed from the
    # model's own group_by/metrics choice at call time and can never be pre-registered in
    # known_metric_names -- but a real value from a real result is just as strongly grounded.
    aggregate_results = {
        "aggregate_curated_data": [{"groups": [{"channel": "EMAIL", "sum_loans_funded": 42}, {"channel": "SOCIAL", "sum_loans_funded": 7}]}]
    }
    result = parse_lifecycle_business_answer(
        _submission("sum_loans_funded", 42, "aggregate_curated_data", row_identifier={"channel": "EMAIL"}),
        called_tool_names={"aggregate_curated_data"},
        known_metric_names=KNOWN_METRICS,  # deliberately does NOT contain "sum_loans_funded"
        tool_results_by_name=aggregate_results,
    )
    assert result.cited_metrics[0].value == 42


def test_still_rejects_a_truly_fabricated_field_name_never_returned_by_any_tool():
    aggregate_results = {"aggregate_curated_data": [{"groups": [{"channel": "EMAIL", "sum_loans_funded": 42}]}]}
    with pytest.raises(AnswerValidationError, match="unknown metric name"):
        parse_lifecycle_business_answer(
            _submission("totally_made_up_field", 1, "aggregate_curated_data", row_identifier={"channel": "EMAIL"}),
            called_tool_names={"aggregate_curated_data"},
            known_metric_names=KNOWN_METRICS,
            tool_results_by_name=aggregate_results,
        )


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


def test_accepts_source_reference_with_a_trailing_call_style_suffix():
    # Confirmed live: a model sometimes cites a bounded query tool as "tool_name(dataset)"
    # instead of the bare tool name -- normalized the same way "functions." prefixes are.
    answer = parse_lifecycle_business_answer(
        _submission("loans_funded", 23, "get_campaign_funnel(campaign_funnel)", row_identifier={"campaign_id": "CMP0008"}),
        called_tool_names={"get_campaign_funnel"},
        known_metric_names=KNOWN_METRICS,
        tool_results_by_name=MULTI_ROW_RESULTS,
    )
    assert answer.cited_metrics[0].source_reference == "get_campaign_funnel"


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
