"""Tests for src/lifecycle_business_tools.py against the REAL curated S3 data.
Skips cleanly if unreachable, matching the established pattern.
"""

from __future__ import annotations

import json

import pytest

from src.lifecycle_business_tools import LifecycleBusinessTools, ToolError, dispatch_tool

METRICS_PIPELINES = ["loan_portfolio", "campaign_funnel", "underwriting_performance", "payment_performance", "delinquency_default"]


@pytest.fixture(scope="module")
def tools(s3_storage):
    if not s3_storage.exists("curated/loan_portfolio.parquet"):
        pytest.skip("curated lifecycle data not present in this environment")

    loan_portfolio = s3_storage.read_parquet("curated/loan_portfolio.parquet").iloc[0].to_dict()
    campaign_funnel = s3_storage.read_parquet("curated/campaign_funnel.parquet").to_dict(orient="records")
    underwriting_performance = s3_storage.read_parquet("curated/underwriting_performance.parquet").to_dict(orient="records")
    rejections_df = s3_storage.read_parquet("curated/underwriting_performance_rejections.parquet")
    underwriting_rejections = dict(zip(rejections_df["rejection_reason"], rejections_df["count"].astype(int)))
    payment_performance = s3_storage.read_parquet("curated/payment_performance.parquet").iloc[0].to_dict()
    delinquency_default = s3_storage.read_parquet("curated/delinquency_default.parquet").to_dict(orient="records")
    business_rules = s3_storage.read_json("context/business_rules.json")
    metrics_by_pipeline = {p: s3_storage.read_json(f"context/metrics/{p}.json") for p in METRICS_PIPELINES}

    return LifecycleBusinessTools(
        loan_portfolio=loan_portfolio,
        campaign_funnel=campaign_funnel,
        underwriting_performance=underwriting_performance,
        underwriting_rejections=underwriting_rejections,
        payment_performance=payment_performance,
        delinquency_default=delinquency_default,
        business_rules=business_rules,
        metrics_by_pipeline=metrics_by_pipeline,
    )


def _assert_no_nan_in_json(result) -> None:
    serialized = json.dumps(result)
    assert "NaN" not in serialized, f"tool result contains a raw NaN token: {serialized[:200]}"


def test_get_loan_portfolio_summary_returns_flat_dict(tools):
    result = tools.get_loan_portfolio_summary()
    assert isinstance(result, dict)
    assert "loan_count" in result
    _assert_no_nan_in_json(result)


def test_get_campaign_funnel_includes_organic_row_with_no_nan(tools):
    result = tools.get_campaign_funnel()
    assert "rows" in result
    organic_rows = [r for r in result["rows"] if r["campaign_id"] is None]
    assert len(organic_rows) == 1
    _assert_no_nan_in_json(result)


def test_get_underwriting_performance_has_both_breakdown_types(tools):
    result = tools.get_underwriting_performance()
    breakdown_types = {r["breakdown_type"] for r in result["rows"]}
    assert breakdown_types == {"risk_segment", "model_version"}
    _assert_no_nan_in_json(result)


def test_get_underwriting_rejection_distribution_is_flat_dict(tools):
    result = tools.get_underwriting_rejection_distribution()
    assert isinstance(result, dict)
    assert all(isinstance(v, int) for v in result.values())
    _assert_no_nan_in_json(result)


def test_get_payment_performance_summary_returns_flat_dict(tools):
    result = tools.get_payment_performance_summary()
    assert "collection_rate" in result
    _assert_no_nan_in_json(result)


def test_get_delinquency_default_has_overall_and_segment_rows(tools):
    result = tools.get_delinquency_default()
    values = {r["breakdown_value"] for r in result["rows"]}
    assert "ALL" in values
    assert {"LOW", "MEDIUM", "HIGH"} <= values
    _assert_no_nan_in_json(result)


def test_get_metric_definition_known_and_unknown(tools):
    known = tools.get_metric_definition(pipeline="loan_portfolio", metric_name="total_outstanding_principal")
    assert "total_outstanding_principal" in known

    with pytest.raises(ToolError):
        tools.get_metric_definition(pipeline="loan_portfolio", metric_name="does_not_exist")

    with pytest.raises(ToolError):
        tools.get_metric_definition(pipeline="not_a_real_pipeline", metric_name="x")


def test_get_business_rules_returns_full_dict(tools):
    result = tools.get_business_rules()
    assert "prepayment_threshold_days" in result


def test_dispatch_tool_catches_tool_error_without_raising(tools):
    result = dispatch_tool(tools, "get_metric_definition", {"pipeline": "loan_portfolio", "metric_name": "nope"})
    assert "error" in result


def test_dispatch_tool_rejects_unknown_tool_name(tools):
    result = dispatch_tool(tools, "delete_everything", {})
    assert "error" in result
