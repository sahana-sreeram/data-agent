"""Tests for the loan_portfolio lifecycle diagnostic tools: facts only, validated
arguments, no filesystem/write access. Uses hand-built fixtures (no S3 needed) --
the general-purpose dataset tools themselves are covered by
tests/test_dataset_registry_tools.py; this file covers the lifecycle-specific tools plus
the wiring that plugs the shared module in.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.lifecycle_diagnostic_tools import ALLOWLISTED_TOOL_NAMES, LifecycleDiagnosticTools, ToolError, dispatch_tool

LOANS = pd.DataFrame(
    [
        {"loan_id": "L1", "principal_amount": 1000.0, "loan_status": "ACTIVE"},
        {"loan_id": "L2", "principal_amount": 2000.0, "loan_status": "CLOSED"},
    ]
)
PAYMENT_EVENTS = pd.DataFrame(
    [
        {"loan_id": "L1", "payment_status": "MISSED", "amount": 0.0},
        {"loan_id": "L2", "payment_status": "PAID", "amount": 2000.0},
    ]
)
VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [
        {"id": "loan_count_reconciliation", "status": "FAIL", "expected": 2, "actual": 1},
        {"id": "avg_interest_rate_reconciliation", "status": "PASS"},
    ],
}
BUSINESS_RULES = {"successful_payment_statuses": ["PAID"]}
METRICS = {
    "metrics": {
        "loan_count": {"formula": "count(loans)", "source_tables": ["lifecycle.loans"]},
    }
}


@pytest.fixture()
def tools():
    return LifecycleDiagnosticTools(
        loans=LOANS,
        payment_events=PAYMENT_EVENTS,
        validation_results=VALIDATION_RESULTS,
        business_rules=BUSINESS_RULES,
        metrics=METRICS,
    )


def test_list_datasets_exposes_loans_and_payment_events(tools):
    assert tools.list_datasets() == {"datasets": ["loans", "payment_events"]}


def test_get_dataset_schema_delegates_to_shared_module(tools):
    result = tools.get_dataset_schema("loans")
    assert result["row_count"] == 2


def test_compare_dataset_keys_delegates_to_shared_module(tools):
    result = tools.compare_dataset_keys("loans", "payment_events", ["loan_id"])
    assert result["matching_key_count"] == 2


def test_get_validation_results_returns_exact_data(tools):
    assert tools.get_validation_results() == VALIDATION_RESULTS


def test_get_failed_checks_returns_only_failures(tools):
    result = tools.get_failed_checks()
    assert len(result["failed_checks"]) == 1
    assert result["failed_checks"][0]["id"] == "loan_count_reconciliation"


def test_get_business_rules_returns_exact_data(tools):
    assert tools.get_business_rules() == BUSINESS_RULES


def test_get_metric_definition_returns_correct_entry(tools):
    result = tools.get_metric_definition("loan_count")
    assert result["loan_count"]["formula"] == "count(loans)"


def test_get_metric_definition_rejects_unknown_metric(tools):
    with pytest.raises(ToolError):
        tools.get_metric_definition("not_a_real_metric")


def test_get_relevant_etl_source_returns_real_function_source(tools):
    result = tools.get_relevant_etl_source()
    assert result["file"] == "src/etl_spark_loan_portfolio.py"
    assert result["function"] == "compute_loan_portfolio"
    assert "def compute_loan_portfolio" in result["source"]


def test_dispatch_tool_returns_error_dict_instead_of_raising(tools):
    result = dispatch_tool(tools, "get_metric_definition", {"metric_name": "nope"})
    assert "error" in result


def test_dispatch_tool_rejects_unknown_tool_name(tools):
    result = dispatch_tool(tools, "delete_everything", {})
    assert "error" in result


def test_allowlisted_tool_names_includes_generic_and_scenario_tools(tools):
    assert "list_datasets" in ALLOWLISTED_TOOL_NAMES
    assert "get_relevant_etl_source" in ALLOWLISTED_TOOL_NAMES
    assert "aggregate_dataset" in ALLOWLISTED_TOOL_NAMES
