"""Tests for the generic lifecycle diagnostic tools: facts only, validated arguments, no
filesystem/write access. Uses hand-built fixtures (no S3 needed) for the core class -- the
general-purpose dataset tools themselves are covered by tests/test_dataset_registry_tools.py;
this file covers the lifecycle-specific tools, the wiring that plugs the shared module in,
and (against real S3) build_diagnostic_tools_for_pipeline for every registered pipeline.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl_spark_loan_portfolio import compute_loan_portfolio
from src.lifecycle_diagnostic_tools import (
    ALLOWLISTED_TOOL_NAMES,
    LifecycleDiagnosticTools,
    ToolError,
    build_diagnostic_tools_for_pipeline,
    dispatch_tool,
)
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY

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
BUSINESS_RULES = {"successful_payment_statuses": ["PAID"], "interest_accrual": {"accrues_on_statuses": ["ACTIVE"]}}
METRICS = {
    "metrics": {
        "loan_count": {"formula": "count(loans)", "source_tables": ["lifecycle.loans"], "business_rule_dependencies": []},
        "total_outstanding_principal": {
            "formula": "sum(...) where business_rules.successful_payment_statuses",
            "source_tables": ["lifecycle.loans", "lifecycle.payment_events"],
            "business_rule_dependencies": ["successful_payment_statuses"],
        },
        "total_accrued_interest": {
            "formula": "sum(...) where business_rules.interest_accrual.accrues_on_statuses",
            "source_tables": ["lifecycle.loans"],
            "business_rule_dependencies": ["interest_accrual.accrues_on_statuses"],
        },
    }
}
LINEAGE = {
    "datasets": {
        "curated.loan_portfolio": {
            "path": "s3://x/curated/loan_portfolio.parquet",
            "produced_by": "src/etl_spark_loan_portfolio.py",
            "depends_on": ["lifecycle.loans", "lifecycle.payment_events"],
        }
    }
}


@pytest.fixture()
def tools():
    return LifecycleDiagnosticTools(
        raw_tables={"loans": LOANS, "payment_events": PAYMENT_EVENTS},
        validation_results=VALIDATION_RESULTS,
        business_rules=BUSINESS_RULES,
        metrics=METRICS,
        etl_source_file="src/etl_spark_loan_portfolio.py",
        etl_functions={"compute_loan_portfolio": compute_loan_portfolio},
        lineage=LINEAGE,
        lineage_key="curated.loan_portfolio",
    )


def test_list_datasets_exposes_every_raw_table(tools):
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


def test_get_relevant_etl_source_returns_every_functions_source(tools):
    result = tools.get_relevant_etl_source()
    assert result["file"] == "src/etl_spark_loan_portfolio.py"
    assert set(result["functions"]) == {"compute_loan_portfolio"}
    assert "def compute_loan_portfolio" in result["functions"]["compute_loan_portfolio"]


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


# --- build_diagnostic_tools_for_pipeline, against real S3 data, for every pipeline --------


@pytest.fixture
def real_curated_data_present(s3_storage):
    if not s3_storage.exists("curated/pipeline_run.json"):
        pytest.skip("curated lifecycle data not present in this environment")


@pytest.mark.parametrize("pipeline_name", list(PIPELINE_REGISTRY))
def test_build_diagnostic_tools_for_pipeline_loads_the_right_raw_tables_and_functions(
    pipeline_name, s3_storage, real_curated_data_present
):
    spec = PIPELINE_REGISTRY[pipeline_name]
    tools = build_diagnostic_tools_for_pipeline(pipeline_name, s3_storage, VALIDATION_RESULTS, BUSINESS_RULES)

    assert set(tools.raw_tables) == set(spec.raw_tables)
    for df in tools.raw_tables.values():
        assert isinstance(df, pd.DataFrame)

    source = tools.get_relevant_etl_source()
    assert source["file"] == spec.etl_source_file
    assert set(source["functions"]) == set(spec.etl_function_names)
    for function_name, function_source in source["functions"].items():
        assert f"def {function_name}" in function_source


# --- New directed-investigation tools ------------------------------------------------------


def test_get_pipeline_business_rules_is_an_alias_for_get_business_rules(tools):
    assert tools.get_pipeline_business_rules() == tools.get_business_rules() == BUSINESS_RULES


def test_get_metric_lineage_returns_the_pipelines_lineage_entry(tools):
    result = tools.get_metric_lineage("loan_count")
    assert result["metric_name"] == "loan_count"
    assert result["lineage"]["path"] == "s3://x/curated/loan_portfolio.parquet"


def test_get_metric_lineage_rejects_unknown_metric(tools):
    with pytest.raises(ToolError):
        tools.get_metric_lineage("not_a_real_metric")


def test_get_metric_lineage_rejects_missing_lineage_entry():
    broken_tools = LifecycleDiagnosticTools(
        raw_tables={"loans": LOANS}, validation_results=VALIDATION_RESULTS, business_rules=BUSINESS_RULES,
        metrics=METRICS, etl_source_file="src/etl_spark_loan_portfolio.py",
        etl_functions={"compute_loan_portfolio": compute_loan_portfolio},
        lineage={"datasets": {}}, lineage_key="curated.loan_portfolio",
    )
    with pytest.raises(ToolError):
        broken_tools.get_metric_lineage("loan_count")


def test_get_failed_metric_context_bundles_definition_lineage_and_matching_checks(tools):
    result = tools.get_failed_metric_context("loan_count")
    assert result["definition"]["formula"] == "count(loans)"
    assert result["lineage"]["path"] == "s3://x/curated/loan_portfolio.parquet"
    assert result["failed_checks_mentioning_this_metric"] == ["loan_count_reconciliation"]


def test_get_failed_metric_context_empty_when_no_check_mentions_it(tools):
    result = tools.get_failed_metric_context("total_outstanding_principal")
    assert result["failed_checks_mentioning_this_metric"] == []


def _dummy_fn_without_business_rules_lookup():
    """A stand-in ETL function whose source never reads business_rules at all."""
    return 1 + 1


def test_compare_metric_definition_to_etl_flags_a_missing_business_rule_lookup():
    broken_tools = LifecycleDiagnosticTools(
        raw_tables={}, validation_results=VALIDATION_RESULTS, business_rules=BUSINESS_RULES,
        metrics=METRICS, etl_source_file="src/etl_spark_loan_portfolio.py",
        etl_functions={"compute_loan_portfolio": _dummy_fn_without_business_rules_lookup},
        lineage=LINEAGE, lineage_key="curated.loan_portfolio",
    )
    result = broken_tools.compare_metric_definition_to_etl("total_outstanding_principal")
    assert result["business_rule_dependencies"] == ["successful_payment_statuses"]
    assert result["dependency_present_in_source"] == {"successful_payment_statuses": False}
    assert result["mismatch"] is True


def test_compare_metric_definition_to_etl_passes_when_the_real_lookup_is_present(tools):
    result = tools.compare_metric_definition_to_etl("total_outstanding_principal")
    assert result["dependency_present_in_source"] == {"successful_payment_statuses": True}
    assert result["mismatch"] is False


def test_compare_metric_definition_to_etl_handles_nested_dot_path(tools):
    result = tools.compare_metric_definition_to_etl("total_accrued_interest")
    assert result["business_rule_dependencies"] == ["interest_accrual.accrues_on_statuses"]
    assert result["dependency_present_in_source"] == {"interest_accrual.accrues_on_statuses": True}
    assert result["mismatch"] is False


def test_compare_metric_definition_to_etl_no_dependencies_means_no_mismatch(tools):
    result = tools.compare_metric_definition_to_etl("loan_count")
    assert result["business_rule_dependencies"] == []
    assert result["mismatch"] is False


def test_trace_failed_check_to_code_exact_for_a_per_metric_reconciliation_check(tools):
    result = tools.trace_failed_check_to_code("loan_count_reconciliation")
    assert result["check"]["id"] == "loan_count_reconciliation"
    assert result["candidate_metrics"] == ["loan_count"]
    assert result["candidate_precision"] == "exact"
    assert result["file"] == "src/etl_spark_loan_portfolio.py"


def test_trace_failed_check_to_code_coarse_for_an_aggregate_check():
    aggregate_validation_results = {
        "overall_status": "FAIL",
        "checks": [{"id": "loan_portfolio_breakdown_rows_match", "status": "FAIL", "details": "mismatched: ['ALL']"}],
    }
    aggregate_tools = LifecycleDiagnosticTools(
        raw_tables={}, validation_results=aggregate_validation_results, business_rules=BUSINESS_RULES,
        metrics=METRICS, etl_source_file="src/etl_spark_loan_portfolio.py",
        etl_functions={"compute_loan_portfolio": compute_loan_portfolio}, lineage=LINEAGE,
        lineage_key="curated.loan_portfolio",
    )
    result = aggregate_tools.trace_failed_check_to_code("loan_portfolio_breakdown_rows_match")
    assert set(result["candidate_metrics"]) == set(METRICS["metrics"])
    assert "coarse" in result["candidate_precision"]


def test_trace_failed_check_to_code_rejects_unknown_check_id(tools):
    with pytest.raises(ToolError):
        tools.trace_failed_check_to_code("not_a_real_check")


# --- Regression guard: the real business_rule_dependencies audit must stay accurate --------


@pytest.mark.parametrize("pipeline_name", list(PIPELINE_REGISTRY))
def test_declared_business_rule_dependencies_are_actually_present_in_real_etl_source(
    pipeline_name, s3_storage, real_curated_data_present
):
    """Every metric's declared business_rule_dependencies must be found in the REAL, current,
    correct ETL source -- proving the audit behind compare_metric_definition_to_etl can't
    silently drift stale as the codebase evolves without a test catching it."""
    spec = PIPELINE_REGISTRY[pipeline_name]
    business_rules = s3_storage.read_json("context/business_rules.json")
    tools = build_diagnostic_tools_for_pipeline(pipeline_name, s3_storage, VALIDATION_RESULTS, business_rules)

    for metric_name in spec_metrics(s3_storage, spec):
        result = tools.compare_metric_definition_to_etl(metric_name)
        assert result["mismatch"] is False, f"{pipeline_name}.{metric_name}: {result}"


def spec_metrics(storage, spec) -> list:
    return list(storage.read_json(spec.metrics_key).get("metrics", {}))
