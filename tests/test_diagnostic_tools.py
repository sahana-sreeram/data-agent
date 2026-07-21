"""Tests for the read-only diagnostic tools: facts only, validated arguments, no filesystem/write/exec access."""

from __future__ import annotations

import inspect

import pandas as pd
import pytest

from src.diagnostic_tools import ALLOWLISTED_TOOL_NAMES, DiagnosticTools, ToolError, dispatch_tool

LOANS = [
    {"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"},
]
PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 500.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000001", "amount_paid": 200.0, "payment_status": "SETTLED"},
    {"payment_id": "P0000003", "loan_id": "L000001", "amount_paid": 0.0, "payment_status": "MISSED"},
]
SUMMARY = {
    "total_original_principal": 1000.0,
    "total_successful_payments": 500.0,
    "total_outstanding_balance": 500.0,
}
BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "valid_payment_statuses": ["PAID", "MISSED", "SCHEDULED", "LATE", "FAILED"],
}
VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [
        {"id": "payment_status_enum_valid", "status": "FAIL", "details": "unexpected values found: ['SETTLED']"},
        {"id": "loan_count_reconciliation", "status": "PASS"},
    ],
}
VALIDATION_RULES = {"tolerance": {"currency": 0.01, "count": 0}, "rules": []}
LINEAGE = {
    "datasets": {
        "processed.portfolio_summary": {
            "path": "data/processed/portfolio_summary.json",
            "depends_on": ["raw.loans", "raw.payments"],
        }
    }
}
DATA_DICTIONARY = {
    "portfolio_summary": {"fields": {"total_successful_payments": {"type": "float", "description": "d"}}},
    "payments": {"fields": {"payment_status": {"type": "enum", "description": "d"}}},
}


@pytest.fixture()
def tools():
    return DiagnosticTools(
        loans_df=pd.DataFrame(LOANS),
        payments_df=pd.DataFrame(PAYMENTS),
        portfolio_summary=SUMMARY,
        business_rules=BUSINESS_RULES,
        validation_results=VALIDATION_RESULTS,
        validation_rules=VALIDATION_RULES,
        lineage=LINEAGE,
        data_dictionary=DATA_DICTIONARY,
        pipeline_run=None,
    )


def test_get_validation_results_returns_exact_data(tools):
    assert tools.get_validation_results() == VALIDATION_RESULTS


def test_get_failed_checks_returns_only_failures(tools):
    result = tools.get_failed_checks()
    assert len(result["failed_checks"]) == 1
    assert result["failed_checks"][0]["id"] == "payment_status_enum_valid"


def test_get_portfolio_summary_returns_exact_data(tools):
    assert tools.get_portfolio_summary() == SUMMARY


def test_get_business_rules_returns_exact_data(tools):
    assert tools.get_business_rules() == BUSINESS_RULES


def test_get_payment_status_counts(tools):
    assert tools.get_payment_status_counts() == {"PAID": 1, "SETTLED": 1, "MISSED": 1}


def test_get_payment_amount_totals_by_status(tools):
    assert tools.get_payment_amount_totals_by_status() == {"PAID": 500.0, "SETTLED": 200.0, "MISSED": 0.0}


def test_get_payment_samples_by_status_returns_bounded_samples(tools):
    result = tools.get_payment_samples_by_status("SETTLED", limit=5)
    assert result["status"] == "SETTLED"
    assert len(result["samples"]) == 1
    assert result["samples"][0]["payment_id"] == "P0000002"


def test_get_payment_samples_by_status_rejects_unobserved_status(tools):
    with pytest.raises(ToolError):
        tools.get_payment_samples_by_status("NOT_A_REAL_STATUS")


def test_get_payment_samples_by_status_rejects_excessive_limit(tools):
    with pytest.raises(ToolError):
        tools.get_payment_samples_by_status("PAID", limit=1000)


def test_get_payment_samples_by_status_rejects_zero_or_negative_limit(tools):
    with pytest.raises(ToolError):
        tools.get_payment_samples_by_status("PAID", limit=0)
    with pytest.raises(ToolError):
        tools.get_payment_samples_by_status("PAID", limit=-1)


def test_get_metric_lineage_returns_correct_entry(tools):
    result = tools.get_metric_lineage("total_successful_payments")
    assert result["lineage"]["path"] == "data/processed/portfolio_summary.json"


def test_get_metric_lineage_rejects_unknown_metric(tools):
    with pytest.raises(ToolError):
        tools.get_metric_lineage("not_a_real_metric")


def test_get_metric_definition_returns_correct_entry(tools):
    result = tools.get_metric_definition("total_successful_payments")
    assert result["total_successful_payments"]["type"] == "float"


def test_get_metric_definition_rejects_unknown_metric(tools):
    with pytest.raises(ToolError):
        tools.get_metric_definition("not_a_real_metric")


def test_get_source_record_counts(tools):
    assert tools.get_source_record_counts() == {"loans": 1, "payments": 3}


def test_get_pipeline_run_metadata_when_absent(tools):
    assert tools.get_pipeline_run_metadata() == {"available": False}


def test_get_pipeline_run_metadata_when_present():
    tools = DiagnosticTools(
        loans_df=pd.DataFrame(LOANS),
        payments_df=pd.DataFrame(PAYMENTS),
        portfolio_summary=SUMMARY,
        business_rules=BUSINESS_RULES,
        validation_results=VALIDATION_RESULTS,
        validation_rules=VALIDATION_RULES,
        lineage=LINEAGE,
        data_dictionary=DATA_DICTIONARY,
        pipeline_run={"overall_status": "FAILURE"},
    )
    result = tools.get_pipeline_run_metadata()
    assert result["available"] is True
    assert result["overall_status"] == "FAILURE"


def test_get_relevant_etl_source_returns_real_function_source(tools):
    result = tools.get_relevant_etl_source("total_successful_payments")
    assert result["file"] == "src/transform.py"
    assert "def compute_portfolio_summary" in result["source"]


def test_get_relevant_etl_source_rejects_unknown_metric(tools):
    with pytest.raises(ToolError):
        tools.get_relevant_etl_source("not_a_real_metric")


def test_get_data_dictionary_entry(tools):
    result = tools.get_data_dictionary_entry("payments", "payment_status")
    assert result["field"] == "payment_status"


def test_get_data_dictionary_entry_rejects_unknown_dataset(tools):
    with pytest.raises(ToolError):
        tools.get_data_dictionary_entry("not_a_dataset", "x")


def test_get_data_dictionary_entry_rejects_unknown_field(tools):
    with pytest.raises(ToolError):
        tools.get_data_dictionary_entry("payments", "not_a_field")


def test_dispatch_tool_returns_error_dict_instead_of_raising(tools):
    result = dispatch_tool(tools, "get_payment_samples_by_status", {"status": "NOPE"})
    assert "error" in result


def test_dispatch_tool_returns_error_for_wrong_arguments(tools):
    result = dispatch_tool(tools, "get_metric_definition", {"wrong_param": "x"})
    assert "error" in result


def test_dispatch_tool_rejects_unknown_tool_name(tools):
    result = dispatch_tool(tools, "delete_everything", {})
    assert "error" in result


PAYMENT_EVENTS = [
    {"event_id": "E0000001", "payment_id": "P0000001", "loan_id": "L000001", "event_type": "INITIATED", "event_timestamp": "2026-01-01T09:00:00Z", "amount": 500.0},
    {"event_id": "E0000002", "payment_id": "P0000001", "loan_id": "L000001", "event_type": "SETTLED", "event_timestamp": "2026-01-05T15:00:00Z", "amount": 500.0},
    {"event_id": "E0000003", "payment_id": "P0000001", "loan_id": "L000001", "event_type": "SETTLED", "event_timestamp": "2026-01-06T09:00:00Z", "amount": 500.0},
    {"event_id": "E0000004", "payment_id": "P0000002", "loan_id": "L000001", "event_type": "INITIATED", "event_timestamp": "2026-01-01T09:00:00Z", "amount": 300.0},
    {"event_id": "E0000005", "payment_id": "P0000002", "loan_id": "L000001", "event_type": "SETTLED", "event_timestamp": "2026-01-05T15:00:00Z", "amount": 300.0},
    {"event_id": "E0000006", "payment_id": "P0000003", "loan_id": "L000001", "event_type": "FAILED", "event_timestamp": "2026-01-05T15:00:00Z", "amount": 100.0},
]


@pytest.fixture()
def event_tools():
    return DiagnosticTools(
        loans_df=pd.DataFrame(LOANS),
        payments_df=pd.DataFrame(),
        portfolio_summary=SUMMARY,
        business_rules=BUSINESS_RULES,
        validation_results=VALIDATION_RESULTS,
        validation_rules=VALIDATION_RULES,
        lineage=LINEAGE,
        data_dictionary=DATA_DICTIONARY,
        pipeline_run=None,
        payment_events_df=pd.DataFrame(PAYMENT_EVENTS),
        etl_function_name="compute_portfolio_summary_from_payment_events",
    )


def test_event_tools_raise_when_no_payment_events_available(tools):
    with pytest.raises(ToolError):
        tools.get_payment_event_type_counts()
    with pytest.raises(ToolError):
        tools.get_payment_amount_totals_by_event_type()
    with pytest.raises(ToolError):
        tools.get_payment_event_cardinality_summary()
    with pytest.raises(ToolError):
        tools.get_duplicate_payment_id_counts()
    with pytest.raises(ToolError):
        tools.get_payment_event_samples("P0000001")


def test_get_payment_event_type_counts(event_tools):
    assert event_tools.get_payment_event_type_counts() == {"INITIATED": 2, "SETTLED": 3, "FAILED": 1}


def test_get_payment_amount_totals_by_event_type(event_tools):
    totals = event_tools.get_payment_amount_totals_by_event_type()
    assert totals["SETTLED"] == 1300.0  # 500 + 500 (duplicate) + 300
    assert totals["FAILED"] == 100.0


def test_get_payment_event_cardinality_summary(event_tools):
    result = event_tools.get_payment_event_cardinality_summary()
    assert result["total_logical_payments"] == 3
    # P0000001 has 3 rows, P0000002 has 2, P0000003 has 1
    assert result["distribution"] == {"3+": 1, "2": 1, "1": 1}


def test_get_duplicate_payment_id_counts_default_settled(event_tools):
    result = event_tools.get_duplicate_payment_id_counts()
    assert result["event_type"] == "SETTLED"
    assert result["logical_payments_with_multiple_events"] == 1
    assert result["duplicate_event_rows"] == 1
    assert result["duplicate_amount_total"] == 500.0
    assert result["sample_payment_ids"] == ["P0000001"]


def test_get_duplicate_payment_id_counts_rejects_unobserved_event_type(event_tools):
    with pytest.raises(ToolError):
        event_tools.get_duplicate_payment_id_counts(event_type="NOT_REAL")


def test_get_payment_event_samples_returns_sorted_events(event_tools):
    result = event_tools.get_payment_event_samples("P0000001")
    assert result["payment_id"] == "P0000001"
    assert [e["event_id"] for e in result["events"]] == ["E0000001", "E0000002", "E0000003"]


def test_get_payment_event_samples_rejects_unobserved_payment_id(event_tools):
    with pytest.raises(ToolError):
        event_tools.get_payment_event_samples("NOT_A_REAL_PAYMENT_ID")


def test_get_relevant_etl_source_can_target_the_event_based_etl(event_tools):
    result = event_tools.get_relevant_etl_source("total_successful_payments")
    assert result["function"] == "compute_portfolio_summary_from_payment_events"
    assert "def compute_portfolio_summary_from_payment_events" in result["source"]


# --- General-purpose dataset tools (list_datasets, get_dataset_schema, profile_dataset,
# analyze_key_cardinality, compare_dataset_keys, aggregate_dataset, sample_dataset) --
# schema-generic, addressed by alias, no scenario-specific knowledge baked in.

JOIN_LOANS = [
    {"loan_id": "L1", "customer_id": "C1", "principal_amount": 1000.0, "loan_status": "ACTIVE"},
    {"loan_id": "L2", "customer_id": "C2", "principal_amount": 2000.0, "loan_status": "ACTIVE"},
    {"loan_id": "L3", "customer_id": "C3", "principal_amount": 3000.0, "loan_status": "CLOSED"},
]
JOIN_PAYMENTS = [
    {"payment_id": "P1", "loan_id": "L1", "amount_paid": 500.0, "payment_status": "PAID"},
    {"payment_id": "P2", "loan_id": "L3", "amount_paid": 3000.0, "payment_status": "PAID"},
    {"payment_id": "P3", "loan_id": "L3", "amount_paid": 3000.0, "payment_status": "PAID"},
]


@pytest.fixture()
def join_tools():
    return DiagnosticTools(
        loans_df=pd.DataFrame(JOIN_LOANS),
        payments_df=pd.DataFrame(JOIN_PAYMENTS),
        portfolio_summary=SUMMARY,
        business_rules=BUSINESS_RULES,
        validation_results=VALIDATION_RESULTS,
        validation_rules=VALIDATION_RULES,
        lineage=LINEAGE,
        data_dictionary=DATA_DICTIONARY,
        pipeline_run=None,
    )


def test_list_datasets_returns_loans_and_payments(join_tools):
    assert join_tools.list_datasets() == {"datasets": ["loans", "payments"]}


def test_list_datasets_returns_loans_and_payment_events_for_event_incidents(event_tools):
    assert event_tools.list_datasets() == {"datasets": ["loans", "payment_events"]}


def test_get_dataset_schema_returns_columns_and_row_count(join_tools):
    result = join_tools.get_dataset_schema("loans")
    assert result["row_count"] == 3
    assert set(result["columns"]) == {"loan_id", "customer_id", "principal_amount", "loan_status"}


def test_get_dataset_schema_unknown_dataset_raises(join_tools):
    with pytest.raises(ToolError):
        join_tools.get_dataset_schema("not_a_real_dataset")


def test_profile_dataset_reports_null_and_distinct_counts(join_tools):
    result = join_tools.profile_dataset("loans")
    assert result["row_count"] == 3
    assert result["columns"]["loan_status"]["distinct_count"] == 2
    assert result["columns"]["loan_status"]["null_count"] == 0


def test_analyze_key_cardinality_buckets_by_row_count_per_key(join_tools):
    result = join_tools.analyze_key_cardinality("payments", ["loan_id"])
    # L1 has 1 payment, L3 has 2 payments -- two distinct loan_ids in payments total.
    assert result["total_keys"] == 2
    assert result["distribution"] == {"1": 1, "2": 1}


def test_analyze_key_cardinality_unknown_column_raises(join_tools):
    with pytest.raises(ToolError):
        join_tools.analyze_key_cardinality("payments", ["not_a_real_column"])


def test_compare_dataset_keys_finds_loans_with_no_payments(join_tools):
    result = join_tools.compare_dataset_keys("loans", "payments", ["loan_id"])
    assert result["left_only_count"] == 1
    assert result["left_only_sample"] == ["L2"]
    assert result["right_only_count"] == 0
    assert result["matching_key_count"] == 2


def test_compare_dataset_keys_unknown_dataset_raises(join_tools):
    with pytest.raises(ToolError):
        join_tools.compare_dataset_keys("loans", "not_a_real_dataset", ["loan_id"])


def test_aggregate_dataset_groups_and_sums(join_tools):
    result = join_tools.aggregate_dataset("payments", ["loan_id"], [{"agg": "count"}, {"column": "amount_paid", "agg": "sum"}])
    groups_by_loan = {g["loan_id"]: g for g in result["groups"]}
    assert groups_by_loan["L1"]["count"] == 1
    assert groups_by_loan["L1"]["sum_amount_paid"] == 500.0
    assert groups_by_loan["L3"]["count"] == 2
    assert groups_by_loan["L3"]["sum_amount_paid"] == 6000.0


def test_aggregate_dataset_applies_filters(join_tools):
    result = join_tools.aggregate_dataset(
        "loans", ["loan_status"], [{"agg": "count"}], filters={"loan_status": "ACTIVE"}
    )
    assert result["total_groups"] == 1
    assert result["groups"][0] == {"loan_status": "ACTIVE", "count": 2}


def test_aggregate_dataset_rejects_unsupported_agg(join_tools):
    with pytest.raises(ToolError):
        join_tools.aggregate_dataset("payments", ["loan_id"], [{"column": "amount_paid", "agg": "median"}])


def test_aggregate_dataset_count_does_not_require_a_column(join_tools):
    result = join_tools.aggregate_dataset("loans", ["loan_status"], [{"agg": "count"}])
    assert result["total_groups"] == 2


def test_sample_dataset_returns_bounded_filtered_rows(join_tools):
    result = join_tools.sample_dataset("loans", filters={"loan_status": "ACTIVE"}, limit=1)
    assert result["matching_row_count"] == 2
    assert len(result["samples"]) == 1
    assert result["samples"][0]["loan_status"] == "ACTIVE"


def test_sample_dataset_selects_columns(join_tools):
    result = join_tools.sample_dataset("loans", columns=["loan_id"], limit=10)
    assert all(set(row) == {"loan_id"} for row in result["samples"])


def test_sample_dataset_rejects_invalid_limit(join_tools):
    with pytest.raises(ToolError):
        join_tools.sample_dataset("loans", limit=0)


def test_get_relevant_etl_source_can_target_the_join_based_etl():
    tools = DiagnosticTools(
        loans_df=pd.DataFrame(JOIN_LOANS),
        payments_df=pd.DataFrame(JOIN_PAYMENTS),
        portfolio_summary=SUMMARY,
        business_rules=BUSINESS_RULES,
        validation_results=VALIDATION_RESULTS,
        validation_rules=VALIDATION_RULES,
        lineage=LINEAGE,
        data_dictionary=DATA_DICTIONARY,
        pipeline_run=None,
        etl_function_name="compute_portfolio_summary_with_payment_join",
    )
    result = tools.get_relevant_etl_source("total_successful_payments")
    assert result["function"] == "compute_portfolio_summary_with_payment_join"
    assert "def compute_portfolio_summary_with_payment_join" in result["source"]


def test_no_tool_exposes_write_or_execute_capability():
    # Tool names aren't all "get_"-prefixed (list_datasets, aggregate_dataset, etc. are
    # generic investigation verbs, not getters) -- the actual safety property is that none
    # of them can write, delete, or execute anything, checked by keyword instead.
    write_like_keywords = ("write", "delete", "exec", "eval", "subprocess", "system", "patch", "apply")
    for name, _ in inspect.getmembers(DiagnosticTools, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        assert not any(kw in name.lower() for kw in write_like_keywords), f"unexpected write/execute-like method: {name}"


def test_allowlist_matches_actual_tool_methods():
    public_methods = {
        name for name, _ in inspect.getmembers(DiagnosticTools, predicate=inspect.isfunction) if not name.startswith("_")
    }
    assert ALLOWLISTED_TOOL_NAMES == public_methods
