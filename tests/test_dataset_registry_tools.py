"""Tests for the shared, dataset-registry-backed generic investigation tools."""

from __future__ import annotations

import pandas as pd
import pytest

from src.dataset_registry_tools import (
    MAX_JOIN_ROWS,
    ToolError,
    aggregate_dataset,
    analyze_key_cardinality,
    compare_dataset_keys,
    get_dataset_schema,
    join_datasets,
    list_datasets,
    profile_dataset,
    sample_dataset,
)

LOANS = pd.DataFrame(
    [
        {"loan_id": "L1", "principal_amount": 1000.0, "loan_status": "ACTIVE"},
        {"loan_id": "L2", "principal_amount": 2000.0, "loan_status": "CLOSED"},
        {"loan_id": "L3", "principal_amount": 1500.0, "loan_status": "ACTIVE"},
    ]
)
PAYMENT_EVENTS = pd.DataFrame(
    [
        {"loan_id": "L1", "payment_status": "PAID", "amount": 100.0},
        {"loan_id": "L1", "payment_status": "LATE", "amount": 50.0},
        {"loan_id": "L2", "payment_status": "PAID", "amount": 200.0},
    ]
)
REGISTRY = {"loans": LOANS, "payment_events": PAYMENT_EVENTS}


def test_list_datasets_returns_sorted_aliases():
    assert list_datasets(REGISTRY) == {"datasets": ["loans", "payment_events"]}


def test_get_dataset_schema_returns_columns_and_row_count():
    result = get_dataset_schema(REGISTRY, "loans")
    assert result["row_count"] == 3
    assert set(result["columns"]) == {"loan_id", "principal_amount", "loan_status"}


def test_get_dataset_schema_unknown_dataset_raises():
    with pytest.raises(ToolError):
        get_dataset_schema(REGISTRY, "nonexistent")


def test_profile_dataset_reports_null_and_distinct_counts():
    result = profile_dataset(REGISTRY, "loans")
    assert result["columns"]["loan_status"]["distinct_count"] == 2
    assert result["columns"]["loan_status"]["null_count"] == 0


def test_analyze_key_cardinality_buckets_correctly():
    result = analyze_key_cardinality(REGISTRY, "payment_events", ["loan_id"])
    # L1 has 2 event rows, L2 has 1.
    assert result["distribution"] == {"1": 1, "2": 1}


def test_compare_dataset_keys_finds_left_only_loans():
    result = compare_dataset_keys(REGISTRY, "loans", "payment_events", ["loan_id"])
    assert result["left_only_sample"] == ["L3"]
    assert result["matching_key_count"] == 2


def test_aggregate_dataset_groups_and_sums():
    result = aggregate_dataset(
        REGISTRY, "payment_events", ["payment_status"], [{"agg": "count"}, {"column": "amount", "agg": "sum"}]
    )
    by_status = {g["payment_status"]: g for g in result["groups"]}
    assert by_status["PAID"]["count"] == 2
    assert by_status["PAID"]["sum_amount"] == 300.0


def test_aggregate_dataset_applies_filters_before_aggregating():
    result = aggregate_dataset(
        REGISTRY, "payment_events", ["loan_id"], [{"agg": "count"}], filters={"payment_status": {"in": ["PAID"]}}
    )
    assert result["total_groups"] == 2


def test_aggregate_dataset_rejects_unsupported_agg():
    with pytest.raises(ToolError):
        aggregate_dataset(REGISTRY, "loans", ["loan_status"], [{"agg": "median", "column": "principal_amount"}])


def test_sample_dataset_respects_limit_and_filters():
    result = sample_dataset(REGISTRY, "loans", filters={"loan_status": "ACTIVE"}, limit=1)
    assert result["matching_row_count"] == 2
    assert len(result["samples"]) == 1


def test_sample_dataset_rejects_limit_out_of_range():
    with pytest.raises(ToolError):
        sample_dataset(REGISTRY, "loans", limit=0)


# --- comparison-operator filters (gt/gte/lt/lte/ne) ----------------------------------------


def test_sample_dataset_filters_with_gt():
    result = sample_dataset(REGISTRY, "loans", filters={"principal_amount": {"gt": 1000.0}})
    assert {r["loan_id"] for r in result["samples"]} == {"L2", "L3"}


def test_sample_dataset_filters_with_lte():
    result = sample_dataset(REGISTRY, "loans", filters={"principal_amount": {"lte": 1000.0}})
    assert {r["loan_id"] for r in result["samples"]} == {"L1"}


def test_sample_dataset_filters_with_ne():
    result = sample_dataset(REGISTRY, "loans", filters={"loan_status": {"ne": "ACTIVE"}})
    assert {r["loan_id"] for r in result["samples"]} == {"L2"}


def test_filter_condition_with_multiple_operator_keys_rejected():
    with pytest.raises(ToolError):
        sample_dataset(REGISTRY, "loans", filters={"principal_amount": {"gt": 1000.0, "lt": 2000.0}})


def test_filter_condition_with_unrecognized_operator_key_rejected():
    with pytest.raises(ToolError):
        sample_dataset(REGISTRY, "loans", filters={"principal_amount": {"between": [1000.0, 2000.0]}})


# --- join_datasets ---------------------------------------------------------------------


def test_join_datasets_merges_matching_rows():
    result = join_datasets(REGISTRY, "loans", "payment_events", ["loan_id"])
    assert result["matched_row_count"] == 3  # L1 has 2 payment rows, L2 has 1, L3 has none
    assert result["truncated"] is False
    l1_rows = [r for r in result["rows"] if r["loan_id"] == "L1"]
    assert {r["payment_status"] for r in l1_rows} == {"PAID", "LATE"}


def test_join_datasets_applies_filters_to_each_side_independently():
    result = join_datasets(
        REGISTRY,
        "loans",
        "payment_events",
        ["loan_id"],
        left_filters={"loan_status": "ACTIVE"},
        right_filters={"payment_status": "PAID"},
    )
    # Only L1 is ACTIVE and has a PAID event; L3 is ACTIVE but has no payment events at all.
    assert result["matched_row_count"] == 1
    assert result["rows"][0]["loan_id"] == "L1"


def test_join_datasets_rejects_unknown_join_key():
    with pytest.raises(ToolError):
        join_datasets(REGISTRY, "loans", "payment_events", ["not_a_real_column"])


def test_join_datasets_truncates_at_max_join_rows():
    left = pd.DataFrame([{"k": i} for i in range(MAX_JOIN_ROWS + 10)])
    right = pd.DataFrame([{"k": i} for i in range(MAX_JOIN_ROWS + 10)])
    registry = {"left": left, "right": right}
    result = join_datasets(registry, "left", "right", ["k"])
    assert result["matched_row_count"] == MAX_JOIN_ROWS + 10
    assert result["truncated"] is True
    assert len(result["rows"]) == MAX_JOIN_ROWS
