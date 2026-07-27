"""Generic, dataset-registry-backed investigation tools shared across diagnosis tool surfaces.

Extracted from the general-purpose (non-scenario-specific) half of src/diagnostic_tools.py
so a second tool surface (the lifecycle model) doesn't need a second copy of this logic --
src/diagnostic_tools.py itself is left completely unmodified. These are pure functions over
a `registry: dict[str, pd.DataFrame]` -- the closed set of dataset aliases a caller has
already decided to expose -- never touching the filesystem or accepting a path from the model.

Tools return facts (counts, samples, schema), never a diagnosis. Invalid arguments raise
ToolError, which a dispatcher turns into a small structured error message fed back to the
model instead of crashing the agent session.
"""

from __future__ import annotations

import pandas as pd

MAX_SAMPLE_LIMIT = 20
DEFAULT_SAMPLE_LIMIT = 5
MAX_AGGREGATE_GROUPS = 50
MAX_JOIN_ROWS = 50
_SUPPORTED_AGGS = frozenset({"count", "sum", "mean", "nunique"})


class ToolError(Exception):
    """Raised for invalid tool arguments. Caught by a dispatcher; never crashes the agent loop."""


def _require_dataset(registry: dict, dataset: str) -> pd.DataFrame:
    if dataset not in registry:
        raise ToolError(f"unknown dataset {dataset!r}; known datasets: {sorted(registry)}")
    return registry[dataset]


def _require_known_columns(df: pd.DataFrame, columns: list, dataset: str) -> None:
    if not isinstance(columns, list) or not columns or not all(isinstance(c, str) for c in columns):
        raise ToolError("columns must be a non-empty list of strings")
    unknown = [c for c in columns if c not in df.columns]
    if unknown:
        raise ToolError(f"unknown column(s) {unknown} for dataset {dataset!r}; known columns: {sorted(df.columns)}")


_COMPARISON_OPS = {
    "gt": lambda series, value: series > value,
    "gte": lambda series, value: series >= value,
    "lt": lambda series, value: series < value,
    "lte": lambda series, value: series <= value,
    "ne": lambda series, value: series != value,
}

_FILTER_CONDITION_DESCRIPTION = (
    "a scalar (equality), {'in': [...]} (set membership), or exactly one of "
    f"{{{', '.join(repr(op) for op in _COMPARISON_OPS)}}} (comparison)"
)


def _apply_filters(df: pd.DataFrame, dataset: str, filters: dict) -> pd.DataFrame:
    if not filters:
        return df
    if not isinstance(filters, dict):
        raise ToolError(f"filters must be an object of {{column: condition}} where each condition is {_FILTER_CONDITION_DESCRIPTION}")
    _require_known_columns(df, list(filters), dataset)
    result = df
    for column, condition in filters.items():
        if isinstance(condition, dict):
            if "in" in condition:
                if not isinstance(condition["in"], list):
                    raise ToolError(f"filter for column {column!r}: 'in' must be a list")
                result = result[result[column].isin(condition["in"])]
            else:
                matched_ops = [op for op in _COMPARISON_OPS if op in condition]
                if len(matched_ops) != 1:
                    raise ToolError(f"filter for column {column!r} must be {_FILTER_CONDITION_DESCRIPTION}")
                op = matched_ops[0]
                result = result[_COMPARISON_OPS[op](result[column], condition[op])]
        else:
            result = result[result[column] == condition]
    return result


def list_datasets(registry: dict) -> dict:
    """The dataset aliases available in this registry."""
    return {"datasets": sorted(registry)}


def get_dataset_schema(registry: dict, dataset: str) -> dict:
    """Column names, inferred types, and row count for an aliased dataset."""
    df = _require_dataset(registry, dataset)
    return {
        "dataset": dataset,
        "row_count": int(len(df)),
        "columns": {str(col): str(dtype) for col, dtype in df.dtypes.items()},
    }


def profile_dataset(registry: dict, dataset: str) -> dict:
    """Per-column null and distinct-value counts for an aliased dataset."""
    df = _require_dataset(registry, dataset)
    if df.empty:
        return {"dataset": dataset, "row_count": 0, "columns": {}}
    return {
        "dataset": dataset,
        "row_count": int(len(df)),
        "columns": {
            str(col): {"null_count": int(df[col].isna().sum()), "distinct_count": int(df[col].nunique())}
            for col in df.columns
        },
    }


def analyze_key_cardinality(registry: dict, dataset: str, key_columns: list) -> dict:
    """Distribution of how many rows share each value of key_columns within one dataset
    -- e.g. how many loan_ids have exactly 1, 2, or 3+ rows."""
    df = _require_dataset(registry, dataset)
    _require_known_columns(df, key_columns, dataset)
    if df.empty:
        return {"dataset": dataset, "key_columns": key_columns, "total_keys": 0, "distribution": {}}
    counts = df.groupby(key_columns).size()
    distribution: dict[str, int] = {}
    for n in counts:
        bucket = str(n) if n < 3 else "3+"
        distribution[bucket] = distribution.get(bucket, 0) + 1
    return {
        "dataset": dataset,
        "key_columns": key_columns,
        "total_keys": int(counts.shape[0]),
        "distribution": distribution,
    }


def compare_dataset_keys(registry: dict, left_dataset: str, right_dataset: str, join_keys: list) -> dict:
    """Set difference between two datasets' key values -- keys only on the left, only on
    the right, and matching. E.g. comparing loans.loan_id against payments.loan_id
    directly surfaces loans with no corresponding payment rows at all."""
    left_df = _require_dataset(registry, left_dataset)
    right_df = _require_dataset(registry, right_dataset)
    _require_known_columns(left_df, join_keys, left_dataset)
    _require_known_columns(right_df, join_keys, right_dataset)

    left_keys = set(map(tuple, left_df[join_keys].drop_duplicates().to_numpy().tolist())) if not left_df.empty else set()
    right_keys = (
        set(map(tuple, right_df[join_keys].drop_duplicates().to_numpy().tolist())) if not right_df.empty else set()
    )
    left_only = sorted(left_keys - right_keys)
    right_only = sorted(right_keys - left_keys)

    def _unwrap(keys: list) -> list:
        return [list(k) if len(join_keys) > 1 else k[0] for k in keys]

    return {
        "left_dataset": left_dataset,
        "right_dataset": right_dataset,
        "join_keys": join_keys,
        "left_only_count": len(left_only),
        "right_only_count": len(right_only),
        "matching_key_count": len(left_keys & right_keys),
        "left_only_sample": _unwrap(left_only[:10]),
        "right_only_sample": _unwrap(right_only[:10]),
    }


def aggregate_dataset(registry: dict, dataset: str, group_by: list, metrics: list, filters: dict = None) -> dict:
    """Generic group-by aggregation over one dataset -- e.g. count and sum(amount) per
    payment_status. metrics is a list of {"agg": "count"} or
    {"column": ..., "agg": "sum"|"mean"|"nunique"}."""
    df = _require_dataset(registry, dataset)
    _require_known_columns(df, group_by, dataset)
    filtered = _apply_filters(df, dataset, filters or {})

    if not isinstance(metrics, list) or not metrics:
        raise ToolError("metrics must be a non-empty list")
    for metric in metrics:
        if not isinstance(metric, dict) or "agg" not in metric:
            raise ToolError("each metric must be an object with an 'agg' key")
        if metric["agg"] not in _SUPPORTED_AGGS:
            raise ToolError(f"unsupported agg {metric['agg']!r}; supported: {sorted(_SUPPORTED_AGGS)}")
        if metric["agg"] != "count" and "column" not in metric:
            raise ToolError(f"agg {metric['agg']!r} requires a 'column'")
        if "column" in metric:
            _require_known_columns(filtered, [metric["column"]], dataset)

    if filtered.empty:
        return {"dataset": dataset, "group_by": group_by, "total_groups": 0, "truncated": False, "groups": []}

    grouped = filtered.groupby(group_by)
    records = []
    for key, group in grouped:
        key_tuple = key if isinstance(key, tuple) else (key,)
        record = dict(zip(group_by, key_tuple))
        for metric in metrics:
            if metric["agg"] == "count":
                record["count"] = int(len(group))
            else:
                column, agg = metric["column"], metric["agg"]
                value = getattr(group[column], agg)()
                label = f"{agg}_{column}"
                record[label] = round(float(value), 2) if agg in ("sum", "mean") else int(value)
        records.append(record)

    total_groups = len(records)
    truncated = total_groups > MAX_AGGREGATE_GROUPS
    return {
        "dataset": dataset,
        "group_by": group_by,
        "total_groups": total_groups,
        "truncated": truncated,
        "groups": records[:MAX_AGGREGATE_GROUPS],
    }


def join_datasets(
    registry: dict,
    left_dataset: str,
    right_dataset: str,
    join_keys: list,
    left_filters: dict = None,
    right_filters: dict = None,
) -> dict:
    """Row-level inner join of two datasets on join_keys (columns present in both), each
    side optionally pre-filtered first. Unlike compare_dataset_keys (which only reports
    which keys match), this returns the actual merged rows -- e.g. joining
    underwriting_performance (filtered to breakdown_type='risk_segment') to
    delinquency_default on breakdown_value, to compare approval and default rates for the
    same segment in one result."""
    left_df = _require_dataset(registry, left_dataset)
    right_df = _require_dataset(registry, right_dataset)
    if not isinstance(join_keys, list) or not join_keys or not all(isinstance(k, str) for k in join_keys):
        raise ToolError("join_keys must be a non-empty list of strings")
    _require_known_columns(left_df, join_keys, left_dataset)
    _require_known_columns(right_df, join_keys, right_dataset)

    left_filtered = _apply_filters(left_df, left_dataset, left_filters or {})
    right_filtered = _apply_filters(right_df, right_dataset, right_filters or {})
    merged = left_filtered.merge(right_filtered, on=join_keys, suffixes=(f"_{left_dataset}", f"_{right_dataset}"))

    total = len(merged)
    return {
        "left_dataset": left_dataset,
        "right_dataset": right_dataset,
        "join_keys": join_keys,
        "matched_row_count": int(total),
        "truncated": total > MAX_JOIN_ROWS,
        "rows": merged.head(MAX_JOIN_ROWS).to_dict(orient="records"),
    }


def sample_dataset(registry: dict, dataset: str, filters: dict = None, columns: list = None, limit: int = DEFAULT_SAMPLE_LIMIT) -> dict:
    """Bounded row sampling from an aliased dataset, with optional equality/'in' filters and column selection."""
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0 or limit > MAX_SAMPLE_LIMIT:
        raise ToolError(f"limit must be an integer between 1 and {MAX_SAMPLE_LIMIT}, got {limit!r}")
    df = _require_dataset(registry, dataset)
    filtered = _apply_filters(df, dataset, filters or {})
    if columns:
        _require_known_columns(filtered, columns, dataset)
        filtered = filtered[columns]
    return {"dataset": dataset, "matching_row_count": int(len(filtered)), "samples": filtered.head(limit).to_dict(orient="records")}


DATASET_REGISTRY_TOOL_NAMES = frozenset(
    {
        "list_datasets",
        "get_dataset_schema",
        "profile_dataset",
        "analyze_key_cardinality",
        "compare_dataset_keys",
        "aggregate_dataset",
        "join_datasets",
        "sample_dataset",
    }
)

DATASET_REGISTRY_TOOL_SPECS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "list_datasets",
            "description": "Return the dataset aliases available for this investigation.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dataset_schema",
            "description": "Return column names, inferred types, and row count for an aliased dataset.",
            "parameters": {
                "type": "object",
                "properties": {"dataset": {"type": "string", "description": "A dataset alias from list_datasets."}},
                "required": ["dataset"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "profile_dataset",
            "description": "Return per-column null-count and distinct-value-count for an aliased dataset.",
            "parameters": {
                "type": "object",
                "properties": {"dataset": {"type": "string", "description": "A dataset alias from list_datasets."}},
                "required": ["dataset"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_key_cardinality",
            "description": "Return the distribution of how many rows share each value of key_columns within one dataset -- e.g. how many loan_ids have exactly 1, 2, or 3+ rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "A dataset alias from list_datasets."},
                    "key_columns": {"type": "array", "items": {"type": "string"}, "description": "Column(s) to group by."},
                },
                "required": ["dataset", "key_columns"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_dataset_keys",
            "description": "Return the set difference between two datasets' key values on join_keys: how many keys exist only on the left, only on the right, or on both, with samples. E.g. comparing loans.loan_id against payments.loan_id surfaces loans with no matching payment rows at all.",
            "parameters": {
                "type": "object",
                "properties": {
                    "left_dataset": {"type": "string", "description": "A dataset alias from list_datasets."},
                    "right_dataset": {"type": "string", "description": "A dataset alias from list_datasets."},
                    "join_keys": {"type": "array", "items": {"type": "string"}, "description": "Column(s) present in both datasets to compare."},
                },
                "required": ["left_dataset", "right_dataset", "join_keys"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "aggregate_dataset",
            "description": "Return a generic group-by aggregation over one dataset -- e.g. count and sum(amount) per payment_status.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "A dataset alias from list_datasets."},
                    "group_by": {"type": "array", "items": {"type": "string"}, "description": "Column(s) to group by."},
                    "metrics": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string", "description": "Required unless agg is 'count'."},
                                "agg": {"type": "string", "enum": ["count", "sum", "mean", "nunique"]},
                            },
                            "required": ["agg"],
                        },
                        "description": "e.g. [{\"agg\": \"count\"}, {\"column\": \"amount\", \"agg\": \"sum\"}]",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional {column: value}, {column: {\"in\": [...]}}, or {column: {\"gt\"|\"gte\"|\"lt\"|\"lte\"|\"ne\": value}} filters applied before aggregating.",
                    },
                },
                "required": ["dataset", "group_by", "metrics"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "join_datasets",
            "description": "Return the row-level inner join of two datasets on join_keys (columns present in both), each side optionally pre-filtered first. Unlike compare_dataset_keys (key-set diff only), this returns the actual merged rows so you can compare columns from both datasets together.",
            "parameters": {
                "type": "object",
                "properties": {
                    "left_dataset": {"type": "string", "description": "A dataset alias from list_datasets."},
                    "right_dataset": {"type": "string", "description": "A dataset alias from list_datasets."},
                    "join_keys": {"type": "array", "items": {"type": "string"}, "description": "Column(s) present in both datasets to join on."},
                    "left_filters": {
                        "type": "object",
                        "description": "Optional {column: value}, {column: {\"in\": [...]}}, or {column: {\"gt\"|\"gte\"|\"lt\"|\"lte\"|\"ne\": value}} filters applied to left_dataset before joining.",
                    },
                    "right_filters": {
                        "type": "object",
                        "description": "Optional {column: value}, {column: {\"in\": [...]}}, or {column: {\"gt\"|\"gte\"|\"lt\"|\"lte\"|\"ne\": value}} filters applied to right_dataset before joining.",
                    },
                },
                "required": ["left_dataset", "right_dataset", "join_keys"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "sample_dataset",
            "description": "Return up to `limit` rows from an aliased dataset, with optional filters and column selection.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "A dataset alias from list_datasets."},
                    "filters": {
                        "type": "object",
                        "description": "Optional {column: value}, {column: {\"in\": [...]}}, or {column: {\"gt\"|\"gte\"|\"lt\"|\"lte\"|\"ne\": value}} filters.",
                    },
                    "columns": {"type": "array", "items": {"type": "string"}, "description": "Optional column subset to return."},
                    "limit": {"type": "integer", "description": f"1-{MAX_SAMPLE_LIMIT}, defaults to {DEFAULT_SAMPLE_LIMIT}."},
                },
                "required": ["dataset"],
            },
        },
    },
]
