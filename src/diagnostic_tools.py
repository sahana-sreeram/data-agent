"""Read-only, allowlisted investigation tools for the diagnosis agent.

All data (loans, payments, portfolio summary, business rules, validation
results/rules, lineage, data dictionary, pipeline run) is loaded ONCE, at
DiagnosticTools construction, from fixed paths chosen by the CLI -- never by
the model. Every tool method only slices this in-memory data; none of them
touch the filesystem, run a command, or accept a path from the caller.

Tools return facts (counts, samples, source text, config fragments), never a
diagnosis. Invalid arguments raise ToolError, which the dispatcher turns into
a small structured error message fed back to the model instead of crashing
the whole agent session.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass

import pandas as pd

from src import transform as transform_module

MAX_SAMPLE_LIMIT = 20
DEFAULT_SAMPLE_LIMIT = 5

# Explicit, fixed allowlist of ETL functions get_relevant_etl_source may
# introspect -- resolved by NAME (a plain string field on DiagnosticTools),
# never by handing around a bare callable. Keeps the class's public surface
# unambiguous (no function-valued attribute for tool-introspection tests to
# mistake for a method) and keeps the target set of introspectable functions
# closed and reviewable.
ETL_FUNCTIONS_BY_NAME = {
    "compute_portfolio_summary": transform_module.compute_portfolio_summary,
    "compute_portfolio_summary_from_payment_events": transform_module.compute_portfolio_summary_from_payment_events,
    "compute_portfolio_summary_with_payment_join": transform_module.compute_portfolio_summary_with_payment_join,
}
DEFAULT_ETL_FUNCTION_NAME = "compute_portfolio_summary"

MAX_AGGREGATE_GROUPS = 50
_SUPPORTED_AGGS = frozenset({"count", "sum", "mean", "nunique"})


class ToolError(Exception):
    """Raised for invalid tool arguments. Caught by dispatch_tool; never crashes the agent loop."""


@dataclass
class DiagnosticTools:
    loans_df: pd.DataFrame
    payments_df: pd.DataFrame
    portfolio_summary: dict
    business_rules: dict
    validation_results: dict
    validation_rules: dict
    lineage: dict
    data_dictionary: dict
    pipeline_run: dict | None
    # payment_events_df/etl_function_name let the SAME tool surface serve
    # incidents that use the event-stream ETL instead of the one-row-per-
    # payment ETL. Left at their defaults for scenarios that don't involve
    # payment events at all -- the event-specific tools then raise a clear
    # ToolError rather than silently returning empty data.
    payment_events_df: pd.DataFrame | None = None
    etl_function_name: str = DEFAULT_ETL_FUNCTION_NAME

    def _portfolio_summary_fields(self) -> dict:
        return self.data_dictionary.get("portfolio_summary", {}).get("fields", {})

    def _require_known_metric(self, metric_name: str) -> dict:
        fields = self._portfolio_summary_fields()
        if metric_name not in fields:
            raise ToolError(f"unknown metric_name {metric_name!r}; known metrics: {sorted(fields)}")
        return fields

    def _dataset_registry(self) -> dict[str, pd.DataFrame]:
        """The closed set of dataset aliases available for THIS incident -- derived from
        whichever raw data was already loaded for it, never from arbitrary file access."""
        if self.payment_events_df is not None:
            return {"loans": self.loans_df, "payment_events": self.payment_events_df}
        return {"loans": self.loans_df, "payments": self.payments_df}

    def _require_dataset(self, dataset: str) -> pd.DataFrame:
        registry = self._dataset_registry()
        if dataset not in registry:
            raise ToolError(f"unknown dataset {dataset!r}; known datasets: {sorted(registry)}")
        return registry[dataset]

    def _require_known_columns(self, df: pd.DataFrame, columns: list, dataset: str) -> None:
        if not isinstance(columns, list) or not columns or not all(isinstance(c, str) for c in columns):
            raise ToolError("columns must be a non-empty list of strings")
        unknown = [c for c in columns if c not in df.columns]
        if unknown:
            raise ToolError(f"unknown column(s) {unknown} for dataset {dataset!r}; known columns: {sorted(df.columns)}")

    def _apply_filters(self, df: pd.DataFrame, dataset: str, filters: dict) -> pd.DataFrame:
        if not filters:
            return df
        if not isinstance(filters, dict):
            raise ToolError("filters must be an object of {column: value} or {column: {'in': [...]}}")
        self._require_known_columns(df, list(filters), dataset)
        result = df
        for column, condition in filters.items():
            if isinstance(condition, dict):
                if "in" not in condition or not isinstance(condition["in"], list):
                    raise ToolError(f"filter for column {column!r} must be a scalar or an object with an 'in' list")
                result = result[result[column].isin(condition["in"])]
            else:
                result = result[result[column] == condition]
        return result

    def list_datasets(self) -> dict:
        """The dataset aliases available for this incident."""
        return {"datasets": sorted(self._dataset_registry())}

    def get_dataset_schema(self, dataset: str) -> dict:
        """Column names, inferred types, and row count for an aliased dataset."""
        df = self._require_dataset(dataset)
        return {
            "dataset": dataset,
            "row_count": int(len(df)),
            "columns": {str(col): str(dtype) for col, dtype in df.dtypes.items()},
        }

    def profile_dataset(self, dataset: str) -> dict:
        """Per-column null and distinct-value counts for an aliased dataset."""
        df = self._require_dataset(dataset)
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

    def analyze_key_cardinality(self, dataset: str, key_columns: list) -> dict:
        """Distribution of how many rows share each value of key_columns within one dataset
        -- e.g. how many loan_ids have exactly 1, 2, or 3+ payment rows."""
        df = self._require_dataset(dataset)
        self._require_known_columns(df, key_columns, dataset)
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

    def compare_dataset_keys(self, left_dataset: str, right_dataset: str, join_keys: list) -> dict:
        """Set difference between two datasets' key values -- keys only on the left, only on
        the right, and matching. E.g. comparing loans.loan_id against payments.loan_id
        directly surfaces loans with no corresponding payment rows at all."""
        left_df = self._require_dataset(left_dataset)
        right_df = self._require_dataset(right_dataset)
        self._require_known_columns(left_df, join_keys, left_dataset)
        self._require_known_columns(right_df, join_keys, right_dataset)

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

    def aggregate_dataset(self, dataset: str, group_by: list, metrics: list, filters: dict = None) -> dict:
        """Generic group-by aggregation over one dataset -- e.g. count and sum(amount_paid)
        per payment_status. metrics is a list of {"agg": "count"} or {"column": ..., "agg": "sum"|"mean"|"nunique"}."""
        df = self._require_dataset(dataset)
        self._require_known_columns(df, group_by, dataset)
        filtered = self._apply_filters(df, dataset, filters or {})

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
                self._require_known_columns(filtered, [metric["column"]], dataset)

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

    def sample_dataset(self, dataset: str, filters: dict = None, columns: list = None, limit: int = DEFAULT_SAMPLE_LIMIT) -> dict:
        """Bounded row sampling from an aliased dataset, with optional equality/'in' filters and column selection."""
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0 or limit > MAX_SAMPLE_LIMIT:
            raise ToolError(f"limit must be an integer between 1 and {MAX_SAMPLE_LIMIT}, got {limit!r}")
        df = self._require_dataset(dataset)
        filtered = self._apply_filters(df, dataset, filters or {})
        if columns:
            self._require_known_columns(filtered, columns, dataset)
            filtered = filtered[columns]
        return {"dataset": dataset, "matching_row_count": int(len(filtered)), "samples": filtered.head(limit).to_dict(orient="records")}

    def get_validation_results(self) -> dict:
        """The full validation_results.json content."""
        return self.validation_results

    def get_failed_checks(self) -> dict:
        """Just the FAIL entries from validation_results.checks."""
        failed = [c for c in self.validation_results.get("checks", []) if c.get("status") == "FAIL"]
        return {"failed_checks": failed}

    def get_portfolio_summary(self) -> dict:
        """The ETL's reported portfolio_summary.json content."""
        return self.portfolio_summary

    def get_business_rules(self) -> dict:
        """context/business_rules.json content, verbatim."""
        return self.business_rules

    def get_metric_definition(self, metric_name: str) -> dict:
        """The data_dictionary.json entry for a portfolio_summary field."""
        fields = self._require_known_metric(metric_name)
        return {metric_name: fields[metric_name]}

    def get_metric_lineage(self, metric_name: str) -> dict:
        """The lineage.json entry for the dataset that contains this metric."""
        self._require_known_metric(metric_name)
        entry = self.lineage.get("datasets", {}).get("processed.portfolio_summary")
        if entry is None:
            raise ToolError("lineage entry for 'processed.portfolio_summary' not found")
        return {"metric_name": metric_name, "lineage": entry}

    def get_payment_status_counts(self) -> dict:
        """{status: count} across all raw payments."""
        if self.payments_df.empty:
            return {}
        return {str(k): int(v) for k, v in self.payments_df["payment_status"].value_counts().items()}

    def get_payment_amount_totals_by_status(self) -> dict:
        """{status: sum(amount_paid)} across all raw payments."""
        if self.payments_df.empty:
            return {}
        totals = self.payments_df.groupby("payment_status")["amount_paid"].sum()
        return {str(k): round(float(v), 2) for k, v in totals.items()}

    def get_payment_samples_by_status(self, status: str, limit: int = DEFAULT_SAMPLE_LIMIT) -> dict:
        """Up to `limit` raw payment records for a status actually observed in the data."""
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0 or limit > MAX_SAMPLE_LIMIT:
            raise ToolError(f"limit must be an integer between 1 and {MAX_SAMPLE_LIMIT}, got {limit!r}")

        observed_statuses = set(self.payments_df["payment_status"].unique()) if not self.payments_df.empty else set()
        if status not in observed_statuses:
            raise ToolError(
                f"status {status!r} was not observed in payments data; observed statuses: {sorted(observed_statuses)}"
            )

        matching = self.payments_df[self.payments_df["payment_status"] == status].head(limit)
        return {"status": status, "samples": matching.to_dict(orient="records")}

    def get_source_record_counts(self) -> dict:
        """Row counts for the raw loans and payments tables."""
        return {"loans": int(len(self.loans_df)), "payments": int(len(self.payments_df))}

    def get_pipeline_run_metadata(self) -> dict:
        """pipeline_run.json content, if it was produced for this run.

        Note: etl_status reflects only whether the ETL executed without
        raising an error. It does not indicate whether the ETL's output is
        correct relative to the currently approved business rules -- a stale
        ETL (or one run against outdated configuration) can report SUCCESS
        every time while still producing wrong numbers.
        """
        if self.pipeline_run is None:
            return {"available": False}
        return {"available": True, **self.pipeline_run}

    def get_relevant_etl_source(self, metric_name: str) -> dict:
        """The bounded source of the ETL function that computes portfolio_summary metrics for this incident."""
        self._require_known_metric(metric_name)
        etl_function = ETL_FUNCTIONS_BY_NAME[self.etl_function_name]
        source = inspect.getsource(etl_function)
        return {
            "metric_name": metric_name,
            "file": "src/transform.py",
            "function": self.etl_function_name,
            "source": source,
        }

    def get_data_dictionary_entry(self, dataset: str, field: str) -> dict:
        """One field's documentation entry from context/data_dictionary.json."""
        dataset_entry = self.data_dictionary.get(dataset)
        if dataset_entry is None:
            raise ToolError(f"unknown dataset {dataset!r}; known datasets: {sorted(self.data_dictionary)}")
        fields = dataset_entry.get("fields", {})
        if field not in fields:
            raise ToolError(f"unknown field {field!r} for dataset {dataset!r}; known fields: {sorted(fields)}")
        return {"dataset": dataset, "field": field, "entry": fields[field]}

    def _require_payment_events(self) -> pd.DataFrame:
        if self.payment_events_df is None:
            raise ToolError("no payment_events data is available for this incident")
        return self.payment_events_df

    def get_payment_event_type_counts(self) -> dict:
        """{event_type: count} across all raw payment-event rows."""
        events_df = self._require_payment_events()
        if events_df.empty:
            return {}
        return {str(k): int(v) for k, v in events_df["event_type"].value_counts().items()}

    def get_payment_amount_totals_by_event_type(self) -> dict:
        """{event_type: sum(amount)} across all raw payment-event rows."""
        events_df = self._require_payment_events()
        if events_df.empty:
            return {}
        totals = events_df.groupby("event_type")["amount"].sum()
        return {str(k): round(float(v), 2) for k, v in totals.items()}

    def get_payment_event_cardinality_summary(self) -> dict:
        """Distribution of total event-row-count per logical payment_id (any event_type)."""
        events_df = self._require_payment_events()
        if events_df.empty:
            return {"total_logical_payments": 0, "distribution": {}}
        counts = events_df.groupby("payment_id").size()
        distribution: dict[str, int] = {}
        for n in counts:
            key = str(n) if n < 3 else "3+"
            distribution[key] = distribution.get(key, 0) + 1
        return {"total_logical_payments": int(counts.shape[0]), "distribution": distribution}

    def get_duplicate_payment_id_counts(self, event_type: str = "SETTLED") -> dict:
        """Facts about payment_ids with more than one event row of a given event_type (default SETTLED)."""
        events_df = self._require_payment_events()
        observed_types = set(events_df["event_type"].unique()) if not events_df.empty else set()
        if event_type not in observed_types:
            raise ToolError(
                f"event_type {event_type!r} was not observed in payment_events data; observed: {sorted(observed_types)}"
            )

        matching = events_df[events_df["event_type"] == event_type]
        counts_by_payment = matching.groupby("payment_id").size()
        duplicated = counts_by_payment[counts_by_payment > 1]

        extra_rows = 0
        duplicate_amount_total = 0.0
        if not duplicated.empty:
            extra_rows = int((duplicated - 1).sum())
            for payment_id in duplicated.index:
                rows = matching[matching["payment_id"] == payment_id].sort_values("event_timestamp")
                duplicate_amount_total += float(rows["amount"].iloc[1:].sum())

        return {
            "event_type": event_type,
            "logical_payments_with_multiple_events": int(duplicated.shape[0]),
            "duplicate_event_rows": extra_rows,
            "duplicate_amount_total": round(duplicate_amount_total, 2),
            "sample_payment_ids": sorted(duplicated.index.tolist())[:5],
        }

    def get_payment_event_samples(self, payment_id: str) -> dict:
        """All event rows for one payment_id, sorted by event_timestamp (e.g. to inspect a flagged duplicate)."""
        events_df = self._require_payment_events()
        observed_ids = set(events_df["payment_id"].unique()) if not events_df.empty else set()
        if payment_id not in observed_ids:
            raise ToolError(f"payment_id {payment_id!r} was not observed in payment_events data")
        matching = events_df[events_df["payment_id"] == payment_id].sort_values("event_timestamp")
        return {"payment_id": payment_id, "events": matching.to_dict(orient="records")}


ALLOWLISTED_TOOL_NAMES = frozenset(
    {
        "get_validation_results",
        "get_failed_checks",
        "get_portfolio_summary",
        "get_business_rules",
        "get_metric_definition",
        "get_metric_lineage",
        "get_payment_status_counts",
        "get_payment_amount_totals_by_status",
        "get_payment_samples_by_status",
        "get_source_record_counts",
        "get_pipeline_run_metadata",
        "get_relevant_etl_source",
        "get_data_dictionary_entry",
        "get_payment_event_type_counts",
        "get_payment_amount_totals_by_event_type",
        "get_payment_event_cardinality_summary",
        "get_duplicate_payment_id_counts",
        "get_payment_event_samples",
        "list_datasets",
        "get_dataset_schema",
        "profile_dataset",
        "analyze_key_cardinality",
        "compare_dataset_keys",
        "aggregate_dataset",
        "sample_dataset",
    }
)


def dispatch_tool(tools: DiagnosticTools, name: str, arguments: dict) -> dict:
    """Look up an allowlisted tool by name and call it; never raises."""
    if name not in ALLOWLISTED_TOOL_NAMES:
        return {"error": f"unknown tool {name!r}"}
    method = getattr(tools, name)
    try:
        return method(**arguments)
    except ToolError as exc:
        return {"error": str(exc)}
    except TypeError as exc:
        return {"error": f"invalid arguments for tool {name!r}: {exc}"}


TOOL_SPECS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_validation_results",
            "description": "Return the full validation_results.json content (all checks, not just failures).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_failed_checks",
            "description": "Return only the failing entries from validation_results.checks.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_portfolio_summary",
            "description": "Return the ETL's reported portfolio_summary.json content.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_business_rules",
            "description": "Return context/business_rules.json: which payment statuses are valid/successful, and the balance formula.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_definition",
            "description": "Return the data-dictionary definition of a named portfolio_summary metric.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from portfolio_summary.json."}},
                "required": ["metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_lineage",
            "description": "Return the lineage entry (producer, dependencies) for the dataset containing a named metric.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from portfolio_summary.json."}},
                "required": ["metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_status_counts",
            "description": "Return {status: count} across all raw payment records.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_amount_totals_by_status",
            "description": "Return {status: sum(amount_paid)} across all raw payment records.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_samples_by_status",
            "description": "Return up to `limit` raw payment records for a status that actually appears in the data.",
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "description": "A payment_status value observed in the raw data."},
                    "limit": {"type": "integer", "description": f"1-{MAX_SAMPLE_LIMIT}, defaults to {DEFAULT_SAMPLE_LIMIT}."},
                },
                "required": ["status"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_source_record_counts",
            "description": "Return row counts for the raw loans and payments tables.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pipeline_run_metadata",
            "description": (
                "Return pipeline_run.json content for this run, if available. "
                "Note: etl_status only reflects whether the ETL executed without raising an error -- "
                "it does NOT mean the ETL's output is correct relative to current business rules. "
                "A stale ETL can report SUCCESS every time while still producing wrong numbers."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_relevant_etl_source",
            "description": "Return the bounded source of the ETL function that computes portfolio_summary metrics.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from portfolio_summary.json."}},
                "required": ["metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_data_dictionary_entry",
            "description": "Return one field's documentation entry from context/data_dictionary.json.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "e.g. 'customers', 'loans', 'payments', 'portfolio_summary'."},
                    "field": {"type": "string", "description": "A field name within that dataset."},
                },
                "required": ["dataset", "field"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_event_type_counts",
            "description": "Return {event_type: count} across all raw payment-event rows. Only available for incidents with a payment-events source.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_amount_totals_by_event_type",
            "description": "Return {event_type: sum(amount)} across all raw payment-event rows.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_event_cardinality_summary",
            "description": "Return the distribution of total event-row-count per logical payment_id (any event_type) -- e.g. how many payment_ids have 1, 2, or 3+ event rows.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_duplicate_payment_id_counts",
            "description": "Return facts about payment_ids with more than one event row of a given event_type (default SETTLED): how many, the extra row count, the total amount contributed by the extra rows, and example payment_ids.",
            "parameters": {
                "type": "object",
                "properties": {
                    "event_type": {"type": "string", "description": "An event_type observed in the data. Defaults to SETTLED."}
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_event_samples",
            "description": "Return every event row for one payment_id, sorted by event_timestamp -- e.g. to inspect a payment_id flagged as having duplicate events.",
            "parameters": {
                "type": "object",
                "properties": {
                    "payment_id": {"type": "string", "description": "A payment_id observed in the payment_events data."}
                },
                "required": ["payment_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_datasets",
            "description": "Return the dataset aliases available for this incident (e.g. 'loans', 'payments').",
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
            "description": "Return a generic group-by aggregation over one dataset -- e.g. count and sum(amount_paid) per payment_status.",
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
                        "description": "e.g. [{\"agg\": \"count\"}, {\"column\": \"amount_paid\", \"agg\": \"sum\"}]",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional {column: value} or {column: {\"in\": [...]}} filters applied before aggregating.",
                    },
                },
                "required": ["dataset", "group_by", "metrics"],
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
                        "description": "Optional {column: value} or {column: {\"in\": [...]}} filters.",
                    },
                    "columns": {"type": "array", "items": {"type": "string"}, "description": "Optional column subset to return."},
                    "limit": {"type": "integer", "description": f"1-{MAX_SAMPLE_LIMIT}, defaults to {DEFAULT_SAMPLE_LIMIT}."},
                },
                "required": ["dataset"],
            },
        },
    },
]
