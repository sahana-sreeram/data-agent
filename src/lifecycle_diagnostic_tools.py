"""Read-only, allowlisted investigation tools for diagnosing the loan_portfolio lifecycle
pipeline. Parallel to src/diagnostic_tools.py (left completely unmodified) for the 12-table
lifecycle model instead of the original customers/loans/payments model.

All data (raw loans/payment_events, validation results, business rules, metric definitions)
is loaded ONCE, at LifecycleDiagnosticTools construction, from S3 -- never by the model.
Every tool method only slices this in-memory data; none of them touch S3, run a command, or
accept a path from the caller. The 7 general-purpose dataset tools are reused, unmodified,
from src/dataset_registry_tools.py rather than re-implemented here.

Tools return facts, never a diagnosis. Invalid arguments raise ToolError.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass

import pandas as pd

from src import dataset_registry_tools as registry_tools
from src.dataset_registry_tools import DATASET_REGISTRY_TOOL_NAMES, DATASET_REGISTRY_TOOL_SPECS, ToolError
from src.etl_spark_loan_portfolio import compute_loan_portfolio

MAX_SAMPLE_LIMIT = registry_tools.MAX_SAMPLE_LIMIT
DEFAULT_SAMPLE_LIMIT = registry_tools.DEFAULT_SAMPLE_LIMIT


@dataclass
class LifecycleDiagnosticTools:
    loans: pd.DataFrame
    payment_events: pd.DataFrame
    validation_results: dict
    business_rules: dict
    metrics: dict  # context/metrics/loan_portfolio.json content

    def _dataset_registry(self) -> dict[str, pd.DataFrame]:
        return {"loans": self.loans, "payment_events": self.payment_events}

    def _require_known_metric(self, metric_name: str) -> dict:
        metrics = self.metrics.get("metrics", {})
        if metric_name not in metrics:
            raise ToolError(f"unknown metric_name {metric_name!r}; known metrics: {sorted(metrics)}")
        return metrics

    def list_datasets(self) -> dict:
        return registry_tools.list_datasets(self._dataset_registry())

    def get_dataset_schema(self, dataset: str) -> dict:
        return registry_tools.get_dataset_schema(self._dataset_registry(), dataset)

    def profile_dataset(self, dataset: str) -> dict:
        return registry_tools.profile_dataset(self._dataset_registry(), dataset)

    def analyze_key_cardinality(self, dataset: str, key_columns: list) -> dict:
        return registry_tools.analyze_key_cardinality(self._dataset_registry(), dataset, key_columns)

    def compare_dataset_keys(self, left_dataset: str, right_dataset: str, join_keys: list) -> dict:
        return registry_tools.compare_dataset_keys(self._dataset_registry(), left_dataset, right_dataset, join_keys)

    def aggregate_dataset(self, dataset: str, group_by: list, metrics: list, filters: dict = None) -> dict:
        return registry_tools.aggregate_dataset(self._dataset_registry(), dataset, group_by, metrics, filters)

    def sample_dataset(self, dataset: str, filters: dict = None, columns: list = None, limit: int = DEFAULT_SAMPLE_LIMIT) -> dict:
        return registry_tools.sample_dataset(self._dataset_registry(), dataset, filters, columns, limit)

    def get_validation_results(self) -> dict:
        """The full loan_portfolio validation_results content."""
        return self.validation_results

    def get_failed_checks(self) -> dict:
        """Just the FAIL entries from validation_results.checks."""
        failed = [c for c in self.validation_results.get("checks", []) if c.get("status") == "FAIL"]
        return {"failed_checks": failed}

    def get_business_rules(self) -> dict:
        """context/business_rules.json content, verbatim."""
        return self.business_rules

    def get_metric_definition(self, metric_name: str) -> dict:
        """The context/metrics/loan_portfolio.json entry for a curated field."""
        metrics = self._require_known_metric(metric_name)
        return {metric_name: metrics[metric_name]}

    def get_relevant_etl_source(self) -> dict:
        """The source of the ETL function that computes the loan_portfolio curated summary."""
        return {
            "file": "src/etl_spark_loan_portfolio.py",
            "function": "compute_loan_portfolio",
            "source": inspect.getsource(compute_loan_portfolio),
        }


ALLOWLISTED_TOOL_NAMES = frozenset(
    {
        "get_validation_results",
        "get_failed_checks",
        "get_business_rules",
        "get_metric_definition",
        "get_relevant_etl_source",
        *DATASET_REGISTRY_TOOL_NAMES,
    }
)


def dispatch_tool(tools: LifecycleDiagnosticTools, name: str, arguments: dict) -> dict:
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
            "description": "Return the full loan_portfolio validation results (all checks, not just failures).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_failed_checks",
            "description": "Return only the failing entries from the loan_portfolio validation checks.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_business_rules",
            "description": "Return context/business_rules.json: which payment statuses are valid/successful, interest accrual rules, etc.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_definition",
            "description": "Return the context/metrics/loan_portfolio.json definition (formula, source_tables, caveats) of a named curated field.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from the loan_portfolio curated summary."}},
                "required": ["metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_relevant_etl_source",
            "description": "Return the source of compute_loan_portfolio, the ETL function that computes every loan_portfolio curated metric.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    *DATASET_REGISTRY_TOOL_SPECS,
]
