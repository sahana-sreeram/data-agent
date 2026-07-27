"""Read-only, allowlisted investigation tools for diagnosing any of the 5 lifecycle
pipelines. Parallel to src/diagnostic_tools.py (left completely unmodified) for the
12-table lifecycle model instead of the original customers/loans/payments model.

All data (this pipeline's raw tables, validation results, business rules, metric
definitions, lineage) is loaded ONCE, at LifecycleDiagnosticTools construction, from S3 --
never by the model. Every tool method only slices this in-memory data; none of them touch
S3, run a command, or accept a path from the caller. The 7 general-purpose dataset tools
are reused, unmodified, from src/dataset_registry_tools.py rather than re-implemented here.

Which raw tables/ETL functions/lineage key apply to a given pipeline comes from
src/lifecycle_pipeline_registry.py -- this module has no pipeline-specific knowledge of its
own, so the same class serves all 5 pipelines.

Beyond the general-purpose/data-exploration tools, this module also exposes a directed,
metric-first path (get_failed_metric_context, get_metric_lineage,
get_pipeline_business_rules, get_relevant_etl_source, compare_metric_definition_to_etl,
trace_failed_check_to_code) -- see src/lifecycle_diagnosis_agent.py's SYSTEM_PROMPT for the
investigation order this is meant to support. compare_metric_definition_to_etl in
particular is a structural (not semantic) check: a metric's context/metrics/<pipeline>.json
entry can declare which business_rules key(s) its formula depends on
(business_rule_dependencies), and this tool checks whether the ETL source actually
contains a business_rules[...] lookup for each one -- a fast, deterministic way to catch
"the code silently stopped reading the approved business rule" bugs, which the general
dataset-exploration tools aren't shaped to find.

Tools return facts, never a diagnosis. Invalid arguments raise ToolError.
"""

from __future__ import annotations

import importlib
import inspect
import re
from dataclasses import dataclass, field

import pandas as pd

from src import dataset_registry_tools as registry_tools
from src.dataset_registry_tools import DATASET_REGISTRY_TOOL_NAMES, DATASET_REGISTRY_TOOL_SPECS, ToolError
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.storage import S3Storage

MAX_SAMPLE_LIMIT = registry_tools.MAX_SAMPLE_LIMIT
DEFAULT_SAMPLE_LIMIT = registry_tools.DEFAULT_SAMPLE_LIMIT


def _import_etl_module(etl_source_file: str):
    module_name = etl_source_file.replace("/", ".").removesuffix(".py")
    return importlib.import_module(module_name)


def _business_rules_lookup_present(source: str, dependency_path: str) -> bool:
    """Whether `source` contains a literal business_rules[...] lookup chain for a
    (possibly nested, dot-separated) dependency path, e.g. "interest_accrual.accrues_on_statuses"
    -> business_rules["interest_accrual"]["accrues_on_statuses"]. Recognizes both bracket
    access (business_rules["k"]) and .get() access (business_rules.get("k", ...)), in any
    mix for a nested path (e.g. business_rules.get("k1", {})["k2"]), either quote style, and
    arbitrary whitespace -- a repair patch is free to choose either style. Purely
    structural (a regex over the access chain), not a real AST/semantic check --
    deliberately simple and fast, matching what this codebase's ETL modules actually write."""
    keys = dependency_path.split(".")

    def _key_access(key: str) -> str:
        escaped = re.escape(key)
        return rf'(?:\[\s*[\'"]{escaped}[\'"]\s*\]|\.get\(\s*[\'"]{escaped}[\'"])'

    pattern = r"business_rules\s*" + r"\s*".join(_key_access(k) for k in keys)
    return re.search(pattern, source) is not None


@dataclass
class LifecycleDiagnosticTools:
    raw_tables: dict = field(default_factory=dict)  # table name -> pd.DataFrame
    validation_results: dict = field(default_factory=dict)
    business_rules: dict = field(default_factory=dict)
    metrics: dict = field(default_factory=dict)  # context/metrics/<pipeline>.json content
    etl_source_file: str = ""
    etl_functions: dict = field(default_factory=dict)  # function name -> callable
    lineage: dict = field(default_factory=dict)  # full context/lineage.json content
    lineage_key: str = ""  # e.g. "curated.loan_portfolio"

    def _dataset_registry(self) -> dict[str, pd.DataFrame]:
        return self.raw_tables

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

    def join_datasets(
        self, left_dataset: str, right_dataset: str, join_keys: list, left_filters: dict = None, right_filters: dict = None
    ) -> dict:
        return registry_tools.join_datasets(
            self._dataset_registry(), left_dataset, right_dataset, join_keys, left_filters, right_filters
        )

    def sample_dataset(self, dataset: str, filters: dict = None, columns: list = None, limit: int = DEFAULT_SAMPLE_LIMIT) -> dict:
        return registry_tools.sample_dataset(self._dataset_registry(), dataset, filters, columns, limit)

    def get_validation_results(self) -> dict:
        """The full validation_results content for this pipeline."""
        return self.validation_results

    def get_failed_checks(self) -> dict:
        """Just the FAIL entries from validation_results.checks."""
        failed = [c for c in self.validation_results.get("checks", []) if c.get("status") == "FAIL"]
        return {"failed_checks": failed}

    def get_business_rules(self) -> dict:
        """context/business_rules.json content, verbatim."""
        return self.business_rules

    def get_pipeline_business_rules(self) -> dict:
        """Alias for get_business_rules() -- same content, the name step 4 of the directed
        investigation flow uses."""
        return self.get_business_rules()

    def get_metric_definition(self, metric_name: str) -> dict:
        """The context/metrics/<pipeline>.json entry for a curated field."""
        metrics = self._require_known_metric(metric_name)
        return {metric_name: metrics[metric_name]}

    def get_metric_lineage(self, metric_name: str) -> dict:
        """The lineage entry (producer, dependencies) for this pipeline's curated dataset."""
        self._require_known_metric(metric_name)
        entry = self.lineage.get("datasets", {}).get(self.lineage_key)
        if entry is None:
            raise ToolError(f"lineage entry for {self.lineage_key!r} not found")
        return {"metric_name": metric_name, "lineage": entry}

    def get_failed_metric_context(self, metric_name: str) -> dict:
        """Bundle one metric's own definition, lineage, and whether any currently-failed
        check appears to reference it -- the common first stop for a per-metric failure,
        in one call instead of three."""
        metrics = self._require_known_metric(metric_name)
        entry = metrics[metric_name]
        lineage_entry = self.lineage.get("datasets", {}).get(self.lineage_key)
        failed_checks = [c for c in self.validation_results.get("checks", []) if c.get("status") == "FAIL"]
        mentioning = [
            c["id"] for c in failed_checks
            if c["id"].startswith(metric_name) or metric_name in (c.get("details") or "")
        ]
        return {
            "metric": metric_name,
            "definition": entry,
            "lineage": lineage_entry,
            "failed_checks_mentioning_this_metric": mentioning,
        }

    def get_relevant_etl_source(self) -> dict:
        """The source of every ETL function that computes this pipeline's curated output(s)."""
        return {
            "file": self.etl_source_file,
            "functions": {name: inspect.getsource(fn) for name, fn in self.etl_functions.items()},
        }

    def compare_metric_definition_to_etl(self, metric_name: str) -> dict:
        """Structurally compare a metric's declared business-rule dependencies against
        whether the ETL source actually reads each one. mismatch=true means the metric's
        formula is supposed to depend on a business_rules key that the code never looks up
        -- a strong, fast signal of a business-rule-vs-code mismatch, without needing to
        profile or join any raw data. mismatch=false (including when there are no declared
        dependencies at all) means this metric's business-rule wiring is fine and the bug,
        if any, is elsewhere (e.g. a join/aggregation bug -- use compare_dataset_keys /
        aggregate_dataset instead)."""
        metrics = self._require_known_metric(metric_name)
        entry = metrics[metric_name]
        dependencies = entry.get("business_rule_dependencies", [])
        combined_source = "\n".join(inspect.getsource(fn) for fn in self.etl_functions.values())
        dependency_present_in_source = {
            dependency: _business_rules_lookup_present(combined_source, dependency) for dependency in dependencies
        }
        return {
            "metric": metric_name,
            "expected_formula": entry.get("formula"),
            "business_rule_dependencies": dependencies,
            "file": self.etl_source_file,
            "dependency_present_in_source": dependency_present_in_source,
            "mismatch": bool(dependencies) and not all(dependency_present_in_source.values()),
        }

    def trace_failed_check_to_code(self, check_id: str) -> dict:
        """Given a failed check's id, return the check's full record, the candidate
        metric(s) it covers, and the ETL source responsible. For a per-metric check (e.g.
        "total_outstanding_principal_reconciliation") the candidate is exact. For a
        pipeline-wide aggregate check (e.g. "delinquency_default_breakdown_rows_match"),
        every metric in this pipeline is returned as a candidate, since the validator
        reports row-level mismatches, not which column(s) differed -- narrow further with
        compare_metric_definition_to_etl on each candidate."""
        checks_by_id = {c["id"]: c for c in self.validation_results.get("checks", [])}
        if check_id not in checks_by_id:
            raise ToolError(f"unknown check_id {check_id!r}; known checks: {sorted(checks_by_id)}")
        known_metrics = self.metrics.get("metrics", {})
        stripped = check_id.removesuffix("_reconciliation")
        if stripped != check_id and stripped in known_metrics:
            candidate_metrics = [stripped]
            candidate_precision = "exact"
        else:
            candidate_metrics = sorted(known_metrics)
            candidate_precision = "coarse -- this check covers multiple metrics at once"
        return {
            "check": checks_by_id[check_id],
            "candidate_metrics": candidate_metrics,
            "candidate_precision": candidate_precision,
            **self.get_relevant_etl_source(),
        }


def build_diagnostic_tools_for_pipeline(
    pipeline_name: str, storage: S3Storage, validation_results: dict, business_rules: dict
) -> LifecycleDiagnosticTools:
    """Load exactly the raw tables and ETL functions this pipeline needs, per
    PIPELINE_REGISTRY, and construct a LifecycleDiagnosticTools around them."""
    spec = PIPELINE_REGISTRY[pipeline_name]
    raw_tables = {table: storage.read_parquet(f"raw/{table}.parquet") for table in spec.raw_tables}
    etl_module = _import_etl_module(spec.etl_source_file)
    etl_functions = {name: getattr(etl_module, name) for name in spec.etl_function_names}
    metrics = storage.read_json(spec.metrics_key)
    lineage = storage.read_json("context/lineage.json")
    return LifecycleDiagnosticTools(
        raw_tables=raw_tables,
        validation_results=validation_results,
        business_rules=business_rules,
        metrics=metrics,
        etl_source_file=spec.etl_source_file,
        etl_functions=etl_functions,
        lineage=lineage,
        lineage_key=spec.lineage_key,
    )


ALLOWLISTED_TOOL_NAMES = frozenset(
    {
        "get_validation_results",
        "get_failed_checks",
        "get_business_rules",
        "get_pipeline_business_rules",
        "get_metric_definition",
        "get_metric_lineage",
        "get_failed_metric_context",
        "get_relevant_etl_source",
        "compare_metric_definition_to_etl",
        "trace_failed_check_to_code",
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
            "description": "Return the full validation results for this pipeline (all checks, not just failures).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_failed_checks",
            "description": "Return only the failing entries from this pipeline's validation checks. Start here.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trace_failed_check_to_code",
            "description": "Given a failed check's id (from get_failed_checks), return the check's full record, the candidate metric(s) it covers, and the ETL source responsible. Use this right after get_failed_checks to jump straight to the relevant metric(s)/code.",
            "parameters": {
                "type": "object",
                "properties": {"check_id": {"type": "string", "description": "A check id from get_failed_checks."}},
                "required": ["check_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_failed_metric_context",
            "description": "Return one metric's own definition, lineage, and any failed checks that appear to reference it, bundled in one call.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from this pipeline's curated output."}},
                "required": ["metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_definition",
            "description": "Return this pipeline's context/metrics/<pipeline>.json definition (formula, source_tables, business_rule_dependencies, caveats) of a named curated field.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from this pipeline's curated output."}},
                "required": ["metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_lineage",
            "description": "Return the lineage entry (producer, dependencies) for this pipeline's curated dataset.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from this pipeline's curated output."}},
                "required": ["metric_name"],
            },
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
            "name": "get_pipeline_business_rules",
            "description": "Alias for get_business_rules() -- return the approved business rules governing this pipeline's calculations.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_relevant_etl_source",
            "description": "Return the source of every ETL function that computes this pipeline's curated output(s).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_metric_definition_to_etl",
            "description": "Structurally compare a metric's declared business-rule dependencies against whether the ETL source actually reads each one. mismatch=true is a strong, fast signal of a business-rule-vs-code mismatch (the code silently stopped reading an approved business rule) -- check this BEFORE profiling or joining raw data. mismatch=false means look elsewhere (e.g. a join/aggregation bug) using the general-purpose dataset tools instead.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A field name from this pipeline's curated output."}},
                "required": ["metric_name"],
            },
        },
    },
    *DATASET_REGISTRY_TOOL_SPECS,
]
