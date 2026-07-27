"""Read-only, allowlisted planning tools for repairing any of the 5 lifecycle pipelines.
Parallel to src/repair_tools.py (left completely unmodified) for the S3-backed lifecycle
model, which has no pipeline_configuration concept -- so get_pipeline_configuration always
reports unavailable. Which lineage key / ETL source file / function(s) apply to a given
pipeline are instance fields (from src/lifecycle_pipeline_registry.py), not hardcoded, so
the same class serves all 5 pipelines.

The repair agent never receives a write-capable tool; applying a plan is done entirely by
deterministic code in src/lifecycle_apply_repair.py, after the plan passes policy
validation (src/repair_models.py, reused unmodified).
"""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass, field
from pathlib import Path


class ToolError(Exception):
    """Raised for invalid tool arguments. Caught by dispatch_tool; never crashes the agent loop."""


@dataclass
class LifecycleRepairTools:
    diagnosis: dict
    validation_results: dict
    business_rules: dict  # context/business_rules.json content, verbatim
    lineage: dict  # full context/lineage.json content
    metrics: dict  # context/metrics/<pipeline>.json content
    allowed_repair_targets: dict  # target_file -> descriptor, from context/repair_targets.json
    test_inventory: list
    lineage_key: str = ""  # e.g. "curated.loan_portfolio"
    etl_source_file: str = ""
    etl_functions: dict = field(default_factory=dict)  # function name -> callable

    def get_diagnosis(self) -> dict:
        """The full diagnosis this repair is addressing."""
        return self.diagnosis

    def get_failed_checks(self) -> dict:
        """Just the FAIL entries from validation_results.checks."""
        failed = [c for c in self.validation_results.get("checks", []) if c.get("status") == "FAIL"]
        return {"failed_checks": failed}

    def get_business_rules(self, alias: str = "CURRENT") -> dict:
        """context/business_rules.json content. Only one rules file exists for any
        pipeline, so alias is accepted for tool-surface parity but always resolves to it."""
        return {"alias": alias, "content": self.business_rules}

    def get_lineage(self, metric: str) -> dict:
        """The lineage entry (producer, dependencies) for this pipeline's curated dataset."""
        entry = self.lineage.get("datasets", {}).get(self.lineage_key)
        if entry is None:
            raise ToolError(f"lineage entry for {self.lineage_key!r} not found")
        return {"metric": metric, "lineage": entry}

    def get_pipeline_configuration(self) -> dict:
        """No configuration file exists for any lifecycle pipeline -- the ETL source is the
        only place metric formulas live."""
        return {"available": False}

    def get_relevant_etl_source(self) -> dict:
        """The source of every ETL function that computes this pipeline's curated output(s)."""
        return {
            "file": self.etl_source_file,
            "functions": {name: inspect.getsource(fn) for name, fn in self.etl_functions.items()},
        }

    def get_allowed_repair_targets(self) -> dict:
        """The full, fixed registry of files this repair is allowed to target."""
        return {"targets": self.allowed_repair_targets}

    def get_test_inventory(self) -> dict:
        """The test files that verification will run for this repair."""
        return {"tests": list(self.test_inventory)}

    def get_file_hash(self, target_alias: str) -> dict:
        """sha256 of the ETL source file's CURRENT content. Only one alias exists here."""
        if target_alias != "ETL_SOURCE":
            raise ToolError(f"unknown target_alias {target_alias!r}; known aliases: ['ETL_SOURCE']")
        path = Path(self.etl_source_file)
        if not path.exists():
            raise ToolError(f"file for alias {target_alias!r} does not exist: {path}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return {"target_alias": target_alias, "path": str(path), "sha256": digest}


ALLOWLISTED_TOOL_NAMES = frozenset(
    {
        "get_diagnosis",
        "get_failed_checks",
        "get_business_rules",
        "get_lineage",
        "get_pipeline_configuration",
        "get_relevant_etl_source",
        "get_allowed_repair_targets",
        "get_test_inventory",
        "get_file_hash",
    }
)


def dispatch_tool(tools: LifecycleRepairTools, name: str, arguments: dict) -> dict:
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


TOOL_SPECS: list = [
    {
        "type": "function",
        "function": {
            "name": "get_diagnosis",
            "description": "Return the full diagnosis this repair is addressing.",
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
            "name": "get_business_rules",
            "description": "Return context/business_rules.json content (only one rules file governs this pipeline).",
            "parameters": {
                "type": "object",
                "properties": {"alias": {"type": "string", "description": "Accepted for parity; always resolves to the current business rules."}},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_lineage",
            "description": "Return the lineage entry (producer, dependencies) for this pipeline's curated dataset.",
            "parameters": {
                "type": "object",
                "properties": {"metric": {"type": "string", "description": "A curated metric name from this pipeline."}},
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pipeline_configuration",
            "description": "Return this pipeline's configuration, if it has one (no lifecycle pipeline has one -- always reports unavailable).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_relevant_etl_source",
            "description": "Return the bounded source of every ETL function relevant to this pipeline's curated output(s).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_allowed_repair_targets",
            "description": "Return the full, fixed registry of files this repair is allowed to target -- you may ONLY propose a target_file that appears here.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_test_inventory",
            "description": "Return the test files relevant to verifying a repair to this pipeline.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_file_hash",
            "description": "Return the sha256 hash of the ETL source file's current content, by alias.",
            "parameters": {
                "type": "object",
                "properties": {"target_alias": {"type": "string", "description": "A known file alias (only 'ETL_SOURCE' exists)."}},
                "required": ["target_alias"],
            },
        },
    },
]
