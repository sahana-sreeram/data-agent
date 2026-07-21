"""Read-only, allowlisted planning tools for the repair agent.

Mirrors diagnostic_tools.py's design exactly: all data is loaded ONCE at
RepairTools construction from fixed paths chosen by the CLI -- never by the
model. Every tool method only slices this in-memory data or introspects a
bounded, named function's source; none of them write a file, run a command,
or accept a path from the caller. Business-rules files and hashable targets
are addressed by fixed ALIAS, never by a model-supplied filesystem path.

The repair agent never receives a write-capable tool. Its only output is a
structured plan (via the submit_repair_plan tool in repair_agent.py) --
applying that plan is done entirely by deterministic code in apply_repair.py,
AFTER the plan passes policy validation.
"""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass
from pathlib import Path

from src.diagnostic_tools import ETL_FUNCTIONS_BY_NAME


class ToolError(Exception):
    """Raised for invalid tool arguments. Caught by dispatch_tool; never crashes the agent loop."""


@dataclass
class RepairTools:
    diagnosis: dict
    validation_results: dict
    business_rules_by_alias: dict  # alias -> parsed JSON content
    lineage: dict
    pipeline_configuration: dict | None
    allowed_repair_targets: dict  # target_file -> descriptor, from context/repair_targets.json
    test_inventory: list
    etl_function_name: str
    file_hash_paths: dict  # alias -> real repo-relative path (for get_file_hash only)

    def get_diagnosis(self) -> dict:
        """The full diagnosis.json content this repair is addressing."""
        return self.diagnosis

    def get_failed_checks(self) -> dict:
        """Just the FAIL entries from validation_results.checks."""
        failed = [c for c in self.validation_results.get("checks", []) if c.get("status") == "FAIL"]
        return {"failed_checks": failed}

    def get_business_rules(self, alias: str) -> dict:
        """The content of a business-rules file, addressed by a fixed alias (never a raw path)."""
        if alias not in self.business_rules_by_alias:
            raise ToolError(f"unknown business_rules alias {alias!r}; known aliases: {sorted(self.business_rules_by_alias)}")
        return {"alias": alias, "content": self.business_rules_by_alias[alias]}

    def get_lineage(self, metric: str) -> dict:
        """The lineage entry (producer, dependencies) for the dataset containing a named metric."""
        entry = self.lineage.get("datasets", {}).get("processed.portfolio_summary")
        if entry is None:
            raise ToolError("lineage entry for 'processed.portfolio_summary' not found")
        return {"metric": metric, "lineage": entry}

    def get_pipeline_configuration(self) -> dict:
        """The scenario's pipeline_config.json content, if this incident has one."""
        if self.pipeline_configuration is None:
            return {"available": False}
        return {"available": True, **self.pipeline_configuration}

    def get_relevant_etl_source(self, metric_name: str) -> dict:
        """The bounded source of the ETL function relevant to this incident."""
        if self.etl_function_name not in ETL_FUNCTIONS_BY_NAME:
            raise ToolError(f"unknown etl_function_name {self.etl_function_name!r}")
        etl_function = ETL_FUNCTIONS_BY_NAME[self.etl_function_name]
        source = inspect.getsource(etl_function)
        return {
            "metric_name": metric_name,
            "file": "src/transform.py",
            "function": self.etl_function_name,
            "source": source,
        }

    def get_allowed_repair_targets(self) -> dict:
        """The full, fixed registry of files/settings this repair is allowed to target."""
        return {"targets": self.allowed_repair_targets}

    def get_test_inventory(self) -> dict:
        """The test files that verification will run for this incident's area of the codebase."""
        return {"tests": list(self.test_inventory)}

    def get_file_hash(self, target_alias: str) -> dict:
        """sha256 of a fixed, allowlisted file's CURRENT content, addressed by alias."""
        if target_alias not in self.file_hash_paths:
            raise ToolError(f"unknown target_alias {target_alias!r}; known aliases: {sorted(self.file_hash_paths)}")
        path = Path(self.file_hash_paths[target_alias])
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


def dispatch_tool(tools: RepairTools, name: str, arguments: dict) -> dict:
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
            "description": "Return the full diagnosis.json content this repair is addressing.",
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
            "description": "Return the content of a business-rules file by a fixed alias (e.g. 'CURRENT', 'STALE', 'ADOPTED' -- call get_diagnosis or get_pipeline_configuration first if unsure which aliases apply).",
            "parameters": {
                "type": "object",
                "properties": {"alias": {"type": "string", "description": "A known business-rules alias."}},
                "required": ["alias"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_lineage",
            "description": "Return the lineage entry (producer, dependencies) for the dataset containing a named metric.",
            "parameters": {
                "type": "object",
                "properties": {"metric": {"type": "string", "description": "A portfolio_summary metric name."}},
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pipeline_configuration",
            "description": "Return this scenario's pipeline_config.json content, if it has one (e.g. which business-rules file the ETL was told to use).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_relevant_etl_source",
            "description": "Return the bounded source of the ETL function relevant to this incident.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string", "description": "A portfolio_summary metric name."}},
                "required": ["metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_allowed_repair_targets",
            "description": "Return the full, fixed registry of files/settings this repair is allowed to target -- you may ONLY propose a target_file that appears here.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_test_inventory",
            "description": "Return the test files relevant to verifying a repair in this area of the codebase.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_file_hash",
            "description": "Return the sha256 hash of a fixed, allowlisted file's current content, by alias.",
            "parameters": {
                "type": "object",
                "properties": {"target_alias": {"type": "string", "description": "A known file alias."}},
                "required": ["target_alias"],
            },
        },
    },
]
