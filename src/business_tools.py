"""Read-only tools for the business Q&A agent.

Mirrors diagnostic_tools.py/repair_tools.py exactly: all data is loaded ONCE
at BusinessTools construction from fixed paths chosen by the CLI -- never by
the model. Every tool returns a fact (the trusted portfolio summary, a
metric's definition, the approved business rules) -- never an interpretation
or a computed answer. The agent must derive its answer from these facts;
nothing here decides what the answer is.
"""

from __future__ import annotations

from dataclasses import dataclass


class ToolError(Exception):
    """Raised for invalid tool arguments. Caught by dispatch_tool; never crashes the agent loop."""


@dataclass
class BusinessTools:
    portfolio_summary: dict
    business_rules: dict
    data_dictionary: dict

    def get_portfolio_summary(self) -> dict:
        """The full, trusted portfolio_summary.json content -- the only source of numeric facts."""
        return self.portfolio_summary

    def get_metric_definition(self, metric_name: str) -> dict:
        """The data-dictionary definition of a named portfolio_summary metric."""
        fields = self.data_dictionary.get("portfolio_summary", {}).get("fields", {})
        if metric_name not in fields:
            raise ToolError(f"unknown metric_name {metric_name!r}; known metrics: {sorted(fields)}")
        return {metric_name: fields[metric_name]}

    def get_business_rules(self) -> dict:
        """The approved business rules (e.g. which payment statuses count as successful)."""
        return self.business_rules


ALLOWLISTED_TOOL_NAMES = frozenset({"get_portfolio_summary", "get_metric_definition", "get_business_rules"})


def dispatch_tool(tools: BusinessTools, name: str, arguments: dict) -> dict:
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
            "name": "get_portfolio_summary",
            "description": "Return the full, trusted portfolio_summary.json content -- the only source of numeric facts about the loan portfolio.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_definition",
            "description": "Return the definition of a named portfolio_summary metric, to make sure you interpret it correctly before answering.",
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
            "name": "get_business_rules",
            "description": "Return the approved business rules (e.g. which payment statuses count as successful) for context on how metrics were computed.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]
