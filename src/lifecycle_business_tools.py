"""Read-only tools for the lifecycle Q&A agent, over the 5 curated S3 outputs.

Mirrors src/business_tools.py's philosophy exactly (all data loaded ONCE at
construction from fixed sources chosen by the CLI, never by the model; every
tool returns a fact, never an interpretation) but is a separate, parallel
module -- see src/ask_lifecycle.py's module docstring for why this isn't
built into business_tools.py directly.

Tools that return curated rows return the FULL (small) table rather than a
single filtered row, so the model can compare across campaigns/segments
itself -- there is no baked-in "which campaign is best" tool; that's the
model's job from the facts returned here.

Every returned dict/list is passed through _clean_for_json() first: pandas
represents a null in a numeric/object column as NaN, and json.dumps on a
float('nan') emits a non-standard `NaN` token AND breaks equality-based
grounding checks (NaN != NaN) the same way it broke
src/validate_campaign_funnel.py's dict-key lookup during this build -- so
every NaN is converted to a real None before a tool result ever reaches the
model or the grounding check.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


class ToolError(Exception):
    """Raised for invalid tool arguments. Caught by dispatch_tool; never crashes the agent loop."""


def _clean_value(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _clean_for_json(value):
    """Recursively replace NaN with None in a dict, list of dicts, or scalar."""
    if isinstance(value, dict):
        return {k: _clean_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_for_json(v) for v in value]
    return _clean_value(value)


@dataclass
class LifecycleBusinessTools:
    loan_portfolio: dict
    campaign_funnel: list
    underwriting_performance: list
    underwriting_rejections: dict
    payment_performance: dict
    delinquency_default: list
    business_rules: dict
    metrics_by_pipeline: dict = field(default_factory=dict)

    def get_loan_portfolio_summary(self) -> dict:
        """The trusted, curated loan_portfolio summary: funded/outstanding principal, loan counts by status, accrued interest."""
        return _clean_for_json(self.loan_portfolio)

    def get_campaign_funnel(self) -> dict:
        """Every campaign's funnel counts/rates (emails sent through loans funded), plus one campaign_id=null 'organic' row for non-campaign activity."""
        return {"rows": _clean_for_json(self.campaign_funnel)}

    def get_underwriting_performance(self) -> dict:
        """Underwriting decision counts/rates broken out two ways: by risk_segment and by model_version. Check breakdown_type to tell them apart."""
        return {"rows": _clean_for_json(self.underwriting_performance)}

    def get_underwriting_rejection_distribution(self) -> dict:
        """Count of REJECTED underwriting decisions by rejection_reason, portfolio-wide."""
        return _clean_for_json(self.underwriting_rejections)

    def get_payment_performance_summary(self) -> dict:
        """The trusted, curated payment_performance summary: expected vs. collected amounts, missed/late/failed counts, collection/prepayment rates."""
        return _clean_for_json(self.payment_performance)

    def get_delinquency_default(self) -> dict:
        """Delinquency/default/loss metrics: one overall row (breakdown_value='ALL') plus one row per risk_segment."""
        return {"rows": _clean_for_json(self.delinquency_default)}

    def get_metric_definition(self, pipeline: str, metric_name: str) -> dict:
        """The business definition/formula/caveats for a named metric from a named pipeline (loan_portfolio, campaign_funnel, underwriting_performance, payment_performance, or delinquency_default)."""
        if pipeline not in self.metrics_by_pipeline:
            raise ToolError(f"unknown pipeline {pipeline!r}; known pipelines: {sorted(self.metrics_by_pipeline)}")
        metrics = self.metrics_by_pipeline[pipeline].get("metrics", {})
        if metric_name not in metrics:
            raise ToolError(f"unknown metric_name {metric_name!r} for pipeline {pipeline!r}; known metrics: {sorted(metrics)}")
        return {metric_name: metrics[metric_name]}

    def get_business_rules(self) -> dict:
        """The approved business rules (e.g. which payment statuses count as successful, prepayment threshold, interest accrual convention, loss rate denominator)."""
        return self.business_rules


ALLOWLISTED_TOOL_NAMES = frozenset(
    {
        "get_loan_portfolio_summary",
        "get_campaign_funnel",
        "get_underwriting_performance",
        "get_underwriting_rejection_distribution",
        "get_payment_performance_summary",
        "get_delinquency_default",
        "get_metric_definition",
        "get_business_rules",
    }
)


def dispatch_tool(tools: LifecycleBusinessTools, name: str, arguments: dict) -> dict:
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
            "name": "get_loan_portfolio_summary",
            "description": "Return the trusted, curated loan_portfolio summary: funded/outstanding principal, loan counts by status, average interest rate, accrued interest.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_campaign_funnel",
            "description": "Return every campaign's funnel counts and rates (emails sent through loans funded), plus one organic (non-campaign) row. Use this to compare campaigns against each other.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_underwriting_performance",
            "description": "Return underwriting decision counts/rates broken out by risk_segment AND by model_version (check the breakdown_type field to tell which rows are which).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_underwriting_rejection_distribution",
            "description": "Return the count of REJECTED underwriting decisions by rejection_reason, portfolio-wide.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_performance_summary",
            "description": "Return the trusted, curated payment_performance summary: expected vs. collected amounts, missed/late/failed counts, collection rate, prepayment rate.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_delinquency_default",
            "description": "Return delinquency/default/loss metrics: one overall row (breakdown_value='ALL') plus one row per risk_segment. Use this to compare risk tiers.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_definition",
            "description": "Return the business definition/formula/caveats for a named metric from a named pipeline, to confirm you're interpreting it correctly before citing it.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipeline": {
                        "type": "string",
                        "enum": ["loan_portfolio", "campaign_funnel", "underwriting_performance", "payment_performance", "delinquency_default"],
                    },
                    "metric_name": {"type": "string", "description": "A metric field name from that pipeline's curated output."},
                },
                "required": ["pipeline", "metric_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_business_rules",
            "description": "Return the approved business rules (e.g. prepayment threshold, interest accrual convention, loss rate denominator) for context on how metrics were computed.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]
