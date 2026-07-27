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

import pandas as pd

from src import dataset_registry_tools as registry_tools
from src.context_retriever import ContextRetriever
from src.dataset_registry_tools import DEFAULT_SAMPLE_LIMIT, ToolError
from src.storage import S3Storage

# Curated tables with more than one row -- the only ones that support bounded
# filter/group/aggregate/join queries. loan_portfolio and payment_performance are already
# single-row summaries, so query support would be meaningless for them.
QUERYABLE_DATASETS = ("campaign_funnel", "underwriting_performance", "delinquency_default", "coupon_performance")


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
    # Defaults to [] -- every existing construction site (and every existing test's fixture)
    # is unaffected; only src/ask_lifecycle.py's _load_tools passes real data.
    coupon_performance: list = field(default_factory=list)
    # Both None by default -- every existing construction site (and every existing test's
    # fixture) is unaffected. When set (see src/ask_lifecycle.py's _load_tools),
    # get_metric_definition routes through ContextRetriever instead of metrics_by_pipeline
    # directly, for any pipeline that has generated/human context populated (today: all 6) --
    # everything else still falls back to the exact legacy dict lookup.
    context_retriever: ContextRetriever | None = None
    storage: S3Storage | None = None

    def __post_init__(self) -> None:
        self._registry = {
            "campaign_funnel": pd.DataFrame(self.campaign_funnel),
            "underwriting_performance": pd.DataFrame(self.underwriting_performance),
            "delinquency_default": pd.DataFrame(self.delinquency_default),
            "coupon_performance": pd.DataFrame(self.coupon_performance),
        }

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

    def get_coupon_performance(self) -> dict:
        """Redemption-funnel counts (offers/applications/loans) and redemption_rate for every
        coupon_code ever defined -- one row per code, including codes with zero offers."""
        return {"rows": _clean_for_json(self.coupon_performance)}

    def get_metric_definition(self, pipeline: str, metric_name: str) -> dict:
        """The business definition/formula/caveats for a named metric from a named pipeline (loan_portfolio, campaign_funnel, underwriting_performance, payment_performance, or delinquency_default).

        When context_retriever/storage are set (see src/ask_lifecycle.py), the returned dict
        also carries a "_context" block -- provenance, review_status, confidence, and any
        unresolved conflict between the human-approved and code-observed definitions -- for
        any pipeline that has generated/human context populated. A pipeline without one yet
        gets the exact legacy dict, unchanged, either way."""
        if pipeline not in self.metrics_by_pipeline:
            raise ToolError(f"unknown pipeline {pipeline!r}; known pipelines: {sorted(self.metrics_by_pipeline)}")
        metrics = self.metrics_by_pipeline[pipeline].get("metrics", {})
        if metric_name not in metrics:
            raise ToolError(f"unknown metric_name {metric_name!r} for pipeline {pipeline!r}; known metrics: {sorted(metrics)}")

        if self.context_retriever is None or self.storage is None:
            return {metric_name: metrics[metric_name]}

        fact = self.context_retriever.get_metric(pipeline, metric_name, self.storage)
        if fact.provenance == "legacy_file":
            return {metric_name: metrics[metric_name]}
        return {
            metric_name: metrics[metric_name],
            "_context": {
                "provenance": fact.provenance,
                "review_status": fact.review_status.value if fact.review_status else None,
                "confidence": fact.confidence,
                "human_approved_definition": fact.value,
                "conflicts": [c.model_dump() for c in fact.conflicts],
            },
        }

    def get_business_rules(self) -> dict:
        """The approved business rules (e.g. which payment statuses count as successful, prepayment threshold, interest accrual convention, loss rate denominator)."""
        return self.business_rules

    def aggregate_curated_data(self, dataset: str, group_by: list, metrics: list, filters: dict = None) -> dict:
        """Bounded group-by aggregation over one of the multi-row curated tables (campaign_funnel, underwriting_performance, delinquency_default) -- e.g. sum(loans_funded) per channel."""
        return _clean_for_json(registry_tools.aggregate_dataset(self._registry, dataset, group_by, metrics, filters))

    def sample_curated_data(self, dataset: str, filters: dict = None, columns: list = None, limit: int = DEFAULT_SAMPLE_LIMIT) -> dict:
        """Bounded, filtered row sampling from one of the multi-row curated tables, with optional column selection -- e.g. campaigns with open_rate above 0.5."""
        return _clean_for_json(registry_tools.sample_dataset(self._registry, dataset, filters, columns, limit))

    def join_curated_data(
        self, left_dataset: str, right_dataset: str, join_keys: list, left_filters: dict = None, right_filters: dict = None
    ) -> dict:
        """Row-level join of two multi-row curated tables on shared key column(s), each side optionally pre-filtered -- e.g. joining underwriting_performance (filtered to breakdown_type='risk_segment') to delinquency_default on breakdown_value, to compare approval and default rates for the same segment."""
        return _clean_for_json(
            registry_tools.join_datasets(self._registry, left_dataset, right_dataset, join_keys, left_filters, right_filters)
        )


ALLOWLISTED_TOOL_NAMES = frozenset(
    {
        "get_loan_portfolio_summary",
        "get_campaign_funnel",
        "get_underwriting_performance",
        "get_underwriting_rejection_distribution",
        "get_payment_performance_summary",
        "get_delinquency_default",
        "get_coupon_performance",
        "get_metric_definition",
        "get_business_rules",
        "aggregate_curated_data",
        "sample_curated_data",
        "join_curated_data",
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
            "name": "get_coupon_performance",
            "description": "Return every coupon_code's redemption-funnel counts (offers created, applications submitted, loans funded) and redemption_rate, including codes that were defined but never used. coupon_rule_count/currently_valid_rule_count report that a code can be reused by more than one coupon_rule (different campaigns/time windows).",
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
                        "enum": ["loan_portfolio", "campaign_funnel", "underwriting_performance", "payment_performance", "delinquency_default", "coupon_performance"],
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
    {
        "type": "function",
        "function": {
            "name": "aggregate_curated_data",
            "description": "Return a bounded group-by aggregation (count/sum/mean/nunique) over one of the multi-row curated tables (campaign_funnel, underwriting_performance, delinquency_default) -- e.g. total loans_funded per channel, or average approval_rate per model_version.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "enum": list(QUERYABLE_DATASETS)},
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
                        "description": "e.g. [{\"agg\": \"sum\", \"column\": \"loans_funded\"}]",
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
            "name": "sample_curated_data",
            "description": "Return up to `limit` filtered rows from one of the multi-row curated tables (campaign_funnel, underwriting_performance, delinquency_default) -- e.g. campaigns with open_rate above 0.5.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "enum": list(QUERYABLE_DATASETS)},
                    "filters": {
                        "type": "object",
                        "description": "Optional {column: value}, {column: {\"in\": [...]}}, or {column: {\"gt\"|\"gte\"|\"lt\"|\"lte\"|\"ne\": value}} filters, e.g. {\"open_rate\": {\"gt\": 0.5}}.",
                    },
                    "columns": {"type": "array", "items": {"type": "string"}, "description": "Optional column subset to return."},
                    "limit": {"type": "integer", "description": "1-20, defaults to 5."},
                },
                "required": ["dataset"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "join_curated_data",
            "description": "Return the row-level join of two multi-row curated tables on a shared key column -- e.g. joining underwriting_performance (filtered to breakdown_type='risk_segment') to delinquency_default on breakdown_value, to compare approval_rate against default_rate/loss_rate for the same segment in one result.",
            "parameters": {
                "type": "object",
                "properties": {
                    "left_dataset": {"type": "string", "enum": list(QUERYABLE_DATASETS)},
                    "right_dataset": {"type": "string", "enum": list(QUERYABLE_DATASETS)},
                    "join_keys": {"type": "array", "items": {"type": "string"}, "description": "Column(s) present in both datasets to join on, e.g. [\"breakdown_value\"]."},
                    "left_filters": {
                        "type": "object",
                        "description": "Optional {column: value}, {column: {\"in\": [...]}}, or {column: {\"gt\"|\"gte\"|\"lt\"|\"lte\"|\"ne\": value}} filters applied to left_dataset before joining, e.g. {\"breakdown_type\": \"risk_segment\"}.",
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
]
