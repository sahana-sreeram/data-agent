"""The lifecycle Q&A agent's tool-calling loop.

Structurally identical to src/business_agent.py (the model receives a natural-
language question plus read-only tools and must call submit_answer with its
final structured answer), wired to the 5-curated-table lifecycle tool surface
instead. The one real difference: every tool call's raw result is recorded in
tool_results_by_name, since grounding here has to search across possibly
multi-row tool results rather than a single fixed summary dict -- see
src/lifecycle_answer_models.py's docstring for why.

run_lifecycle_business_qa also reports every non-terminal tool call it dispatched
(LifecycleQAResult.called_tool_calls) -- src/ask_lifecycle.py uses this to determine which
pipeline(s) a question actually needed data from ("question lineage"), so a pipeline
failure unrelated to the question doesn't block or trigger a repair attempt.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from src.lifecycle_answer_models import BusinessAnswer, parse_lifecycle_business_answer
from src.lifecycle_business_tools import TOOL_SPECS, LifecycleBusinessTools, dispatch_tool
from src.model_client import DiagnosisModelClient, ToolCall

SUBMIT_ANSWER_TOOL_NAME = "submit_answer"
DEFAULT_MAX_TURNS = 8

SYSTEM_PROMPT = """You are a read-only business Q&A agent for a lending company's full customer lifecycle: marketing campaigns, coupon performance, underwriting, the loan portfolio, payment performance, and delinquency/default risk.

You have 7 read-only whole-table data tools plus get_metric_definition and get_business_rules:
- get_loan_portfolio_summary: portfolio-wide loan/principal/interest facts.
- get_campaign_funnel: every campaign's funnel counts and rates, plus one organic (non-campaign) row.
- get_coupon_performance: every coupon_code's redemption-funnel counts and redemption_rate, including codes never used.
- get_underwriting_performance: decision counts/rates by risk_segment AND by model_version (check breakdown_type).
- get_underwriting_rejection_distribution: rejection counts by reason.
- get_payment_performance_summary: portfolio-wide payment collection facts.
- get_delinquency_default: delinquency/default/loss metrics overall and by risk_segment.

You also have 3 bounded query tools over the multi-row curated tables (campaign_funnel, underwriting_performance, delinquency_default, coupon_performance -- loan_portfolio and payment_performance are already single-row summaries, so these don't apply to them):
- aggregate_curated_data(dataset, group_by, metrics, filters): group-by aggregation (count/sum/mean/nunique), e.g. total loans_funded per channel.
- sample_curated_data(dataset, filters, columns, limit): filtered row lookup, e.g. campaigns with open_rate above 0.5 (filters support equality, {"in": [...]}, and {"gt"|"gte"|"lt"|"lte"|"ne": value} comparisons).
- join_curated_data(left_dataset, right_dataset, join_keys, left_filters, right_filters): row-level join on a shared key -- in particular, underwriting_performance (filtered to breakdown_type="risk_segment") can be joined to delinquency_default on breakdown_value, to compare approval_rate against default_rate/loss_rate for the same segment in one result.

Prefer these bounded tools over asking for a whole table and reasoning by hand whenever a question is naturally a filter, a group-by aggregation, or a join across two of the three tables above -- they give you exact, pre-computed numbers instead of numbers you'd otherwise have to derive yourself. There is still no tool that ranks or judges results for you (e.g. no "best campaign" tool) -- picking the top row from an aggregation's results is your job. Call get_metric_definition to confirm you are interpreting a metric correctly before citing it -- do not guess what a field means from its name alone.

If the question itself asks what a metric MEANS or how it's computed (not for its current value), answer from get_metric_definition's result directly and still cite it: set cited_metrics[].metric_name to the metric's own name (e.g. "loss_rate"), never a nested field of the definition like "business_definition" or "formula", and set value to whatever you're citing from it (the definition text, the formula, etc.).

Some metrics carry a "_context" block from get_metric_definition with a non-empty "conflicts" list -- this means the human-approved definition and what the code actually computes disagree on something (e.g. which statuses count as a successful payment). Treat this as a caveat you must surface in your answer, not something to silently resolve one way or the other -- you are not the authority on which side is right.

You must never invent, estimate, round, or paraphrase a number. Every numeric value you cite must be exactly what a tool returned. When a tool result contains more than one row (get_campaign_funnel, get_underwriting_performance, get_delinquency_default, or any aggregate_curated_data/sample_curated_data/join_curated_data result with more than one row), every cited_metric citing it MUST include a row_identifier -- the field(s) that pinpoint exactly which row the value came from (e.g. {"campaign_id": "CMP0042"} or {"breakdown_type": "risk_segment", "breakdown_value": "PRIME"}) -- so you never attribute one row's number to a different row. Set row_identifier to null only when the source tool result has zero or one row. If the question cannot be answered from the available data, set answer_status to INSUFFICIENT_DATA and explain what's missing in caveats -- do not approximate an answer instead.

You are not a diagnosis or repair agent. If you are given this question, the underlying pipelines have already been validated before you were called; you only need to answer it.

When you are ready to conclude, and only then, call the submit_answer tool exactly once with your full structured answer. You are not finished until you call submit_answer."""


class LifecycleBusinessAgentError(Exception):
    """Raised when the agent fails to reach an answer (e.g. max turns exceeded)."""


@dataclass(frozen=True)
class LifecycleQAResult:
    answer: BusinessAnswer
    # Every non-terminal tool call dispatched this session, in order, as {"name":...,
    # "arguments":...} -- e.g. get_metric_definition's relevant pipeline is only knowable
    # from its "pipeline" argument, not the tool name alone.
    called_tool_calls: list = field(default_factory=list)


def _submit_answer_tool_spec() -> dict:
    cited_metric_schema = {
        "type": "object",
        "properties": {
            "metric_name": {"type": "string", "description": "A field name from one of the curated tool results (or a rejection_reason value, for the rejection-distribution tool)."},
            "value": {"description": "The EXACT value a tool call returned for this field -- never rounded or rephrased."},
            "source_reference": {"type": "string", "description": "The tool you called to get this value (e.g. get_loan_portfolio_summary)."},
            "row_identifier": {
                "type": ["object", "null"],
                "description": "For a multi-row tool result, the field(s) identifying which row this value came from (e.g. {'campaign_id': 'CMP0042'} or {'breakdown_type': 'risk_segment', 'breakdown_value': 'PRIME'}). Null only when the source tool result has zero or one row.",
            },
        },
        "required": ["metric_name", "value", "source_reference", "row_identifier"],
    }
    return {
        "type": "function",
        "function": {
            "name": SUBMIT_ANSWER_TOOL_NAME,
            "description": "Submit your final, grounded answer to the business question. Call this exactly once, when you have enough information.",
            "parameters": {
                "type": "object",
                "properties": {
                    "answer_status": {"type": "string", "enum": ["ANSWERED", "UNRELIABLE_DATA", "INSUFFICIENT_DATA"]},
                    "question": {"type": "string", "description": "Echo the original question."},
                    "answer_summary": {"type": "string", "description": "A short, direct, natural-language answer citing the exact number(s)."},
                    "as_of_date": {"type": ["string", "null"]},
                    "cited_metrics": {"type": "array", "items": cited_metric_schema},
                    "caveats": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["answer_status", "question", "answer_summary", "as_of_date", "cited_metrics", "caveats"],
            },
        },
    }


def _serialize_tool_call(call: ToolCall) -> dict:
    return {
        "id": call.id,
        "type": "function",
        "function": {"name": call.name, "arguments": json.dumps(call.arguments)},
    }


def run_lifecycle_business_qa(
    question: str,
    tools: LifecycleBusinessTools,
    model_client: DiagnosisModelClient,
    *,
    known_metric_names: set,
    max_turns: int = DEFAULT_MAX_TURNS,
) -> LifecycleQAResult:
    """Run the tool-calling loop until the model submits an answer or max_turns is exhausted."""
    tool_specs = [*TOOL_SPECS, _submit_answer_tool_spec()]
    messages: list = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps({"question": question})},
    ]
    called_tool_names: set = set()
    tool_results_by_name: dict = {}
    called_tool_calls: list = []

    for _ in range(max_turns):
        response = model_client.send(messages, tool_specs)
        messages.append(
            {"role": "assistant", "tool_calls": [_serialize_tool_call(call) for call in response.tool_calls]}
        )

        submission: ToolCall = None
        for call in response.tool_calls:
            if call.name == SUBMIT_ANSWER_TOOL_NAME:
                submission = call
                messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps({"received": True})})
                continue
            called_tool_names.add(call.name)
            called_tool_calls.append({"name": call.name, "arguments": call.arguments})
            result = dispatch_tool(tools, call.name, call.arguments)
            tool_results_by_name.setdefault(call.name, []).append(result)
            messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)})

        if submission is not None:
            answer = parse_lifecycle_business_answer(
                submission.arguments,
                called_tool_names=called_tool_names,
                known_metric_names=known_metric_names,
                tool_results_by_name=tool_results_by_name,
            )
            return LifecycleQAResult(answer=answer, called_tool_calls=called_tool_calls)

    raise LifecycleBusinessAgentError(f"agent did not reach an answer within {max_turns} turns")
