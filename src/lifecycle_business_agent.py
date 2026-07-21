"""The lifecycle Q&A agent's tool-calling loop.

Structurally identical to src/business_agent.py (the model receives a natural-
language question plus read-only tools and must call submit_answer with its
final structured answer), wired to the 5-curated-table lifecycle tool surface
instead. The one real difference: every tool call's raw result is recorded in
tool_results_by_name, since grounding here has to search across possibly
multi-row tool results rather than a single fixed summary dict -- see
src/lifecycle_answer_models.py's docstring for why.
"""

from __future__ import annotations

import json

from src.lifecycle_answer_models import BusinessAnswer, parse_lifecycle_business_answer
from src.lifecycle_business_tools import TOOL_SPECS, LifecycleBusinessTools, dispatch_tool
from src.model_client import DiagnosisModelClient, ToolCall

SUBMIT_ANSWER_TOOL_NAME = "submit_answer"
DEFAULT_MAX_TURNS = 6

SYSTEM_PROMPT = """You are a read-only business Q&A agent for a lending company's full customer lifecycle: marketing campaigns, underwriting, the loan portfolio, payment performance, and delinquency/default risk.

You have 6 read-only data tools plus get_metric_definition and get_business_rules:
- get_loan_portfolio_summary: portfolio-wide loan/principal/interest facts.
- get_campaign_funnel: every campaign's funnel counts and rates, plus one organic (non-campaign) row. Use this to compare campaigns.
- get_underwriting_performance: decision counts/rates by risk_segment AND by model_version (check breakdown_type).
- get_underwriting_rejection_distribution: rejection counts by reason.
- get_payment_performance_summary: portfolio-wide payment collection facts.
- get_delinquency_default: delinquency/default/loss metrics overall and by risk_segment.

A question may need just one tool, or may need you to reason across facts from more than one tool call yourself -- there is no pre-built tool that joins across tables or ranks results for you (e.g. no "best campaign" tool). Call get_metric_definition to confirm you are interpreting a metric correctly before citing it -- do not guess what a field means from its name alone.

You must never invent, estimate, round, or paraphrase a number. Every numeric value you cite must be exactly what a tool returned. If the question cannot be answered from the available data, set answer_status to INSUFFICIENT_DATA and explain what's missing in caveats -- do not approximate an answer instead.

You are not a diagnosis or repair agent. If you are given this question, the underlying pipelines have already been validated before you were called; you only need to answer it.

When you are ready to conclude, and only then, call the submit_answer tool exactly once with your full structured answer. You are not finished until you call submit_answer."""


class LifecycleBusinessAgentError(Exception):
    """Raised when the agent fails to reach an answer (e.g. max turns exceeded)."""


def _submit_answer_tool_spec() -> dict:
    cited_metric_schema = {
        "type": "object",
        "properties": {
            "metric_name": {"type": "string", "description": "A field name from one of the curated tool results (or a rejection_reason value, for the rejection-distribution tool)."},
            "value": {"description": "The EXACT value a tool call returned for this field -- never rounded or rephrased."},
            "source_reference": {"type": "string", "description": "The tool you called to get this value (e.g. get_loan_portfolio_summary)."},
        },
        "required": ["metric_name", "value", "source_reference"],
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
) -> BusinessAnswer:
    """Run the tool-calling loop until the model submits an answer or max_turns is exhausted."""
    tool_specs = [*TOOL_SPECS, _submit_answer_tool_spec()]
    messages: list = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps({"question": question})},
    ]
    called_tool_names: set = set()
    tool_results_by_name: dict = {}

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
            result = dispatch_tool(tools, call.name, call.arguments)
            tool_results_by_name.setdefault(call.name, []).append(result)
            messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)})

        if submission is not None:
            return parse_lifecycle_business_answer(
                submission.arguments,
                called_tool_names=called_tool_names,
                known_metric_names=known_metric_names,
                tool_results_by_name=tool_results_by_name,
            )

    raise LifecycleBusinessAgentError(f"agent did not reach an answer within {max_turns} turns")
