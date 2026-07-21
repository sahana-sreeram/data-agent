"""The business Q&A agent's tool-calling loop.

Mirrors diagnosis_agent.py/repair_agent.py exactly: the model receives a
natural-language business question plus a narrow set of read-only tools over
the trusted portfolio summary, and must call submit_answer with its final
structured answer. It is never told which metric answers the question --
it must map the question to a metric itself, using get_metric_definition to
confirm its interpretation. Every cited number is grounded against the real
tool output at parse time; the model cannot report a fabricated value.
"""

from __future__ import annotations

import json

from src.answer_models import BusinessAnswer, parse_business_answer
from src.business_tools import TOOL_SPECS, BusinessTools, dispatch_tool
from src.model_client import DiagnosisModelClient, ToolCall

SUBMIT_ANSWER_TOOL_NAME = "submit_answer"
DEFAULT_MAX_TURNS = 6

SYSTEM_PROMPT = """You are a read-only business Q&A agent for a lending portfolio.

You receive a natural-language business question. Use the available tools to look up the trusted, already-validated portfolio summary and answer the question from it. Call get_metric_definition to confirm you are interpreting a metric correctly before citing it -- do not guess what a field means from its name alone.

You must never invent, estimate, round, or paraphrase a number. Every numeric value you cite must be exactly what a tool returned. If the question cannot be answered from the available metrics, set answer_status to INSUFFICIENT_DATA and explain what's missing in caveats -- do not approximate an answer instead.

You are not a diagnosis or repair agent -- you do not investigate incidents or propose fixes. If you are given this question, the data has already been validated (or automatically repaired and re-verified) before you were called; you only need to answer it.

When you are ready to conclude, and only then, call the submit_answer tool exactly once with your full structured answer. You are not finished until you call submit_answer."""


class BusinessAgentError(Exception):
    """Raised when the agent fails to reach an answer (e.g. max turns exceeded)."""


def _submit_answer_tool_spec() -> dict:
    cited_metric_schema = {
        "type": "object",
        "properties": {
            "metric_name": {"type": "string", "description": "A field name from portfolio_summary.json."},
            "value": {"description": "The EXACT value get_portfolio_summary returned for this field -- never rounded or rephrased."},
            "source_reference": {"type": "string", "description": "The tool you called to get this value (e.g. get_portfolio_summary)."},
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


def run_business_qa(
    question: str,
    tools: BusinessTools,
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
            messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)})

        if submission is not None:
            return parse_business_answer(
                submission.arguments,
                called_tool_names=called_tool_names,
                known_metric_names=known_metric_names,
                portfolio_summary=tools.portfolio_summary,
            )

    raise BusinessAgentError(f"agent did not reach an answer within {max_turns} turns")
