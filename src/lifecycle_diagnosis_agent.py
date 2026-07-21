"""The lifecycle diagnosis agent's tool-calling reasoning loop. Parallel to
src/diagnosis_agent.py (left completely unmodified) for the loan_portfolio pipeline's tool
surface. The loop shape, SYSTEM_PROMPT, and submit_diagnosis tool schema are all generic --
imported directly from src.diagnosis_agent rather than duplicated -- only the tool surface
(src.lifecycle_diagnostic_tools) differs.
"""

from __future__ import annotations

import json

from src.diagnosis_agent import (
    DEFAULT_MAX_TURNS,
    SUBMIT_DIAGNOSIS_TOOL_NAME,
    SYSTEM_PROMPT,
    DiagnosisAgentError,
    _serialize_tool_call,
    _submit_diagnosis_tool_spec,
)
from src.diagnosis_models import DiagnosisResult, parse_diagnosis_result
from src.lifecycle_diagnostic_tools import TOOL_SPECS, LifecycleDiagnosticTools, dispatch_tool
from src.model_client import DiagnosisModelClient, ToolCall

__all__ = ["DiagnosisAgentError", "run_lifecycle_diagnosis"]


def run_lifecycle_diagnosis(
    starting_context: dict,
    tools: LifecycleDiagnosticTools,
    model_client: DiagnosisModelClient,
    *,
    known_metric_names: set[str],
    known_file_paths: set[str],
    validation_overall_status: str,
    max_turns: int = DEFAULT_MAX_TURNS,
) -> DiagnosisResult:
    """Run the tool-calling loop until the model submits a diagnosis or max_turns is exhausted."""
    tool_specs = [*TOOL_SPECS, _submit_diagnosis_tool_spec()]
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(starting_context)},
    ]
    called_tool_names: set[str] = set()

    for _ in range(max_turns):
        response = model_client.send(messages, tool_specs)
        messages.append(
            {"role": "assistant", "tool_calls": [_serialize_tool_call(call) for call in response.tool_calls]}
        )

        submission: ToolCall | None = None
        for call in response.tool_calls:
            if call.name == SUBMIT_DIAGNOSIS_TOOL_NAME:
                submission = call
                messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps({"received": True})})
                continue
            called_tool_names.add(call.name)
            result = dispatch_tool(tools, call.name, call.arguments)
            messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)})

        if submission is not None:
            return parse_diagnosis_result(
                submission.arguments,
                validation_overall_status=validation_overall_status,
                called_tool_names=called_tool_names,
                known_metric_names=known_metric_names,
                known_file_paths=known_file_paths,
            )

    raise DiagnosisAgentError(f"agent did not reach a diagnosis within {max_turns} turns")
