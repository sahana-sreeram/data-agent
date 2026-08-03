"""The lifecycle repair agent's tool-calling planning loop. Parallel to src/repair_agent.py
(left completely unmodified) for the loan_portfolio pipeline's tool surface. The loop shape,
SYSTEM_PROMPT, and submit_repair_plan tool schema are all generic -- imported directly from
src.repair_agent rather than duplicated -- only the tool surface (src.lifecycle_repair_tools)
differs.
"""

from __future__ import annotations

import json

from src.model_client import DiagnosisModelClient, ToolCall
from src.repair_agent import (
    DEFAULT_MAX_TURNS,
    SUBMIT_REPAIR_PLAN_TOOL_NAME,
    SYSTEM_PROMPT,
    RepairAgentError,
    _serialize_tool_call,
    _submit_repair_plan_tool_spec,
)
from src.repair_models import RepairPlan, parse_repair_plan
from src.lifecycle_repair_tools import TOOL_SPECS, LifecycleRepairTools, dispatch_tool

__all__ = ["RepairAgentError", "run_lifecycle_repair_planning"]


def run_lifecycle_repair_planning(
    starting_context: dict,
    tools: LifecycleRepairTools,
    model_client: DiagnosisModelClient,
    *,
    diagnosis: dict,
    allowed_targets: dict,
    max_turns: int = DEFAULT_MAX_TURNS,
) -> RepairPlan:
    """Run the tool-calling loop until the model submits a repair plan or max_turns is exhausted."""
    tool_specs = [*TOOL_SPECS, _submit_repair_plan_tool_spec()]
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(starting_context)},
    ]

    for _ in range(max_turns):
        response = model_client.send(messages, tool_specs)
        messages.append(
            {"role": "assistant", "tool_calls": [_serialize_tool_call(call) for call in response.tool_calls]}
        )

        submission: ToolCall | None = None
        for call in response.tool_calls:
            if call.name == SUBMIT_REPAIR_PLAN_TOOL_NAME:
                submission = call
                messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps({"received": True})})
                continue
            result = dispatch_tool(tools, call.name, call.arguments)
            messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)})

        if submission is not None:
            return parse_repair_plan(submission.arguments, diagnosis=diagnosis, allowed_targets=allowed_targets)

    raise RepairAgentError(f"agent did not reach a repair plan within {max_turns} turns")
