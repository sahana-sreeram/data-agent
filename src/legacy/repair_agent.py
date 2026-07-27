"""The repair agent's tool-calling planning loop.

Mirrors diagnosis_agent.py exactly: starts from the grounded diagnosis plus
a narrow set of read-only planning tools, lets the model choose what to
inspect, and terminates when it calls the special submit_repair_plan tool --
at which point the arguments are schema- and grounding-validated into a
RepairPlan. The model NEVER receives a write-capable tool; applying a plan
is done entirely by deterministic code in apply_repair.py, after the plan
passes policy validation (see src/repair_models.py).
"""

from __future__ import annotations

import json

from src.model_client import DiagnosisModelClient, ToolCall
from src.legacy.repair_models import RepairPlan, parse_repair_plan
from src.legacy.repair_tools import TOOL_SPECS, RepairTools, dispatch_tool

SUBMIT_REPAIR_PLAN_TOOL_NAME = "submit_repair_plan"
DEFAULT_MAX_TURNS = 8

SYSTEM_PROMPT = """You are a constrained coding-repair agent for a data pipeline.

You receive an evidence-grounded diagnosis produced by a separate read-only investigator. Your task is to propose the smallest safe repair that addresses the diagnosed root cause.

First determine whether the issue is repairable with the available authoritative evidence. Never guess business semantics. If the diagnosis is uncertain, contradictory, or lacks an approved business rule, set repair_decision to HUMAN_REVIEW_REQUIRED instead of proposing a repair.

Prefer configuration changes over code changes when the implementation is already capable and the problem is stale configuration -- call get_pipeline_configuration and get_allowed_repair_targets to check whether a configuration-only fix is available before proposing a code change. Prefer narrow code changes over refactoring: touch only the one function implicated by the diagnosis and the ETL source you inspected.

You must propose exactly one target_file, and it MUST be one of the entries returned by get_allowed_repair_targets -- call that tool before proposing anything. If the diagnosis's own recommended_fix names a file that is not in that allowlist, look for the real, precise, allowed target instead (e.g. a configuration file that controls which business rules the ETL actually uses) rather than proposing the disallowed file.

files_expected_to_change must list ONLY the file(s) your patch itself edits -- for this MVP that is exactly one file, the target_file. Do NOT include output artifacts that will be regenerated as a downstream consequence of rerunning the pipeline (e.g. portfolio_summary.json, validation_results.json) -- those are produced by a separate verification step, not by your patch, and listing them will cause your plan to be rejected.

You must not modify raw data, validation rules, validation code, expected outputs, diagnosis evidence, credentials, or unrelated files. You must not weaken checks to make the pipeline pass. A CONFIGURATION_CHANGE must use patch.format=STRUCTURED_CONFIG_EDIT (a small list of field/value operations, never free-form text); a CODE_CHANGE must use patch.format=UNIFIED_DIFF against exactly the function implicated by the diagnosis -- derive the actual diff yourself from the ETL source and the business rule; do not invent a patch that isn't grounded in what you actually inspected.

Every evidence_references entry must be the exact source_reference of an evidence item that already exists in the diagnosis you were given -- do not invent new evidence.

Produce a structured repair plan. Do not claim success or that a repair has occurred. A separate deterministic verifier will validate policy, apply the change in an isolated workspace, run tests, rerun the ETL, and rerun validation -- only that process may mark a repair verified.

When you are ready to conclude, and only then, call the submit_repair_plan tool exactly once with your full structured plan. You are not finished until you call submit_repair_plan."""


class RepairAgentError(Exception):
    """Raised when the agent fails to reach a plan (e.g. max turns exceeded)."""


def _submit_repair_plan_tool_spec() -> dict:
    patch_schema = {
        "type": ["object", "null"],
        "properties": {
            "format": {"type": "string", "enum": ["UNIFIED_DIFF", "STRUCTURED_CONFIG_EDIT", "NONE"]},
            "content": {
                "description": "A unified diff string (UNIFIED_DIFF), an object with an 'operations' list of {field, value} (STRUCTURED_CONFIG_EDIT), or null (NONE).",
            },
        },
        "required": ["format", "content"],
    }
    return {
        "type": "function",
        "function": {
            "name": SUBMIT_REPAIR_PLAN_TOOL_NAME,
            "description": "Submit your final, evidence-backed repair plan. Call this exactly once, when planning is complete.",
            "parameters": {
                "type": "object",
                "properties": {
                    "repair_decision": {
                        "type": "string",
                        "enum": ["PROPOSE_REPAIR", "NO_SAFE_REPAIR", "HUMAN_REVIEW_REQUIRED"],
                    },
                    "repair_type": {"type": "string", "enum": ["CONFIGURATION_CHANGE", "CODE_CHANGE", "NONE"]},
                    "incident_id": {"type": "string"},
                    "diagnosis_reference": {"type": "string", "description": "e.g. the diagnosis's root_cause_category or incident_summary, identifying which diagnosis this plan addresses."},
                    "root_cause_addressed": {"type": "string"},
                    "target_file": {"type": ["string", "null"], "description": "Must be one of get_allowed_repair_targets()'s keys, or null."},
                    "target_symbol_or_setting": {"type": ["string", "null"]},
                    "current_behavior": {"type": "string"},
                    "proposed_behavior": {"type": "string"},
                    "change_description": {"type": "string"},
                    "patch": patch_schema,
                    "files_expected_to_change": {"type": "array", "items": {"type": "string"}},
                    "files_expected_not_to_change": {"type": "array", "items": {"type": "string"}},
                    "verification_steps": {"type": "array", "items": {"type": "string"}},
                    "rollback_description": {"type": "string"},
                    "risk_level": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
                    "assumptions": {"type": "array", "items": {"type": "string"}},
                    "evidence_references": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Each entry must exactly match a source_reference from the diagnosis's own evidence list.",
                    },
                },
                "required": [
                    "repair_decision", "repair_type", "incident_id", "diagnosis_reference", "root_cause_addressed",
                    "target_file", "target_symbol_or_setting", "current_behavior", "proposed_behavior",
                    "change_description", "patch", "files_expected_to_change", "files_expected_not_to_change",
                    "verification_steps", "rollback_description", "risk_level", "assumptions", "evidence_references",
                ],
            },
        },
    }


def _serialize_tool_call(call: ToolCall) -> dict:
    return {
        "id": call.id,
        "type": "function",
        "function": {"name": call.name, "arguments": json.dumps(call.arguments)},
    }


def run_repair_planning(
    starting_context: dict,
    tools: RepairTools,
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
