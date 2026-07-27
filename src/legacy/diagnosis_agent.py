"""The diagnosis agent's tool-calling reasoning loop.

Starts from a validation-failure signal only (see
diagnose_incident.build_starting_context) and lets the model choose which
read-only tools to call, how many times, and in what order. The loop itself
encodes no investigation sequence -- it just dispatches whatever the model
asks for and feeds the real result back, until the model calls the special
submit_diagnosis tool, at which point the arguments are schema- and
grounding-validated into a DiagnosisResult.
"""

from __future__ import annotations

import json

from src.legacy.diagnosis_models import DiagnosisResult, parse_diagnosis_result
from src.legacy.diagnostic_tools import TOOL_SPECS, DiagnosticTools, dispatch_tool
from src.model_client import DiagnosisModelClient, ToolCall

SUBMIT_DIAGNOSIS_TOOL_NAME = "submit_diagnosis"
DEFAULT_MAX_TURNS = 8

SYSTEM_PROMPT = """You are a read-only data-pipeline incident investigator. Your job is to diagnose why deterministic validation failed.

Begin with the failed validation checks. Use the available tools to inspect approved business definitions, lineage, raw-source characteristics, pipeline metadata, and relevant ETL logic.

Do not guess from the validation numbers alone. Gather evidence before concluding.

IMPORTANT: an ETL or pipeline "SUCCESS" status means only that it executed without raising an error. It does NOT mean the ETL's output is correct relative to the currently approved business rules. A pipeline can run to completion every time and still be silently wrong -- for example, if the ETL's logic (or the configuration it was last run with) has fallen out of sync with an updated, approved business rule. Do not treat a SUCCESS execution status as evidence against an ETL logic or staleness problem. If reconciliation checks are failing (reported/expected numeric mismatches), that is direct evidence of an output-correctness problem regardless of execution status -- investigate metric lineage and the relevant ETL source to understand why.

Prefer the smallest root cause that explains all relevant failures. Distinguish observed facts from inference. Every important conclusion must cite evidence returned by a tool -- in your final diagnosis, each evidence item's source_reference must be either the exact name of a tool you called this session or one of the fixed source file paths you were told about. Do not invent file paths, tool names, or metric names.

Distinguish an initiating_event from the root_cause when they differ. An initiating_event is an external trigger -- e.g. an approved upstream source or business-rule change -- that is not itself broken and does not need repair. The root_cause is always the specific, repairable thing that must change to fix the incident: e.g. a downstream component (ETL logic, or the configuration it was run with) that failed to keep up with that change. An approved contract change is not, by itself, a root cause explaining why a downstream component produced wrong output -- if a downstream component is stale relative to the current approved rule, name that staleness as the root_cause and set initiating_event to the external trigger that made it stale. Set initiating_event to null when there is no separate external trigger (e.g. a plain implementation bug).

You may recommend a minimal repair targeting the root_cause, but you must not modify code, run commands, rerun pipelines, or claim that a repair has occurred.

If the evidence is insufficient or contradictory, set diagnosis_status to INSUFFICIENT_EVIDENCE and explain what additional evidence is needed in additional_evidence_needed. Do not fabricate a confident diagnosis when the evidence does not support one.

You are not required to call every available tool -- choose based on what the failed checks suggest and what you learn along the way.

When you are ready to conclude, and only then, call the submit_diagnosis tool exactly once with your full structured diagnosis. You are not finished until you call submit_diagnosis."""


class DiagnosisAgentError(Exception):
    """Raised when the agent fails to reach a diagnosis (e.g. max turns exceeded)."""


def _submit_diagnosis_tool_spec() -> dict:
    evidence_schema = {
        "type": "object",
        "properties": {
            "source_type": {
                "type": "string",
                "enum": ["VALIDATION", "BUSINESS_RULE", "RAW_DATA", "LINEAGE", "ETL_SOURCE", "PIPELINE_METADATA"],
            },
            "source_reference": {
                "type": "string",
                "description": "The exact name of a tool you called this session, or a known source file path.",
            },
            "finding": {"type": "string"},
            "expected": {"type": ["string", "null"]},
            "actual": {"type": ["string", "null"]},
        },
        "required": ["source_type", "source_reference", "finding", "expected", "actual"],
    }
    recommended_fix_schema = {
        "type": ["object", "null"],
        "properties": {
            "target_file": {"type": ["string", "null"]},
            "change_summary": {"type": "string"},
            "scope": {"type": "string", "enum": ["MINIMAL", "BROADER_REVIEW"]},
        },
        "required": ["target_file", "change_summary", "scope"],
    }
    return {
        "type": "function",
        "function": {
            "name": SUBMIT_DIAGNOSIS_TOOL_NAME,
            "description": "Submit your final, evidence-backed diagnosis. Call this exactly once, when investigation is complete.",
            "parameters": {
                "type": "object",
                "properties": {
                    "diagnosis_status": {
                        "type": "string",
                        "enum": ["DIAGNOSED", "NO_INCIDENT", "INSUFFICIENT_EVIDENCE"],
                    },
                    "incident_summary": {"type": "string"},
                    "affected_metrics": {"type": "array", "items": {"type": "string"}},
                    "root_cause_category": {
                        "type": "string",
                        "enum": [
                            "SOURCE_CONTRACT_CHANGE", "ETL_LOGIC", "MISSING_DATA", "DUPLICATION",
                            "SCHEMA_MISMATCH", "BUSINESS_RULE_MISMATCH", "UNKNOWN",
                        ],
                    },
                    "initiating_event": {
                        "type": ["string", "null"],
                        "description": "The external trigger (e.g. an approved contract change), if distinct from root_cause. Null if none.",
                    },
                    "root_cause": {
                        "type": "string",
                        "description": "The specific, repairable cause -- not the initiating event.",
                    },
                    "reasoning_summary": {"type": "string"},
                    "evidence": {"type": "array", "items": evidence_schema},
                    "recommended_fix": recommended_fix_schema,
                    "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
                    "uncertainties": {"type": "array", "items": {"type": "string"}},
                    "additional_evidence_needed": {"type": "array", "items": {"type": "string"}},
                },
                "required": [
                    "diagnosis_status", "incident_summary", "affected_metrics", "root_cause_category",
                    "initiating_event", "root_cause", "reasoning_summary", "evidence", "recommended_fix",
                    "confidence", "uncertainties", "additional_evidence_needed",
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


def run_diagnosis(
    starting_context: dict,
    tools: DiagnosticTools,
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
