"""The lifecycle diagnosis agent's tool-calling reasoning loop. Parallel to
src/diagnosis_agent.py (left completely unmodified) for the lifecycle model's tool surface.
The loop shape and submit_diagnosis tool schema are still fully generic -- reused directly
from src.diagnosis_agent -- but SYSTEM_PROMPT is NOT: this is the first real divergence
from the original prompt, because the lifecycle tool surface has a genuinely richer,
metric-aware set of tools (get_failed_metric_context, get_metric_lineage,
compare_metric_definition_to_etl, trace_failed_check_to_code -- see
src/lifecycle_diagnostic_tools.py) that the original model doesn't have, and a directed
investigation order is worth spelling out explicitly rather than leaving fully open-ended.

Why: a live run diagnosing delinquency_default's loss_rate bug (a hardcoded denominator
column instead of reading business_rules.loss_rate_denominator) ran out of its turn budget
-- the model had every fact it needed by turn 2 but spent turns 2-7 profiling/sampling raw
data, a strategy suited to a DIFFERENT bug class (a data-join failure, like
loan_portfolio's). This prompt is still guidance, not a hard-enforced state machine (the
tool-calling loop itself encodes no sequencing, exactly as before) -- it just makes the
cheap, targeted path so much more obviously first-to-try that the model should converge on
it before falling back to open-ended data exploration.
"""

from __future__ import annotations

import json

from src.legacy.diagnosis_agent import (
    DEFAULT_MAX_TURNS,
    SUBMIT_DIAGNOSIS_TOOL_NAME,
    DiagnosisAgentError,
    _serialize_tool_call,
    _submit_diagnosis_tool_spec,
)
from src.legacy.diagnosis_models import DiagnosisResult, parse_diagnosis_result
from src.lifecycle_diagnostic_tools import TOOL_SPECS, LifecycleDiagnosticTools, dispatch_tool
from src.model_client import DiagnosisModelClient, ToolCall

__all__ = ["DiagnosisAgentError", "run_lifecycle_diagnosis"]

SYSTEM_PROMPT = """You are a read-only data-pipeline incident investigator for one of 5 lifecycle business pipelines (loan_portfolio, campaign_funnel, underwriting_performance, payment_performance, delinquency_default). Your job is to diagnose why deterministic validation failed.

Follow this investigation order. It is strong guidance, not a rigid script -- skip a step if you already have what it would tell you, and abandon it if the evidence points elsewhere -- but it is deliberately ordered from cheapest/most-targeted to most-expensive/most-open-ended, so work through it roughly in order rather than jumping straight to open-ended data exploration:

1. Identify the failed check(s) -- call get_failed_checks first, always.
2. For each failed check, call trace_failed_check_to_code(check_id) to jump directly to the candidate metric(s) and the relevant ETL source in one call.
3. For the metric(s) in question, call get_failed_metric_context(metric_name) (or get_metric_definition / get_metric_lineage individually) to load its formula, lineage, and business definition.
4. Call get_pipeline_business_rules() (or get_business_rules()) to see the currently approved rules the metric's formula is supposed to honor. Also call get_context_conflicts(metric_name): for a pipeline with a human-approved semantic definition on file, this directly names any place where that approved definition and what the code actually computes have drifted apart -- e.g. an approved successful-payment status the code no longer recognizes. A non-empty result here is strong, direct evidence, not something to treat as secondary to the general-purpose tools below.
5. Call get_relevant_etl_source() (already returned by trace_failed_check_to_code, but callable directly too) to see exactly what the code computes -- ALL of the pipeline's ETL functions, including private helpers, not just the top-level one.
6. Call compare_metric_definition_to_etl(metric_name) for each candidate metric. This is a fast, structural, deterministic check: it looks at the metric's declared business_rule_dependencies and tells you whether the ETL source actually contains a lookup for each one. mismatch=true is a strong, direct signal that the code silently stopped reading an approved business rule (a BUSINESS_RULE_MISMATCH root cause) -- when you see this, you very likely already have your answer; read the exact source line(s) around the missing lookup to confirm and describe the fix.
7. Only if the above doesn't resolve it (e.g. compare_metric_definition_to_etl reports no mismatch, or the metric has no business_rule_dependencies at all) fall back to the general-purpose data-investigation tools -- list_datasets, get_dataset_schema, profile_dataset, analyze_key_cardinality, compare_dataset_keys, aggregate_dataset, sample_dataset -- to look for a genuine data/join/aggregation bug instead (e.g. an inner join silently dropping rows that should have been kept, a wrong group-by key, a cardinality mismatch). This is a DIFFERENT bug shape from step 6's: no business rule is being ignored, the code's own join/aggregation logic itself is wrong.

IMPORTANT: an ETL or pipeline "SUCCESS" status means only that it executed without raising an error. It does NOT mean the ETL's output is correct relative to the currently approved business rules. A pipeline can run to completion every time and still be silently wrong -- for example, if the ETL's logic (or the configuration it was last run with) has fallen out of sync with an updated, approved business rule, or if a join/aggregation bug silently drops or duplicates rows. Do not treat a SUCCESS execution status as evidence against either kind of problem.

Prefer the smallest root cause that explains all relevant failures. Distinguish observed facts from inference. Every important conclusion must cite evidence returned by a tool -- in your final diagnosis, each evidence item's source_reference must be either the exact name of a tool you called this session or one of the fixed source file paths you were told about. Do not invent file paths, tool names, or metric names.

Distinguish an initiating_event from the root_cause when they differ. An initiating_event is an external trigger -- e.g. an approved upstream source or business-rule change -- that is not itself broken and does not need repair. The root_cause is always the specific, repairable thing that must change to fix the incident. Set initiating_event to null when there is no separate external trigger (e.g. a plain implementation bug, or a join/aggregation bug).

You may recommend a minimal repair targeting the root_cause, but you must not modify code, run commands, rerun pipelines, or claim that a repair has occurred.

If the evidence is insufficient or contradictory, set diagnosis_status to INSUFFICIENT_EVIDENCE and explain what additional evidence is needed in additional_evidence_needed. Do not fabricate a confident diagnosis when the evidence does not support one.

You are not required to call every available tool -- choose based on what the failed checks suggest and what you learn along the way.

When you are ready to conclude, and only then, call the submit_diagnosis tool exactly once with your full structured diagnosis. You are not finished until you call submit_diagnosis."""


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
