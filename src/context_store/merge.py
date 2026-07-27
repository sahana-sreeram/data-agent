"""Precedence-aware merge of the context layers for one asset.

Precedence (highest to lowest): human-approved business semantics > deterministic platform
metadata (runtime health, schema) > code-derived context > model-generated inference. In
practice this means: human annotation fields always win in the resolved view, but a mismatch
between what a human approved and what the code actually does is never silently dropped --
it's surfaced as a ContextConflict for the diagnosis agent to use as evidence.

Conflict detection here is intentionally a cheap, explicit heuristic (substring containment
between a human-approved business-rule value and the code-observed formula string), not a
semantic equivalence checker -- same documented tradeoff as
lifecycle_diagnostic_tools.compare_metric_definition_to_etl's regex-based structural check.
"""

from __future__ import annotations

from src.context_store.models import (
    ContextConflict,
    ConflictStatus,
    GeneratedContext,
    HumanAnnotation,
    ResolvedContext,
    RuntimeHealth,
)


def _metric_conflicts(generated: GeneratedContext | None, human: HumanAnnotation | None) -> list[ContextConflict]:
    if generated is None or human is None:
        return []

    derived_by_name = {metric.name: metric for metric in generated.derived_metrics}
    conflicts: list[ContextConflict] = []

    for metric_name, annotation in human.metrics.items():
        code_metric = derived_by_name.get(metric_name)
        if code_metric is None:
            continue  # nothing to compare against -- not a conflict, just not yet extracted

        for rule_key, rule_value in annotation.business_rule.items():
            if str(rule_value) not in code_metric.formula:
                conflicts.append(
                    ContextConflict(
                        field=f"{metric_name}.{rule_key}",
                        human_approved=rule_value,
                        code_observed=code_metric.formula,
                        conflict_status=ConflictStatus.MISMATCH,
                    )
                )

    return conflicts


def merge_context(
    asset_id: str,
    generated: GeneratedContext | None,
    human: HumanAnnotation | None,
    runtime: RuntimeHealth | None = None,
) -> ResolvedContext:
    """Combine generated + human + runtime context for one asset, preserving any conflict
    between the human-approved and code-observed layers rather than picking a winner."""
    return ResolvedContext(
        asset_id=asset_id,
        generated=generated,
        human=human,
        runtime=runtime,
        conflicts=_metric_conflicts(generated, human),
    )
