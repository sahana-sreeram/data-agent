"""Tests for src/context_store/merge.py -- precedence and, most importantly, that a
human-vs-code disagreement is surfaced as a conflict rather than silently resolved."""

from __future__ import annotations

from src.context_store.merge import merge_context
from src.context_store.models import (
    ConflictStatus,
    DerivedMetric,
    GeneratedContext,
    HumanAnnotation,
    MetricAnnotation,
    RuntimeHealth,
)


def _generated(formula: str) -> GeneratedContext:
    return GeneratedContext(
        asset_id="delinquency_default",
        generated_by="code_enricher",
        generated_at="2026-07-27T00:00:00Z",
        derived_metrics=[DerivedMetric(name="loss_rate", formula=formula, source_fields=["net_loss"])],
    )


def _human(denominator: str) -> HumanAnnotation:
    return HumanAnnotation(
        data_product="delinquency_default",
        authoritative=True,
        owner="risk-data-team",
        metrics={"loss_rate": MetricAnnotation(canonical_definition="net loss over funded principal", business_rule={"denominator": denominator}, approved_by="risk-finance")},
    )


def test_merge_with_no_conflict():
    generated = _generated("net_loss / total_funded_principal")
    human = _human("total_funded_principal")
    resolved = merge_context("delinquency_default", generated, human)
    assert resolved.conflicts == []
    assert resolved.human == human
    assert resolved.generated == generated


def test_merge_detects_denominator_mismatch_and_preserves_both_values():
    generated = _generated("net_loss / total_balance_at_default")
    human = _human("total_funded_principal")
    resolved = merge_context("delinquency_default", generated, human)

    assert len(resolved.conflicts) == 1
    conflict = resolved.conflicts[0]
    assert conflict.field == "loss_rate.denominator"
    assert conflict.human_approved == "total_funded_principal"
    assert conflict.code_observed == "net_loss / total_balance_at_default"
    assert conflict.conflict_status == ConflictStatus.MISMATCH

    # neither value is dropped or overwritten -- both survive on the resolved object
    assert resolved.human.metrics["loss_rate"].business_rule["denominator"] == "total_funded_principal"
    assert resolved.generated.derived_metrics[0].formula == "net_loss / total_balance_at_default"


def test_merge_with_only_generated_context_has_no_conflicts():
    resolved = merge_context("loan_portfolio", _generated("count(loans)"), None)
    assert resolved.conflicts == []
    assert resolved.human is None


def test_merge_with_only_human_annotation_has_no_conflicts():
    resolved = merge_context("delinquency_default", None, _human("total_funded_principal"))
    assert resolved.conflicts == []
    assert resolved.generated is None


def test_merge_includes_runtime_health():
    runtime = RuntimeHealth(pipeline_name="loan_portfolio", etl_status="SUCCESS", validation_status="PASS")
    resolved = merge_context("loan_portfolio", None, None, runtime)
    assert resolved.runtime == runtime
