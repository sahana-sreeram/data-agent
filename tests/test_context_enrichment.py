"""Tests for src/context_enrichment/*.py. Structural extraction and lineage-building are
pure functions, tested without any infra. Schema/runtime introspection need real S3
(skip cleanly if unreachable, via tests/conftest.py's s3_storage fixture). The Codex pass
uses ScriptedDiagnosisModelClient -- no live model call, ever, in this file."""

from __future__ import annotations

import uuid

import pandas as pd
import pytest

from src.context_enrichment.code_enricher import (
    _SUBMIT_TOOL_NAME,
    enrich_pipeline_structurally,
    enrich_pipeline_with_codex,
    extract_business_rule_references,
    extract_filters,
    extract_joins,
)
from src.context_enrichment.lineage_builder import build_lineage
from src.context_enrichment.runtime_enricher import enrich_runtime_health
from src.context_enrichment.schema_introspector import introspect_dataset
from src.context_enrichment.validation import EnrichmentValidationError, validate_context
from src.context_store.models import GeneratedContext
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall
from tests.conftest import PrefixedStorage

SAMPLE_SOURCE = '''
def compute_loan_portfolio(spark, business_rules, as_of_date):
    net_payment_statuses = business_rules["successful_payment_statuses"] + ["REVERSED"]
    filtered = payment_events.filter(F.col("payment_status").isin(net_payment_statuses))
    joined = loans.join(net_paid_by_loan, on="loan_id", how="left").fillna({"net_paid": 0.0})
    accrual_statuses = business_rules.get("interest_accrual", {})["accrues_on_statuses"]
    return joined
'''


def test_extract_joins_finds_on_and_how():
    joins = extract_joins(SAMPLE_SOURCE)
    assert len(joins) == 1
    assert joins[0].right == "net_paid_by_loan"
    assert joins[0].on == ["loan_id"]
    assert joins[0].how == "left"


def test_extract_joins_resolves_left_when_a_bare_identifier_precedes_join():
    # "loans" sits directly before ".join(" -- recoverable without a real AST walk.
    assert extract_joins(SAMPLE_SOURCE)[0].left == "loans"


def test_extract_joins_leaves_left_unresolved_for_a_chained_continuation():
    # The second .join(...) here is chained onto the first call's (anonymous) result --
    # genuinely not nameable from source text alone, so it must stay the honest "?"
    # placeholder rather than a guess, while the FIRST join's "a" is still resolved.
    chained_source = """
    result = (
        a.join(b, on="k", how="left")
        .join(c, on="k", how="left")
    )
    """
    joins = extract_joins(chained_source)
    assert len(joins) == 2
    assert joins[0].left == "a"
    assert joins[0].right == "b"
    assert joins[1].left == "?"
    assert joins[1].right == "c"


def test_extract_filters_finds_filter_expression():
    filters = extract_filters(SAMPLE_SOURCE)
    assert len(filters) == 1
    assert "payment_status" in filters[0].expression


def test_extract_business_rule_references_finds_both_access_styles():
    references = extract_business_rule_references(SAMPLE_SOURCE)
    assert references == ["interest_accrual", "successful_payment_statuses"]


def test_enrich_pipeline_structurally_real_loan_portfolio_etl():
    metadata = enrich_pipeline_structurally("loan_portfolio")
    assert metadata.pipeline_name == "loan_portfolio"
    assert metadata.etl_source_file == "src/etl_spark_loan_portfolio.py"
    assert "compute_loan_portfolio" in metadata.functions
    assert any(join.how == "left" for join in metadata.joins)
    assert "successful_payment_statuses" in metadata.business_rule_lookups


def test_build_lineage_chain_shape():
    chain = build_lineage("loan_portfolio", "total_outstanding_principal")
    kinds = [step.kind for step in chain.steps]
    assert kinds[:3] == ["business_metric", "curated_field", "curated_dataset"]
    assert "spark_function" in kinds
    assert "source_table" in kinds


def test_build_lineage_traces_to_the_owning_upstream_service():
    # loan_portfolio reads loans (owned by loan_service) and payment_events (owned by
    # payment_service) -- RAW_TABLE_TO_SERVICE is derived from
    # src.events_to_lifecycle_tables's own event-type mappings, not hand-duplicated, so this
    # is what makes "trace the incident to the upstream service" a real, working lineage step
    # rather than the empty placeholder it used to be.
    chain = build_lineage("loan_portfolio", "total_outstanding_principal")
    services = [step.name for step in chain.steps if step.kind == "upstream_service"]
    assert services == ["loan_service", "payment_service"]

    # Every source_table step is immediately followed by its owning upstream_service step,
    # in raw_tables order -- not just present somewhere in the chain.
    steps = [(step.kind, step.name) for step in chain.steps]
    source_index = steps.index(("source_table", "loans"))
    assert steps[source_index + 1] == ("upstream_service", "loan_service")


def test_enrich_pipeline_with_codex_uses_scripted_client_not_live():
    scripted = ScriptedDiagnosisModelClient(
        [
            ModelResponse(
                tool_calls=[
                    ToolCall(
                        id="1",
                        name=_SUBMIT_TOOL_NAME,
                        arguments={
                            "grain": "portfolio-wide",
                            "caveats": ["nets REVERSED against PAID"],
                            "derived_metrics": [{"name": "total_outstanding_principal", "formula": "principal - net_paid", "source_fields": ["principal_amount"]}],
                            "confidence": {"grain": 0.9},
                        },
                    )
                ]
            )
        ]
    )
    context = enrich_pipeline_with_codex("loan_portfolio", scripted, generated_at="2026-07-27T00:00:00Z")
    assert isinstance(context, GeneratedContext)
    assert context.generated_by == "codex"
    assert context.grain == "portfolio-wide"
    assert context.derived_metrics[0].name == "total_outstanding_principal"
    assert context.confidence == {"grain": 0.9}
    # structural extraction is still present alongside the model-derived fields
    assert any(join.how == "left" for join in context.joins)


def test_validate_context_rejects_malformed_input():
    with pytest.raises(EnrichmentValidationError):
        validate_context(GeneratedContext, {"asset_id": "x"})  # missing required fields


def test_validate_context_accepts_well_formed_input():
    result = validate_context(
        GeneratedContext,
        {"asset_id": "x", "generated_by": "test", "generated_at": "2026-07-27T00:00:00Z"},
    )
    assert isinstance(result, GeneratedContext)


def test_introspect_dataset(s3_storage):
    prefix = f"test-context-enrichment-{uuid.uuid4().hex[:8]}/"
    storage = PrefixedStorage(s3_storage, prefix)
    df = pd.DataFrame({"loan_id": ["L1", "L2", "L3"], "principal_amount": [1000.0, 2000.0, None]})
    storage.write_parquet("curated/loan_portfolio.parquet", df)

    metadata = introspect_dataset(storage, "loan_portfolio", "curated/loan_portfolio.parquet")
    assert metadata.dataset_name == "loan_portfolio"
    assert metadata.row_count_estimate == 3
    assert "loan_id" in metadata.candidate_keys
    assert "principal_amount" in metadata.nullable_columns


def test_enrich_runtime_health(s3_storage):
    prefix = f"test-context-enrichment-{uuid.uuid4().hex[:8]}/"
    storage = PrefixedStorage(s3_storage, prefix)
    storage.write_json(
        "curated/pipeline_run.json",
        {"pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "FAIL"}}},
    )
    storage.write_json(
        "curated/loan_portfolio_validation_results.json",
        {"checks": [{"id": "loan_count_reconciliation", "status": "FAIL"}, {"id": "other_check", "status": "PASS"}]},
    )

    health = enrich_runtime_health(storage, "loan_portfolio")
    assert health.etl_status == "SUCCESS"
    assert health.validation_status == "FAIL"
    assert health.failed_check_ids == ["loan_count_reconciliation"]


def test_enrich_runtime_health_degrades_gracefully_when_missing(s3_storage):
    prefix = f"test-context-enrichment-{uuid.uuid4().hex[:8]}/"
    storage = PrefixedStorage(s3_storage, prefix)
    health = enrich_runtime_health(storage, "loan_portfolio")
    assert health.etl_status == "UNKNOWN"
    assert health.failed_check_ids == []
