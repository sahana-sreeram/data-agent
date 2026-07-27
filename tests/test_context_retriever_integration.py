"""Integration proof that editing ONLY a context file (no agent code) changes live Q&A and
diagnosis behavior for loan_portfolio -- the explicit "does the new context layer actually do
anything" requirement for this vertical slice.

Against real MinIO (via the s3_storage fixture, skips cleanly if unreachable) so
ContextRetriever.ensure_fresh's contract-version detection reads real data, exactly as it
would in production -- but the ContextStore itself is rooted at tmp_path, fully isolated from
the real context/generated/ and context/human/ files, so this test never touches (or depends
on) the committed loan_portfolio context.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from src.context_enrichment.contract_detector import detect_payment_service_contract_version
from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.context_store.models import (
    ApprovalStatus,
    DerivedMetric,
    GeneratedContext,
    HumanAnnotation,
    MetricAnnotation,
    PipelineMetadata,
)
from src.lifecycle_business_tools import LifecycleBusinessTools
from src.lifecycle_diagnostic_tools import LifecycleDiagnosticTools
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY

PIPELINE_NAME = "loan_portfolio"
METRIC_NAME = "total_outstanding_principal"
ORIGINAL_DEFINITION = "ORIGINAL: net of PAID payments only."
REVISED_DEFINITION = "REVISED: also recognizes SETTLED as a successful payment."


def _current_etl_source_hash() -> str:
    spec = PIPELINE_REGISTRY[PIPELINE_NAME]
    return hashlib.sha256(Path(spec.etl_source_file).read_bytes()).hexdigest()


@pytest.fixture
def retriever(tmp_path, s3_storage):
    store = FileContextStore(root=tmp_path)
    generated = GeneratedContext(
        asset_id=PIPELINE_NAME,
        generated_by="test_fixture",
        generated_at="2026-07-20T00:00:00+00:00",
        derived_metrics=[
            DerivedMetric(
                name=METRIC_NAME,
                formula="sum where payment_status == 'PAID'",  # deliberately contains "PAID"
                source_fields=["payment_status", "amount"],
            )
        ],
        pipeline_metadata=PipelineMetadata(
            pipeline_name=PIPELINE_NAME,
            etl_source_file=PIPELINE_REGISTRY[PIPELINE_NAME].etl_source_file,
            etl_source_hash=_current_etl_source_hash(),
        ),
        service_contract_versions={"payment_service": detect_payment_service_contract_version(s3_storage)},
    )
    store.save_generated_context(generated)

    human = HumanAnnotation(
        data_product=PIPELINE_NAME,
        authoritative=True,
        metrics={
            METRIC_NAME: MetricAnnotation(
                canonical_definition=ORIGINAL_DEFINITION,
                business_rule={"successful_payment_statuses": "PAID"},  # contained in the formula above -- no conflict
                approved_by="test",
                approval_status=ApprovalStatus.APPROVED,
            )
        },
    )
    store.save_human_annotation(human)

    return ContextRetriever(store=store), store


def test_editing_only_the_human_annotation_changes_context_retriever_output(retriever, s3_storage):
    ctx_retriever, store = retriever

    before = ctx_retriever.get_metric(PIPELINE_NAME, METRIC_NAME, s3_storage)
    assert before.value["canonical_definition"] == ORIGINAL_DEFINITION
    assert before.provenance == "merged"
    assert before.conflicts == []

    # Edit ONLY the human annotation file -- revise the definition AND declare a business rule
    # ("SETTLED") the generated formula (still "PAID" only) doesn't contain.
    revised_human = HumanAnnotation(
        data_product=PIPELINE_NAME,
        authoritative=True,
        metrics={
            METRIC_NAME: MetricAnnotation(
                canonical_definition=REVISED_DEFINITION,
                business_rule={"successful_payment_statuses": "SETTLED"},
                approved_by="test",
                approval_status=ApprovalStatus.APPROVED,
            )
        },
    )
    store.save_human_annotation(revised_human)

    after = ctx_retriever.get_metric(PIPELINE_NAME, METRIC_NAME, s3_storage)
    assert after.value["canonical_definition"] == REVISED_DEFINITION
    assert len(after.conflicts) == 1
    assert after.conflicts[0].field == f"{METRIC_NAME}.successful_payment_statuses"
    assert after.conflicts[0].human_approved == "SETTLED"


def test_editing_only_the_human_annotation_changes_live_qa_tool_output(retriever, s3_storage):
    """Same edit as above, but proven through the ACTUAL tool the Q&A agent calls
    (LifecycleBusinessTools.get_metric_definition), not the retriever directly -- no agent
    code is touched between the two calls."""
    ctx_retriever, store = retriever
    metrics_by_pipeline = {PIPELINE_NAME: {"metrics": {METRIC_NAME: {"business_definition": "legacy text", "formula": "x"}}}}
    tools = LifecycleBusinessTools(
        loan_portfolio={}, campaign_funnel=[], underwriting_performance=[], underwriting_rejections={},
        payment_performance={}, delinquency_default=[], business_rules={},
        metrics_by_pipeline=metrics_by_pipeline, context_retriever=ctx_retriever, storage=s3_storage,
    )

    before = tools.get_metric_definition(pipeline=PIPELINE_NAME, metric_name=METRIC_NAME)
    assert before["_context"]["human_approved_definition"]["canonical_definition"] == ORIGINAL_DEFINITION
    assert before["_context"]["conflicts"] == []

    store.save_human_annotation(
        HumanAnnotation(
            data_product=PIPELINE_NAME,
            metrics={
                METRIC_NAME: MetricAnnotation(
                    canonical_definition=REVISED_DEFINITION,
                    business_rule={"successful_payment_statuses": "SETTLED"},
                    approval_status=ApprovalStatus.APPROVED,
                )
            },
        )
    )

    after = tools.get_metric_definition(pipeline=PIPELINE_NAME, metric_name=METRIC_NAME)
    assert after["_context"]["human_approved_definition"]["canonical_definition"] == REVISED_DEFINITION
    assert len(after["_context"]["conflicts"]) == 1
    # The legacy dict (metrics_by_pipeline) is untouched by any of this -- only the "_context"
    # block reflects the context-file edit.
    assert before[METRIC_NAME] == after[METRIC_NAME] == {"business_definition": "legacy text", "formula": "x"}


@pytest.mark.parametrize(
    "pipeline_name,metric_name",
    [
        ("loan_portfolio", "total_outstanding_principal"),
        ("campaign_funnel", "approval_to_funded_rate"),
        ("underwriting_performance", "approval_rate"),
        ("payment_performance", "collection_rate"),
        ("delinquency_default", "loss_rate"),
    ],
)
def test_all_five_pipelines_are_migrated_off_the_legacy_fallback(pipeline_name, metric_name, s3_storage):
    """Phase 2: every pipeline now has context/generated/<name>.json + context/human/<name>.yaml
    committed for real (not the isolated tmp_path store the other tests in this file use) --
    against the REAL FileContextStore, get_metric() must resolve to "human" (or "merged"/
    "generated"), never fall back to "legacy_file", for every one of the 5 original pipelines.
    Proves the migration needed zero agent-code changes: the exact same ContextRetriever/
    LifecycleBusinessTools code loan_portfolio used now serves all 5."""
    retriever = ContextRetriever(store=FileContextStore())
    fact = retriever.get_metric(pipeline_name, metric_name, s3_storage)
    assert fact.provenance != "legacy_file"


def test_editing_only_the_generated_context_surfaces_via_get_context_conflicts(retriever, s3_storage):
    """Same idea from the diagnosis tool surface: mutating the GENERATED (code-derived) side
    instead of the human side still surfaces a conflict, purely from the context-file edit."""
    ctx_retriever, store = retriever
    tools = LifecycleDiagnosticTools(
        metrics={"metrics": {METRIC_NAME: {"formula": "x"}}},
        pipeline_name=PIPELINE_NAME,
        storage=s3_storage,
        context_retriever=ctx_retriever,
    )

    before = tools.get_context_conflicts(METRIC_NAME)
    assert before["conflicts"] == []

    drifted_generated = GeneratedContext(
        asset_id=PIPELINE_NAME,
        generated_by="test_fixture",
        generated_at="2026-07-20T00:00:00+00:00",
        derived_metrics=[
            DerivedMetric(name=METRIC_NAME, formula="sum where payment_status == 'SETTLED'", source_fields=["payment_status"])
        ],
        pipeline_metadata=PipelineMetadata(
            pipeline_name=PIPELINE_NAME,
            etl_source_file=PIPELINE_REGISTRY[PIPELINE_NAME].etl_source_file,
            etl_source_hash=_current_etl_source_hash(),
        ),
        service_contract_versions={"payment_service": detect_payment_service_contract_version(s3_storage)},
    )
    store.save_generated_context(drifted_generated)

    after = tools.get_context_conflicts(METRIC_NAME)
    assert len(after["conflicts"]) == 1
    assert after["conflicts"][0]["field"] == f"{METRIC_NAME}.successful_payment_statuses"
