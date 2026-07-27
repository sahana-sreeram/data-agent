"""Tests for src/context_store/{file_store,sqlite_store}.py -- both ContextStore
implementations must behave identically against the same Protocol, and neither should ever
touch the existing hand-authored context/*.json files."""

from __future__ import annotations

from src.context_store.file_store import FileContextStore
from src.context_store.models import (
    DatasetMetadata,
    DerivedMetric,
    GeneratedContext,
    HumanAnnotation,
    LineageChain,
    LineageStep,
    MetricAnnotation,
    PipelineMetadata,
    RepairPolicy,
    RuntimeHealth,
)
from src.context_store.sqlite_store import SQLiteContextStore

GENERATED = GeneratedContext(
    asset_id="loan_portfolio",
    generated_by="code_enricher",
    generated_at="2026-07-27T00:00:00Z",
    grain="portfolio-wide",
    sources=["lifecycle.loans", "lifecycle.payment_events"],
    derived_metrics=[DerivedMetric(name="loss_rate", formula="net_loss / total_balance_at_default", source_fields=["net_loss"])],
    dataset_metadata=DatasetMetadata(dataset_name="loan_portfolio", physical_location="s3://data-agent/curated/loan_portfolio.parquet"),
    pipeline_metadata=PipelineMetadata(pipeline_name="loan_portfolio", etl_source_file="src/etl_spark_loan_portfolio.py"),
    lineage=LineageChain(asset_id="loan_portfolio", steps=[LineageStep(kind="curated_dataset", name="loan_portfolio")]),
)

HUMAN = HumanAnnotation(
    data_product="loan_portfolio",
    authoritative=True,
    owner="risk-data-team",
    metrics={"loss_rate": MetricAnnotation(canonical_definition="net loss over funded principal", business_rule={"denominator": "total_funded_principal"}, approved_by="risk-finance")},
    repair_policy=RepairPolicy(auto_repair=["ETL_LOGIC_JOIN"], human_review=["SOURCE_CONTRACT_CHANGE"]),
)

RUNTIME = RuntimeHealth(pipeline_name="loan_portfolio", etl_status="SUCCESS", validation_status="PASS")


def _round_trip(store) -> None:
    assert store.get_generated_context("loan_portfolio") is None
    store.save_generated_context(GENERATED)
    loaded = store.get_generated_context("loan_portfolio")
    assert loaded == GENERATED

    assert store.get_data_product("loan_portfolio").dataset_name == "loan_portfolio"
    assert store.get_pipeline_context("loan_portfolio").etl_source_file == "src/etl_spark_loan_portfolio.py"
    assert store.get_lineage("loan_portfolio").steps[0].name == "loan_portfolio"

    assert store.get_human_annotation("loan_portfolio") is None
    store.save_human_annotation(HUMAN)
    loaded_human = store.get_human_annotation("loan_portfolio")
    assert loaded_human == HUMAN

    # get_metric prefers the human-approved annotation over the code-derived one
    metric = store.get_metric("loan_portfolio", "loss_rate")
    assert metric["canonical_definition"] == "net loss over funded principal"

    assert store.get_runtime_health("loan_portfolio") is None
    store.save_runtime_health(RUNTIME)
    assert store.get_runtime_health("loan_portfolio") == RUNTIME


def test_file_context_store_round_trip(tmp_path):
    _round_trip(FileContextStore(root=tmp_path / "context"))


def test_file_context_store_never_touches_real_context_dir(tmp_path):
    from pathlib import Path

    real_business_rules = Path("context/business_rules.json")
    before = real_business_rules.read_bytes() if real_business_rules.exists() else None

    store = FileContextStore(root=tmp_path / "context")
    store.save_generated_context(GENERATED)

    assert (tmp_path / "context" / "generated" / "loan_portfolio.json").exists()
    after = real_business_rules.read_bytes() if real_business_rules.exists() else None
    assert before == after


def test_sqlite_context_store_round_trip(tmp_path):
    store = SQLiteContextStore(db_path=tmp_path / "context.db")
    try:
        _round_trip(store)
    finally:
        store.close()


def test_file_and_sqlite_stores_agree(tmp_path):
    file_store = FileContextStore(root=tmp_path / "context")
    sqlite_store = SQLiteContextStore(db_path=tmp_path / "context.db")
    try:
        file_store.save_generated_context(GENERATED)
        sqlite_store.save_generated_context(GENERATED)
        assert file_store.get_generated_context("loan_portfolio") == sqlite_store.get_generated_context("loan_portfolio")
    finally:
        sqlite_store.close()
