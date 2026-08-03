"""Tests for src/context_retriever.py's DEMO_CONTEXT_MODE ablation toggle
(BlindContextRetriever/build_context_retriever) -- the live "does this actually need the
context layer" demo capability. No real S3/model calls; BlindContextRetriever never touches
storage at all except for the one method it deliberately leaves working."""

from __future__ import annotations

from src.context_retriever import (
    DEMO_CONTEXT_MODE_ENV_VAR,
    BlindContextRetriever,
    ContextRetriever,
    build_context_retriever,
)

PIPELINE_NAME = "loan_portfolio"


class _FakeStore:
    def get_runtime_health(self, pipeline_name: str):
        return None


class _FakeStorage:
    def exists(self, path: str) -> bool:
        return False

    def read_json(self, path: str):
        raise AssertionError(f"should not read {path!r} in this test")


def _blind_with_fake_real():
    return BlindContextRetriever(_real=ContextRetriever(store=_FakeStore()))


def test_blind_context_retriever_get_metric_returns_no_value():
    fact = _blind_with_fake_real().get_metric(PIPELINE_NAME, "total_outstanding_principal", _FakeStorage())
    assert fact.value is None
    assert fact.provenance == "context_layer_disabled"
    assert fact.asset_id == PIPELINE_NAME
    assert fact.field == "total_outstanding_principal"


def test_blind_context_retriever_get_lineage_returns_no_value():
    fact = _blind_with_fake_real().get_lineage(PIPELINE_NAME, _FakeStorage())
    assert fact.value is None
    assert fact.provenance == "context_layer_disabled"
    assert fact.field == "lineage"


def test_blind_context_retriever_get_pipeline_metadata_returns_no_value():
    fact = _blind_with_fake_real().get_pipeline_metadata(PIPELINE_NAME, _FakeStorage())
    assert fact.value is None
    assert fact.provenance == "context_layer_disabled"


def test_blind_context_retriever_get_relevant_code_returns_no_value():
    fact = _blind_with_fake_real().get_relevant_code(PIPELINE_NAME, _FakeStorage())
    assert fact.value is None
    assert fact.provenance == "context_layer_disabled"


def test_blind_context_retriever_get_business_rules_returns_no_value():
    fact = _blind_with_fake_real().get_business_rules(PIPELINE_NAME, _FakeStorage())
    assert fact.value is None
    assert fact.provenance == "context_layer_disabled"


def test_blind_context_retriever_still_reports_runtime_health():
    """get_runtime_health is deliberately NOT blinded -- an agent should still see basic
    operational status (is this pipeline healthy right now) even with the context layer off;
    only the semantic/explanatory tools go dark."""
    storage = _FakeStorage()
    storage.exists = lambda path: path == "curated/pipeline_run.json"
    storage.read_json = lambda path: {"pipelines": {PIPELINE_NAME: {"etl_status": "SUCCESS", "validation_status": "FAIL"}}}

    fact = _blind_with_fake_real().get_runtime_health(PIPELINE_NAME, storage)

    assert fact.provenance == "legacy_file"
    assert fact.value["validation_status"] == "FAIL"


def test_build_context_retriever_defaults_to_full_context(monkeypatch):
    monkeypatch.delenv(DEMO_CONTEXT_MODE_ENV_VAR, raising=False)
    assert isinstance(build_context_retriever(), ContextRetriever)


def test_build_context_retriever_full_is_explicit_too(monkeypatch):
    monkeypatch.setenv(DEMO_CONTEXT_MODE_ENV_VAR, "full")
    assert isinstance(build_context_retriever(), ContextRetriever)


def test_build_context_retriever_blind_mode(monkeypatch):
    monkeypatch.setenv(DEMO_CONTEXT_MODE_ENV_VAR, "blind")
    assert isinstance(build_context_retriever(), BlindContextRetriever)
