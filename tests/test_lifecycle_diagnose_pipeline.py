"""Tests for src/lifecycle_diagnose_pipeline.py -- no dedicated test file existed for this
module before (it was only exercised indirectly, with the whole function monkeypatched, by
tests/test_lifecycle_run_self_healing.py and tests/test_eval_harness.py). This file tests its
real internal branching logic directly: curated-validation-fails (existing behavior),
curated-and-raw-both-pass (existing behavior), and curated-passes-but-raw-fails (new --
added for the upstream-contract-change scenario, see src/eval_scenarios.py's
UPSTREAM_CONTRACT_SCENARIOS). No real S3/Spark/model calls.
"""

from __future__ import annotations

import src.lifecycle_diagnose_pipeline as diagnose_module
from src.context_retriever import ContextRetriever

PIPELINE_NAME = "payment_performance"


class _FakeStorage:
    def __init__(self, raw_tables: dict | None = None, raise_on_raw_read: bool = False) -> None:
        self.raw_tables = raw_tables or {}
        self.raise_on_raw_read = raise_on_raw_read
        self.json: dict = {
            "context/business_rules.json": {},
            "context/validations/payment_performance.json": {},
            "context/validations/lifecycle_raw.json": {},
        }

    def read_json(self, path: str):
        return self.json[path]

    def read_parquet(self, path: str):
        if self.raise_on_raw_read:
            raise RuntimeError("raw table unavailable")
        table_name = path.removeprefix("raw/").removesuffix(".parquet")
        return self.raw_tables[table_name]


def _fake_spec(run_validate):
    return type(
        "Spec",
        (),
        {
            "run_validate": staticmethod(run_validate),
            "validation_rules_key": "context/validations/payment_performance.json",
            "metrics_key": "context/metrics/payment_performance.json",
            "etl_source_file": "src/etl_spark_payment_performance.py",
            "raw_tables": ("payment_schedule", "payment_events"),
        },
    )()


def test_curated_pass_and_raw_pass_returns_no_incident_without_calling_the_model(monkeypatch):
    monkeypatch.setattr(
        diagnose_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _fake_spec(lambda *a: {"overall_status": "PASS", "checks": []})}
    )
    monkeypatch.setattr(diagnose_module, "validate_lifecycle_raw", lambda tables, br, vr: {"overall_status": "PASS", "checks": []})
    monkeypatch.setattr(diagnose_module, "TABLE_FILENAMES", {"payment_schedule": "x", "payment_events": "y"})

    def boom():
        raise AssertionError("should never construct a model client for a genuinely healthy pipeline")

    result = diagnose_module.run_diagnose_pipeline(PIPELINE_NAME, _FakeStorage({"payment_schedule": None, "payment_events": None}), boom)
    assert result["diagnosis_status"] == "NO_INCIDENT"


def test_curated_pass_but_raw_unavailable_returns_no_incident_defensively(monkeypatch):
    monkeypatch.setattr(diagnose_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _fake_spec(lambda *a: {"overall_status": "PASS", "checks": []})})
    monkeypatch.setattr(diagnose_module, "TABLE_FILENAMES", {"payment_schedule": "x"})

    result = diagnose_module.run_diagnose_pipeline(
        PIPELINE_NAME, _FakeStorage(raise_on_raw_read=True), lambda: (_ for _ in ()).throw(AssertionError("no model call expected"))
    )
    assert result["diagnosis_status"] == "NO_INCIDENT"


def test_curated_fail_investigates_directly_and_never_checks_raw_validation(monkeypatch):
    calls: list = []
    monkeypatch.setattr(
        diagnose_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _fake_spec(lambda *a: {"overall_status": "FAIL", "checks": [{"id": "x", "status": "FAIL"}]})}
    )

    def raw_check_should_not_run(*a, **k):
        raise AssertionError("curated validation already failed -- must not also check raw validation")

    monkeypatch.setattr(diagnose_module, "validate_lifecycle_raw", raw_check_should_not_run)
    monkeypatch.setattr(diagnose_module, "build_diagnostic_tools_for_pipeline", lambda *a, **k: calls.append("build_tools") or type("Tools", (), {"metrics": {}})())
    monkeypatch.setattr(diagnose_module, "_build_starting_context", lambda validation_results: calls.append("starting_context") or {})
    monkeypatch.setattr(
        diagnose_module,
        "run_lifecycle_diagnosis",
        lambda *a, **k: calls.append("run_diagnosis") or type("R", (), {"diagnosis_status": type("S", (), {"value": "DIAGNOSED"})()})(),
    )
    monkeypatch.setattr(diagnose_module, "diagnosis_to_dict", lambda result: {"diagnosis_status": "DIAGNOSED"})

    result = diagnose_module.run_diagnose_pipeline(PIPELINE_NAME, _FakeStorage(), lambda: "fake-client")
    assert calls == ["build_tools", "starting_context", "run_diagnosis"]
    assert result["diagnosis_status"] == "DIAGNOSED"


def test_curated_pass_but_raw_fail_uses_raw_checks_as_diagnosis_evidence(monkeypatch):
    """The upstream-contract-change case: a genuine data problem (SETTLED not in the approved
    enum) that curated reconciliation cannot see because the ETL and its validator apply the
    same filter to the same data and agree with each other."""
    raw_failure = {"overall_status": "FAIL", "checks": [{"id": "payment_events_payment_status_enum_valid", "status": "FAIL", "actual": ["SETTLED"]}]}
    seen_validation_results: list = []

    monkeypatch.setattr(
        diagnose_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _fake_spec(lambda *a: {"overall_status": "PASS", "checks": []})}
    )
    monkeypatch.setattr(diagnose_module, "validate_lifecycle_raw", lambda tables, br, vr: raw_failure)
    monkeypatch.setattr(diagnose_module, "TABLE_FILENAMES", {"payment_schedule": "x", "payment_events": "y"})
    monkeypatch.setattr(diagnose_module, "build_diagnostic_tools_for_pipeline", lambda pipeline_name, storage, validation_results, business_rules, **k: seen_validation_results.append(validation_results) or type("Tools", (), {"metrics": {}})())
    monkeypatch.setattr(diagnose_module, "_build_starting_context", lambda validation_results: seen_validation_results.append(validation_results) or {})
    monkeypatch.setattr(
        diagnose_module,
        "run_lifecycle_diagnosis",
        lambda *a, **k: type("R", (), {})(),
    )
    monkeypatch.setattr(diagnose_module, "diagnosis_to_dict", lambda result: {"diagnosis_status": "DIAGNOSED", "root_cause_category": "SOURCE_CONTRACT_CHANGE"})

    result = diagnose_module.run_diagnose_pipeline(
        PIPELINE_NAME, _FakeStorage({"payment_schedule": None, "payment_events": None}), lambda: "fake-client"
    )

    assert result["root_cause_category"] == "SOURCE_CONTRACT_CHANGE"
    # both the tool-builder and the starting context got the RAW validator's failed checks,
    # not an empty/passing curated result
    assert all(vr == raw_failure for vr in seen_validation_results)


def test_run_diagnose_pipeline_respects_demo_context_mode(monkeypatch):
    """DEMO_CONTEXT_MODE=blind (see src/context_retriever.py) must reach the inner diagnosis
    agent's own tools, not just the outer MCP data-ops tools -- this is what
    create_candidate_repair's diagnosis actually runs on."""
    from src.context_retriever import BlindContextRetriever

    seen_context_retrievers: list = []
    seen_blind_flags: list = []

    monkeypatch.setattr(
        diagnose_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _fake_spec(lambda *a: {"overall_status": "FAIL", "checks": [{"id": "x", "status": "FAIL"}]})}
    )
    monkeypatch.setattr(diagnose_module, "validate_lifecycle_raw", lambda *a: (_ for _ in ()).throw(AssertionError("should not run")))

    def fake_build_tools(pipeline_name, storage, validation_results, business_rules, context_retriever=None, blind_raw_context=False):
        seen_context_retrievers.append(context_retriever)
        seen_blind_flags.append(blind_raw_context)
        return type("Tools", (), {"metrics": {}})()

    monkeypatch.setattr(diagnose_module, "build_diagnostic_tools_for_pipeline", fake_build_tools)
    monkeypatch.setattr(diagnose_module, "_build_starting_context", lambda validation_results: {})
    monkeypatch.setattr(diagnose_module, "run_lifecycle_diagnosis", lambda *a, **k: type("R", (), {})())
    monkeypatch.setattr(diagnose_module, "diagnosis_to_dict", lambda result: {"diagnosis_status": "DIAGNOSED"})

    monkeypatch.delenv("DEMO_CONTEXT_MODE", raising=False)
    diagnose_module.run_diagnose_pipeline(PIPELINE_NAME, _FakeStorage(), lambda: "fake-client")
    assert isinstance(seen_context_retrievers[-1], ContextRetriever)
    assert seen_blind_flags[-1] is False

    monkeypatch.setenv("DEMO_CONTEXT_MODE", "blind")
    diagnose_module.run_diagnose_pipeline(PIPELINE_NAME, _FakeStorage(), lambda: "fake-client")
    assert isinstance(seen_context_retrievers[-1], BlindContextRetriever)
    assert seen_blind_flags[-1] is True
