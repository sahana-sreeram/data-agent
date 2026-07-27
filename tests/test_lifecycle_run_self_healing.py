"""Tests for the diagnose -> apply -> verify composition. The individual stages are
already covered end-to-end (against real S3/Spark where relevant) by
test_lifecycle_apply_repair.py and test_lifecycle_verify_repair.py -- this file only checks
that run_lifecycle_self_healing wires them together correctly, generates a run_id, and
persists all 4 artifacts (run-specific + "latest") -- using monkeypatched stage functions so
it needs no real S3/Spark/model.
"""

from __future__ import annotations

import src.lifecycle_run_self_healing as self_healing_module

PIPELINE_NAME = "loan_portfolio"


class _FakeStorage:
    def __init__(self) -> None:
        self.written: dict = {}

    def read_json(self, path: str) -> dict:
        assert path in ("context/business_rules.json", "context/validations/loan_portfolio.json")
        return {}

    def write_json(self, path: str, value) -> None:
        self.written[path] = value


def test_composes_diagnose_apply_verify_and_returns_combined_dict(monkeypatch):
    calls = []

    monkeypatch.setattr(
        self_healing_module,
        "PIPELINE_REGISTRY",
        {
            PIPELINE_NAME: type(
                "Spec", (), {
                    "validation_rules_key": "context/validations/loan_portfolio.json",
                    "run_validate": staticmethod(lambda storage, br, vr, as_of: calls.append("validate") or {"overall_status": "FAIL", "checks": []}),
                },
            )
        },
    )
    monkeypatch.setattr(
        self_healing_module,
        "run_diagnose_pipeline",
        lambda pipeline_name, storage, factory: calls.append("diagnose") or {"diagnosis_status": "DIAGNOSED"},
    )

    def _fake_apply(pipeline_name, storage, diagnosis, validation_results, factory):
        calls.append("apply")
        assert diagnosis == {"diagnosis_status": "DIAGNOSED"}
        return {"repair_decision": "PROPOSE_REPAIR"}, {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "src/etl_spark_loan_portfolio.py"}

    monkeypatch.setattr(self_healing_module, "run_apply_lifecycle_repair", _fake_apply)

    def _fake_verify(pipeline_name, spark, storage, br, vr, validation_before, repair_result, run_id=None):
        calls.append("verify")
        assert repair_result["repair_status"] == "APPLIED"
        assert run_id is not None
        return {"verification_status": "VERIFIED", "summary": "ok"}

    monkeypatch.setattr(self_healing_module, "run_verify_lifecycle_repair", _fake_verify)

    storage = _FakeStorage()
    result = self_healing_module.run_lifecycle_self_healing(
        PIPELINE_NAME, spark="fake-spark", storage=storage,
        diagnosis_model_client_factory=lambda: None, repair_model_client_factory=lambda: None,
    )

    assert calls == ["validate", "diagnose", "apply", "verify"]
    assert "run_id" in result and len(result["run_id"]) > 0
    assert result["diagnosis"] == {"diagnosis_status": "DIAGNOSED"}
    assert result["repair_plan"] == {"repair_decision": "PROPOSE_REPAIR"}
    assert result["repair_result"]["repair_status"] == "APPLIED"
    assert result["repair_verification"] == {"verification_status": "VERIFIED", "summary": "ok"}

    run_id = result["run_id"]
    for artifact in ("diagnosis", "repair_plan", "repair_result", "repair_verification"):
        run_specific_key = f"curated/self_heal_runs/{PIPELINE_NAME}/{run_id}/{artifact}.json"
        latest_key = f"curated/{PIPELINE_NAME}_{artifact}.json"
        assert run_specific_key in storage.written, f"missing run-specific artifact: {run_specific_key}"
        assert latest_key in storage.written, f"missing latest convenience artifact: {latest_key}"
        assert storage.written[run_specific_key] == storage.written[latest_key]


def test_each_invocation_gets_a_distinct_run_id(monkeypatch):
    monkeypatch.setattr(
        self_healing_module,
        "PIPELINE_REGISTRY",
        {
            PIPELINE_NAME: type(
                "Spec", (), {
                    "validation_rules_key": "context/validations/loan_portfolio.json",
                    "run_validate": staticmethod(lambda storage, br, vr, as_of: {"overall_status": "PASS", "checks": []}),
                },
            )
        },
    )
    monkeypatch.setattr(self_healing_module, "run_diagnose_pipeline", lambda p, s, f: {"diagnosis_status": "NO_INCIDENT"})
    monkeypatch.setattr(
        self_healing_module, "run_apply_lifecycle_repair",
        lambda p, s, d, v, f: ({"repair_decision": "NO_SAFE_REPAIR"}, {"repair_status": "NO_REPAIR", "workspace_dir": None, "target_file": None}),
    )
    monkeypatch.setattr(
        self_healing_module, "run_verify_lifecycle_repair",
        lambda p, spark, s, br, vr, vb, rr, run_id=None: {"verification_status": "BLOCKED", "summary": "nothing to do", "run_id_seen": run_id},
    )

    storage = _FakeStorage()
    result1 = self_healing_module.run_lifecycle_self_healing(PIPELINE_NAME, "fake-spark", storage, lambda: None, lambda: None)
    result2 = self_healing_module.run_lifecycle_self_healing(PIPELINE_NAME, "fake-spark", storage, lambda: None, lambda: None)

    assert result1["run_id"] != result2["run_id"]
