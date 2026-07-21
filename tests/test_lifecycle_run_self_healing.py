"""Tests for the diagnose -> apply -> verify composition. The individual stages are
already covered end-to-end (against real S3/Spark where relevant) by
test_lifecycle_apply_repair.py and test_lifecycle_verify_repair.py -- this file only
checks that run_lifecycle_self_healing wires them together correctly and returns the
combined dict, using monkeypatched stage functions so it needs no real S3/Spark/model.
"""

from __future__ import annotations

import src.lifecycle_run_self_healing as self_healing_module


class _FakeStorage:
    def read_json(self, path: str) -> dict:
        assert path in ("context/business_rules.json", "context/validations/loan_portfolio.json")
        return {}


def test_composes_diagnose_apply_verify_and_returns_combined_dict(monkeypatch):
    calls = []

    monkeypatch.setattr(
        self_healing_module, "validate_loan_portfolio", lambda storage, br, vr: calls.append("validate") or {"overall_status": "FAIL", "checks": []}
    )
    monkeypatch.setattr(
        self_healing_module,
        "run_diagnose_loan_portfolio",
        lambda storage, factory: calls.append("diagnose") or {"diagnosis_status": "DIAGNOSED"},
    )

    def _fake_apply(storage, diagnosis, validation_results, factory):
        calls.append("apply")
        assert diagnosis == {"diagnosis_status": "DIAGNOSED"}
        return {"repair_decision": "PROPOSE_REPAIR"}, {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "src/etl_spark_loan_portfolio.py"}

    monkeypatch.setattr(self_healing_module, "run_apply_lifecycle_repair", _fake_apply)

    def _fake_verify(spark, storage, br, vr, validation_before, repair_result):
        calls.append("verify")
        assert repair_result["repair_status"] == "APPLIED"
        return {"verification_status": "VERIFIED", "summary": "ok"}

    monkeypatch.setattr(self_healing_module, "run_verify_lifecycle_repair", _fake_verify)

    result = self_healing_module.run_lifecycle_self_healing(
        spark="fake-spark", storage=_FakeStorage(), diagnosis_model_client_factory=lambda: None, repair_model_client_factory=lambda: None
    )

    assert calls == ["validate", "diagnose", "apply", "verify"]
    assert result == {
        "diagnosis": {"diagnosis_status": "DIAGNOSED"},
        "repair_plan": {"repair_decision": "PROPOSE_REPAIR"},
        "repair_result": {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "src/etl_spark_loan_portfolio.py"},
        "repair_verification": {"verification_status": "VERIFIED", "summary": "ok"},
    }
