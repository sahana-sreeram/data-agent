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

    def _fake_apply(pipeline_name, storage, diagnosis, validation_results, factory, **kwargs):
        calls.append("apply")
        assert diagnosis == {"diagnosis_status": "DIAGNOSED"}
        return {"repair_decision": "PROPOSE_REPAIR"}, {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "src/etl_spark_loan_portfolio.py"}

    monkeypatch.setattr(self_healing_module, "run_apply_lifecycle_repair", _fake_apply)

    def _fake_verify(pipeline_name, spark, storage, br, vr, validation_before, repair_result, run_id=None, **kwargs):
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


# --- mode parameter --------------------------------------------------------------------------


def _spec_with_validate(run_validate):
    return type("Spec", (), {"validation_rules_key": "context/validations/loan_portfolio.json", "run_validate": staticmethod(run_validate)})


def test_diagnose_only_mode_stops_after_diagnosis(monkeypatch):
    calls = []
    monkeypatch.setattr(self_healing_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _spec_with_validate(lambda *a: {"overall_status": "FAIL", "checks": []})})
    monkeypatch.setattr(self_healing_module, "run_diagnose_pipeline", lambda p, s, f: calls.append("diagnose") or {"diagnosis_status": "DIAGNOSED"})
    monkeypatch.setattr(self_healing_module, "run_apply_lifecycle_repair", lambda *a: (_ for _ in ()).throw(AssertionError("must not apply in diagnose_only mode")))
    monkeypatch.setattr(self_healing_module, "run_verify_lifecycle_repair", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not verify in diagnose_only mode")))

    result = self_healing_module.run_lifecycle_self_healing(
        PIPELINE_NAME, "fake-spark", _FakeStorage(), lambda: None, lambda: None, mode="diagnose_only"
    )

    assert calls == ["diagnose"]
    assert result["diagnosis"] == {"diagnosis_status": "DIAGNOSED"}
    assert result["repair_plan"] is None
    assert result["repair_result"] is None
    assert result["repair_verification"] is None


def test_propose_patch_mode_stops_after_apply(monkeypatch):
    calls = []
    monkeypatch.setattr(self_healing_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _spec_with_validate(lambda *a: {"overall_status": "FAIL", "checks": []})})
    monkeypatch.setattr(self_healing_module, "run_diagnose_pipeline", lambda p, s, f: {"diagnosis_status": "DIAGNOSED"})
    def fake_apply(p, s, d, v, f, **kwargs):
        calls.append("apply")
        return {"repair_decision": "PROPOSE_REPAIR"}, {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "x.py"}

    monkeypatch.setattr(self_healing_module, "run_apply_lifecycle_repair", fake_apply)
    monkeypatch.setattr(self_healing_module, "run_verify_lifecycle_repair", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not verify in propose_patch mode")))

    result = self_healing_module.run_lifecycle_self_healing(
        PIPELINE_NAME, "fake-spark", _FakeStorage(), lambda: None, lambda: None, mode="propose_patch"
    )

    assert result["repair_result"]["repair_status"] == "APPLIED"
    assert result["repair_verification"] is None


def test_create_pr_mode_passes_mode_diagnosis_and_plan_through_to_verify(monkeypatch):
    seen_kwargs = {}
    monkeypatch.setattr(self_healing_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _spec_with_validate(lambda *a: {"overall_status": "FAIL", "checks": []})})
    diagnosis = {"diagnosis_status": "DIAGNOSED", "root_cause_category": "ETL_LOGIC"}
    repair_plan = {"repair_decision": "PROPOSE_REPAIR", "change_summary": "x"}
    monkeypatch.setattr(self_healing_module, "run_diagnose_pipeline", lambda p, s, f: diagnosis)
    monkeypatch.setattr(
        self_healing_module,
        "run_apply_lifecycle_repair",
        lambda p, s, d, v, f, **k: (repair_plan, {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "x.py"}),
    )

    def fake_verify(pipeline_name, spark, storage, br, vr, vb, rr, **kwargs):
        seen_kwargs.update(kwargs)
        return {"verification_status": "VERIFIED_PENDING_PR", "pr_artifact": {"branch": "repair/abc"}}

    monkeypatch.setattr(self_healing_module, "run_verify_lifecycle_repair", fake_verify)

    result = self_healing_module.run_lifecycle_self_healing(
        PIPELINE_NAME, "fake-spark", _FakeStorage(), lambda: None, lambda: None, mode="create_pr"
    )

    assert seen_kwargs["mode"] == "create_pr"
    assert seen_kwargs["diagnosis"] == diagnosis
    assert seen_kwargs["repair_plan"] == repair_plan
    assert result["repair_verification"]["verification_status"] == "VERIFIED_PENDING_PR"

    from src.sandbox.backend import GitWorktreeSandbox

    assert isinstance(seen_kwargs["sandbox_backend"], GitWorktreeSandbox)


def test_auto_promote_is_the_default_and_never_passes_mode_kwargs_to_verify(monkeypatch):
    """The single most important regression guard in this file: every pre-existing call site
    (and every other test in this file) calls run_lifecycle_self_healing without `mode` at
    all -- verify must receive exactly the same kwargs it always did (run_id, plus a
    TempDirSandbox sandbox_backend -- byte-identical to the original behavior that
    predates sandbox_backend existing at all), never mode/diagnosis/repair_plan."""
    seen_kwargs = {}
    monkeypatch.setattr(self_healing_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _spec_with_validate(lambda *a: {"overall_status": "FAIL", "checks": []})})
    monkeypatch.setattr(self_healing_module, "run_diagnose_pipeline", lambda p, s, f: {"diagnosis_status": "DIAGNOSED"})
    monkeypatch.setattr(
        self_healing_module,
        "run_apply_lifecycle_repair",
        lambda p, s, d, v, f, **k: ({"repair_decision": "PROPOSE_REPAIR"}, {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "x.py"}),
    )

    def fake_verify(pipeline_name, spark, storage, br, vr, vb, rr, **kwargs):
        seen_kwargs.update(kwargs)
        return {"verification_status": "VERIFIED"}

    monkeypatch.setattr(self_healing_module, "run_verify_lifecycle_repair", fake_verify)

    self_healing_module.run_lifecycle_self_healing(PIPELINE_NAME, "fake-spark", _FakeStorage(), lambda: None, lambda: None)

    assert set(seen_kwargs) == {"run_id", "sandbox_backend"}
    from src.sandbox.backend import TempDirSandbox

    assert isinstance(seen_kwargs["sandbox_backend"], TempDirSandbox)


def test_unknown_mode_raises_value_error():
    import pytest

    with pytest.raises(ValueError):
        self_healing_module.run_lifecycle_self_healing(PIPELINE_NAME, "fake-spark", _FakeStorage(), lambda: None, lambda: None, mode="not_a_real_mode")


def test_human_approved_categories_requires_create_pr_mode():
    import pytest

    with pytest.raises(ValueError):
        self_healing_module.run_lifecycle_self_healing(
            PIPELINE_NAME, "fake-spark", _FakeStorage(), lambda: None, lambda: None,
            mode="auto_promote", human_approved_categories=frozenset({"SOURCE_CONTRACT_CHANGE"}),
        )


def test_human_approved_categories_is_threaded_through_to_apply_repair(monkeypatch):
    seen_kwargs = {}
    monkeypatch.setattr(self_healing_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: _spec_with_validate(lambda *a: {"overall_status": "FAIL", "checks": []})})
    monkeypatch.setattr(self_healing_module, "run_diagnose_pipeline", lambda p, s, f: {"diagnosis_status": "DIAGNOSED", "root_cause_category": "SOURCE_CONTRACT_CHANGE"})

    def fake_apply(p, s, d, v, f, **kwargs):
        seen_kwargs.update(kwargs)
        return {"repair_decision": "PROPOSE_REPAIR"}, {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "x.py"}

    monkeypatch.setattr(self_healing_module, "run_apply_lifecycle_repair", fake_apply)
    monkeypatch.setattr(self_healing_module, "run_verify_lifecycle_repair", lambda *a, **k: {"verification_status": "VERIFIED_PENDING_PR"})

    self_healing_module.run_lifecycle_self_healing(
        PIPELINE_NAME, "fake-spark", _FakeStorage(), lambda: None, lambda: None,
        mode="create_pr", human_approved_categories=frozenset({"SOURCE_CONTRACT_CHANGE"}),
    )

    assert seen_kwargs["human_approved_categories"] == frozenset({"SOURCE_CONTRACT_CHANGE"})


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
        lambda p, s, d, v, f, **k: ({"repair_decision": "NO_SAFE_REPAIR"}, {"repair_status": "NO_REPAIR", "workspace_dir": None, "target_file": None}),
    )
    monkeypatch.setattr(
        self_healing_module, "run_verify_lifecycle_repair",
        lambda p, spark, s, br, vr, vb, rr, run_id=None, **k: {"verification_status": "BLOCKED", "summary": "nothing to do", "run_id_seen": run_id},
    )

    storage = _FakeStorage()
    result1 = self_healing_module.run_lifecycle_self_healing(PIPELINE_NAME, "fake-spark", storage, lambda: None, lambda: None)
    result2 = self_healing_module.run_lifecycle_self_healing(PIPELINE_NAME, "fake-spark", storage, lambda: None, lambda: None)

    assert result1["run_id"] != result2["run_id"]
