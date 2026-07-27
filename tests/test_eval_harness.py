"""Tests for src/eval_harness.py. No live model/Spark/S3 calls: InstrumentedModelClient and
run_refusal_accuracy_suite are exercised directly against ScriptedDiagnosisModelClient and
the real (deterministic) repair-eligibility gate; run_bug_scenario's orchestration is
exercised against a throwaway on-disk fixture file and monkeypatched stage functions,
mirroring tests/test_lifecycle_run_self_healing.py's own monkeypatched-composition style.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import src.eval_harness as eval_harness_module
from src.eval_scenarios import BugScenario
from src.model_client import ModelClientError, ModelResponse, ScriptedDiagnosisModelClient, ToolCall

PIPELINE_NAME = "fake_pipeline"


# --- InstrumentedModelClient ---------------------------------------------------------------


def test_instrumented_model_client_records_turns_and_tool_calls_across_multiple_sends():
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="a", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="b", arguments={}), ToolCall(id="3", name="c", arguments={})]),
    ]
    client = eval_harness_module.InstrumentedModelClient(ScriptedDiagnosisModelClient(responses))
    client.send([], [])
    client.send([], [])
    assert client.stats.turns_used == 2
    assert client.stats.tool_calls_used == 3
    assert client.stats.total_latency_seconds >= 0.0


def test_instrumented_model_client_propagates_errors_without_counting_the_failed_turn():
    client = eval_harness_module.InstrumentedModelClient(ScriptedDiagnosisModelClient([]))
    with pytest.raises(ModelClientError):
        client.send([], [])
    assert client.stats.turns_used == 0
    assert client.stats.tool_calls_used == 0


# --- _ensure_spark_session --------------------------------------------------------------
#
# Regression coverage for a real bug found on this harness's first live run:
# run_verify_lifecycle_repair's targeted-test rerun invokes pytest.main() in-process, and if
# the pipeline's test file pulls in tests/conftest.py's session-scoped `spark_session`
# fixture, that fixture's teardown stops the one shared local[*] SparkContext -- killing
# every subsequent Spark operation in this same process, including this harness's own
# restore-and-reheal rerun and every scenario after it.


def _fake_spark(stopped: bool):
    return SimpleNamespace(
        sparkContext=SimpleNamespace(_jsc=SimpleNamespace(sc=lambda: SimpleNamespace(isStopped=lambda: stopped)))
    )


def test_ensure_spark_session_returns_the_same_session_when_still_alive(monkeypatch):
    def boom(app_name):
        raise AssertionError("should not fetch a new session when the current one is still alive")

    monkeypatch.setattr(eval_harness_module, "get_spark_session", boom)
    spark = _fake_spark(stopped=False)
    assert eval_harness_module._ensure_spark_session(spark) is spark


def test_ensure_spark_session_recovers_a_fresh_session_when_stopped(monkeypatch):
    fresh = SimpleNamespace(sparkContext=SimpleNamespace(setLogLevel=lambda level: None))
    monkeypatch.setattr(eval_harness_module, "get_spark_session", lambda app_name: fresh)
    spark = _fake_spark(stopped=True)
    assert eval_harness_module._ensure_spark_session(spark) is fresh


def test_ensure_spark_session_treats_an_inspection_failure_as_stopped(monkeypatch):
    fresh = SimpleNamespace(sparkContext=SimpleNamespace(setLogLevel=lambda level: None))
    monkeypatch.setattr(eval_harness_module, "get_spark_session", lambda app_name: fresh)
    broken_spark = SimpleNamespace(sparkContext=SimpleNamespace(_jsc=None))  # ._jsc.sc() raises AttributeError
    assert eval_harness_module._ensure_spark_session(broken_spark) is fresh


# --- run_refusal_accuracy_suite ------------------------------------------------------------


def test_run_refusal_accuracy_suite_scores_100_percent_against_the_real_gate():
    report = eval_harness_module.run_refusal_accuracy_suite()
    assert report["accuracy"] == 1.0
    assert all(case["correct"] for case in report["cases"])
    assert report["cases"]


# --- run_bug_scenario orchestration ---------------------------------------------------------


class _FakeStorage:
    def __init__(self) -> None:
        self.written_parquet: dict = {}
        self.written_json: dict = {}

    def read_json(self, path: str):
        if path in ("context/business_rules.json", "context/validations/fake.json"):
            return {}
        raise AssertionError(f"unexpected read_json path: {path}")

    def write_parquet(self, path: str, df) -> None:
        self.written_parquet[path] = df

    def write_json(self, path: str, value) -> None:
        self.written_json[path] = value

    def exists(self, path: str) -> bool:
        return path in self.written_json


def _make_scenario(tmp_path) -> tuple[BugScenario, Path]:
    target = tmp_path / "fake_etl.py"
    target.write_text("MARKER = 'clean'\n")
    scenario = BugScenario(
        name="fake_scenario",
        pipeline_name=PIPELINE_NAME,
        bug_class="ETL_LOGIC_JOIN",
        target_file=str(target),
        find="MARKER = 'clean'",
        replace="MARKER = 'buggy'",
        expected_root_cause_category="ETL_LOGIC",
        description="throwaway fixture, not a real lifecycle pipeline",
    )
    return scenario, target


def test_run_bug_scenario_injects_diagnoses_repairs_verifies_and_always_restores(tmp_path, monkeypatch):
    scenario, target = _make_scenario(tmp_path)
    original_source = target.read_text()
    calls: list = []
    etl_contents_seen: list = []

    def fake_run_etl(etl_module, spark, business_rules, as_of_date):
        calls.append("run_etl")
        etl_contents_seen.append(target.read_text())
        return {"curated/fake.parquet": "df-stand-in"}

    def fake_run_validate(storage, business_rules, validation_rules, as_of_date):
        calls.append("run_validate")
        return {"overall_status": "PASS", "checks": []}

    fake_spec = type(
        "Spec", (), {"run_etl": staticmethod(fake_run_etl), "run_validate": staticmethod(fake_run_validate), "validation_rules_key": "context/validations/fake.json"}
    )()
    monkeypatch.setattr(eval_harness_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: fake_spec})
    monkeypatch.setattr(eval_harness_module, "_reload_etl_module", lambda target_file: "fake-module")
    monkeypatch.setattr(eval_harness_module, "_ensure_spark_session", lambda spark: spark)

    def fake_diagnose(pipeline_name, storage, factory):
        calls.append("diagnose")
        factory().send([], [])  # exercise the instrumented-client factory wiring
        return {"diagnosis_status": "DIAGNOSED", "root_cause_category": "ETL_LOGIC", "confidence": "HIGH"}

    monkeypatch.setattr(eval_harness_module, "run_diagnose_pipeline", fake_diagnose)

    def fake_apply(pipeline_name, storage, diagnosis, validation_before, factory):
        calls.append("apply")
        return {"repair_decision": "PROPOSE_REPAIR"}, {
            "repair_status": "APPLIED",
            "workspace_dir": "/tmp/x",
            "target_file": scenario.target_file,
        }

    monkeypatch.setattr(eval_harness_module, "run_apply_lifecycle_repair", fake_apply)

    def fake_verify(pipeline_name, spark, storage, business_rules, validation_rules, validation_before, repair_result, run_id=None):
        calls.append("verify")
        assert run_id is not None
        return {"verification_status": "VERIFIED"}

    monkeypatch.setattr(eval_harness_module, "run_verify_lifecycle_repair", fake_verify)
    monkeypatch.setattr(
        eval_harness_module, "_persist_run_artifacts", lambda storage, pipeline_name, run_id, artifacts: calls.append("persist")
    )

    diagnosis_client_factory = lambda: ScriptedDiagnosisModelClient(  # noqa: E731
        [ModelResponse(tool_calls=[ToolCall(id="1", name="x", arguments={})])]
    )

    result = eval_harness_module.run_bug_scenario(scenario, "fake-spark", _FakeStorage(), diagnosis_client_factory, lambda: None)

    assert calls == ["run_etl", "diagnose", "run_validate", "apply", "verify", "persist", "run_etl", "run_validate"]
    assert etl_contents_seen[0] == original_source.replace(scenario.find, scenario.replace, 1)
    assert etl_contents_seen[1] == original_source
    assert target.read_text() == original_source

    assert result["diagnosis"]["status"] == "DIAGNOSED"
    assert result["diagnosis"]["matches_expected"] is True
    assert result["diagnosis"]["turns_used"] == 1
    assert result["diagnosis"]["tool_calls_used"] == 1
    assert result["repair"]["repair_status"] == "APPLIED"
    assert result["verify"]["verification_status"] == "VERIFIED"
    assert result["verify"]["promoted"] is True
    assert result["end_to_end_latency_seconds"] >= 0.0


def test_run_bug_scenario_restores_file_even_when_a_stage_raises(tmp_path, monkeypatch):
    scenario, target = _make_scenario(tmp_path)
    original_source = target.read_text()

    fake_spec = type(
        "Spec",
        (),
        {
            "run_etl": staticmethod(lambda etl_module, spark, business_rules, as_of_date: {"curated/fake.parquet": "df"}),
            "run_validate": staticmethod(lambda storage, business_rules, validation_rules, as_of_date: {"overall_status": "PASS", "checks": []}),
            "validation_rules_key": "context/validations/fake.json",
        },
    )()
    monkeypatch.setattr(eval_harness_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: fake_spec})
    monkeypatch.setattr(eval_harness_module, "_reload_etl_module", lambda target_file: "fake-module")
    monkeypatch.setattr(eval_harness_module, "_ensure_spark_session", lambda spark: spark)
    monkeypatch.setattr(
        eval_harness_module,
        "run_diagnose_pipeline",
        lambda p, s, f: {"diagnosis_status": "DIAGNOSED", "root_cause_category": "ETL_LOGIC", "confidence": "HIGH"},
    )

    def boom(*args, **kwargs):
        raise RuntimeError("repair model exploded")

    monkeypatch.setattr(eval_harness_module, "run_apply_lifecycle_repair", boom)

    with pytest.raises(RuntimeError):
        eval_harness_module.run_bug_scenario(
            scenario, "fake-spark", _FakeStorage(), lambda: ScriptedDiagnosisModelClient([]), lambda: None
        )

    assert target.read_text() == original_source


def test_run_bug_scenario_raises_eval_scenario_error_when_find_string_is_not_unique(tmp_path, monkeypatch):
    target = tmp_path / "dup_etl.py"
    target.write_text("X = 1\nX = 1\n")
    scenario = BugScenario(
        name="dup",
        pipeline_name=PIPELINE_NAME,
        bug_class="ETL_LOGIC_JOIN",
        target_file=str(target),
        find="X = 1",
        replace="X = 2",
        expected_root_cause_category="ETL_LOGIC",
        description="duplicate-find fixture",
    )
    monkeypatch.setattr(eval_harness_module, "PIPELINE_REGISTRY", {PIPELINE_NAME: object()})
    monkeypatch.setattr(eval_harness_module, "_ensure_spark_session", lambda spark: spark)

    with pytest.raises(eval_harness_module.EvalScenarioError):
        eval_harness_module.run_bug_scenario(scenario, "fake-spark", _FakeStorage(), lambda: None, lambda: None)

    assert target.read_text() == "X = 1\nX = 1\n"
