"""Tests for src/platform_backends/* and src/config.py's env-var-driven factories.

LocalSparkRunner/LocalRuntimeInspector/FileStateStore are exercised against real local
Spark/S3 (skip cleanly via conftest's s3_storage/spark_session fixtures, matching every other
integration test in this repo). RHOAISparkRunner/SparkHistoryRuntimeInspector/RedisStateStore
are exercised against injected fakes only -- no real kubernetes/redis package import needed to
run these, no real cluster/HTTP/Redis calls.
"""

from __future__ import annotations

import os

import pytest

from src.platform_backends.pipeline_runner import LocalSparkRunner, RHOAISparkRunner
from src.platform_backends.runtime_inspector import LocalRuntimeInspector, SparkHistoryRuntimeInspector
from src.platform_backends.state_store import FileStateStore, RedisStateStore


# --- LocalSparkRunner --------------------------------------------------------------------


def test_local_spark_runner_submits_loan_portfolio(s3_storage, spark_session):
    runner = LocalSparkRunner(storage=s3_storage)
    handle = runner.submit("loan_portfolio")
    assert handle.backend == "local"
    assert handle.pipeline_name == "loan_portfolio"

    status = runner.get_status(handle)
    assert status["status"] == "SUCCEEDED"
    assert status["etl_status"] == "SUCCESS"
    assert status["validation"] is not None

    awaited = runner.await_completion(handle)
    assert awaited == status


def test_local_spark_runner_unknown_pipeline_raises(s3_storage):
    runner = LocalSparkRunner(storage=s3_storage)
    with pytest.raises(ValueError):
        runner.submit("not_a_real_pipeline")


def test_local_spark_runner_unknown_handle_status_is_unknown(s3_storage):
    runner = LocalSparkRunner(storage=s3_storage)
    from src.platform_backends.pipeline_runner import RunHandle

    fake_handle = RunHandle(pipeline_name="loan_portfolio", run_id="does-not-exist", backend="local")
    assert runner.get_status(fake_handle) == {"status": "UNKNOWN"}


# --- RHOAISparkRunner (fake k8s client, no real cluster) ---------------------------------


class _FakeK8sClient:
    def __init__(self) -> None:
        self.created: list[dict] = []
        self.state = "COMPLETED"

    def create_namespaced_custom_object(self, group, version, namespace, plural, body):
        self.created.append(body)
        return body

    def get_namespaced_custom_object(self, group, version, namespace, plural, name):
        return {"status": {"applicationState": {"state": self.state}}}


def test_rhoai_spark_runner_submits_spark_application_manifest():
    fake_client = _FakeK8sClient()
    runner = RHOAISparkRunner(namespace="data-agent-demo", image="quay.io/example/data-agent:test", k8s_client=fake_client)

    handle = runner.submit("loan_portfolio")

    assert handle.backend == "rhoai"
    assert handle.backend_ref["namespace"] == "data-agent-demo"
    assert len(fake_client.created) == 1
    manifest = fake_client.created[0]
    assert manifest["kind"] == "SparkApplication"
    assert manifest["apiVersion"] == "sparkoperator.k8s.io/v1beta2"
    assert manifest["spec"]["mainApplicationFile"] == "local:///opt/spark-app/src/etl_spark_loan_portfolio.py"
    assert manifest["spec"]["sparkConf"]["spark.eventLog.enabled"] == "true"


def test_rhoai_spark_runner_get_status_reads_application_state():
    fake_client = _FakeK8sClient()
    fake_client.state = "FAILED"
    runner = RHOAISparkRunner(k8s_client=fake_client)
    handle = runner.submit("loan_portfolio")

    status = runner.get_status(handle)

    assert status["status"] == "FAILED"


def test_rhoai_spark_runner_unknown_pipeline_raises():
    runner = RHOAISparkRunner(k8s_client=_FakeK8sClient())
    with pytest.raises(ValueError):
        runner.submit("not_a_real_pipeline")


# --- LocalRuntimeInspector ----------------------------------------------------------------


def test_local_runtime_inspector_scopes_to_run_id_job_group(spark_session):
    run_id = "test-run-abc123"
    spark_session.sparkContext.setJobGroup(run_id, "test")
    spark_session.range(1000).count()  # triggers one real job tagged with run_id
    spark_session.sparkContext.setJobGroup(None, None)  # reset job group for later tests

    inspector = LocalRuntimeInspector(spark=spark_session)
    summary = inspector.get_run_summary(run_id)

    assert summary["run_id"] == run_id
    assert summary["job_count"] >= 1
    assert summary["overall_status"] == "SUCCEEDED"


def test_local_runtime_inspector_unknown_run_id_has_zero_jobs(spark_session):
    inspector = LocalRuntimeInspector(spark=spark_session)
    summary = inspector.get_run_summary("no-such-run-id")
    assert summary["job_count"] == 0
    assert summary["overall_status"] == "UNKNOWN"


def test_local_runtime_inspector_pod_status_and_log_are_local_stubs(spark_session):
    inspector = LocalRuntimeInspector(spark=spark_session)
    assert inspector.get_pod_status("any-run")["available"] is False
    assert "local mode" in inspector.get_driver_log_excerpt("any-run")


# --- SparkHistoryRuntimeInspector (fake HTTP client, no real History Server) --------------


class _FakeHistoryServerClient:
    def __init__(self) -> None:
        self.applications = {"run-1": {"attempts": [{"completed": True}]}}
        self.stages = {"run-1": [{"status": "COMPLETE"}, {"status": "FAILED", "stageId": 2}]}
        self.log_lines = [f"line {i}" for i in range(1000)]

    def get_application(self, run_id: str) -> dict:
        return self.applications[run_id]

    def get_stages(self, run_id: str) -> list[dict]:
        return self.stages[run_id]

    def get_executor_log(self, run_id: str, executor_id: str, log_type: str) -> str:
        return "\n".join(self.log_lines)


def test_spark_history_runtime_inspector_get_run_summary():
    inspector = SparkHistoryRuntimeInspector(client=_FakeHistoryServerClient())
    summary = inspector.get_run_summary("run-1")
    assert summary["overall_status"] == "SUCCEEDED"


def test_spark_history_runtime_inspector_get_failed_stages():
    inspector = SparkHistoryRuntimeInspector(client=_FakeHistoryServerClient())
    failed = inspector.get_failed_stages("run-1")
    assert len(failed) == 1
    assert failed[0]["stageId"] == 2


def test_spark_history_runtime_inspector_truncates_driver_log():
    inspector = SparkHistoryRuntimeInspector(client=_FakeHistoryServerClient(), truncate_at=50)
    excerpt = inspector.get_driver_log_excerpt("run-1", max_lines=500)
    assert len(excerpt.splitlines()) == 50
    assert excerpt.splitlines()[-1] == "line 999"


# --- FileStateStore -----------------------------------------------------------------------


def test_file_state_store_round_trip(s3_storage):
    store = FileStateStore(storage=s3_storage)
    key = "test/platform-backends-round-trip"
    store.delete(key)

    assert store.get(key) is None

    store.set(key, {"status": "IN_PROGRESS", "run_id": "abc"})
    assert store.get(key) == {"status": "IN_PROGRESS", "run_id": "abc"}

    store.set(key, {"status": "DONE"})
    assert store.get(key) == {"status": "DONE"}

    store.delete(key)
    assert store.get(key) is None


def test_file_state_store_list_keys(s3_storage):
    store = FileStateStore(storage=s3_storage)
    store.set("test/list-keys/one", {"a": 1})
    store.set("test/list-keys/two", {"a": 2})

    keys = store.list_keys("test/list-keys/")

    assert "test/list-keys/one" in keys
    assert "test/list-keys/two" in keys

    store.delete("test/list-keys/one")
    store.delete("test/list-keys/two")


# --- RedisStateStore (fake redis client, no real Redis needed) ---------------------------


class _FakeRedis:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value

    def delete(self, key):
        self._data.pop(key, None)

    def keys(self, pattern):
        prefix = pattern.rstrip("*")
        return [k for k in self._data if k.startswith(prefix)]


def test_redis_state_store_round_trip():
    store = RedisStateStore(client=_FakeRedis())
    store.set("run/1", {"status": "PENDING"})
    assert store.get("run/1") == {"status": "PENDING"}
    store.delete("run/1")
    assert store.get("run/1") is None


def test_redis_state_store_list_keys():
    store = RedisStateStore(client=_FakeRedis())
    store.set("run/1", {"a": 1})
    store.set("run/2", {"a": 2})
    keys = store.list_keys("run/")
    assert sorted(keys) == ["run/1", "run/2"]


# --- src/config.py factories ---------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_backend_env(monkeypatch):
    for var in ("EXECUTION_BACKEND", "RUNTIME_BACKEND", "STATE_BACKEND", "AGENT_HARNESS"):
        monkeypatch.delenv(var, raising=False)


def test_config_defaults_to_local_backends():
    import src.config as config

    assert config.execution_backend() == "local"
    assert config.runtime_backend() == "local"
    assert config.state_backend() == "file"
    assert config.agent_harness() == "current"

    assert isinstance(config.get_pipeline_runner(), LocalSparkRunner)
    assert isinstance(config.get_runtime_inspector(), LocalRuntimeInspector)
    assert isinstance(config.get_state_store(), FileStateStore)


def test_config_selects_rhoai_and_spark_history_and_redis_backends(monkeypatch):
    import src.config as config

    monkeypatch.setenv("EXECUTION_BACKEND", "rhoai")
    monkeypatch.setenv("RUNTIME_BACKEND", "spark_history")
    monkeypatch.setenv("STATE_BACKEND", "redis")
    monkeypatch.setenv("AGENT_HARNESS", "codex_mcp")

    assert config.agent_harness() == "codex_mcp"
    assert isinstance(config.get_pipeline_runner(), RHOAISparkRunner)
    assert isinstance(config.get_runtime_inspector(), SparkHistoryRuntimeInspector)
    # RedisStateStore's client is built lazily -- constructing it here would require a real
    # `redis` package + server, neither of which this test should need.
    from src.platform_backends.state_store import RedisStateStore as RSS

    store = config.get_state_store()
    assert isinstance(store, RSS)
    assert store.client is None


def test_config_rejects_unknown_backend_values(monkeypatch):
    import src.config as config

    monkeypatch.setenv("EXECUTION_BACKEND", "not-a-real-backend")
    with pytest.raises(ValueError):
        config.get_pipeline_runner()
