"""Unit tests for the plain tools classes behind both MCP servers (src/mcp_servers/*),
calling them directly -- bypassing MCP transport entirely, the same way this codebase already
tests LifecycleDiagnosticTools/dispatch_tool.

ContextRetriever/validation-backed tools (get_data_product_context, get_metric_context,
get_lineage, get_runtime_health, get_relevant_pipeline_code, run_data_product_validation) are
exercised against real local S3 (skip cleanly via conftest's s3_storage fixture) -- they're
thin wraps of already-tested code (src/context_retriever.py, PipelineSpec.run_validate), so
this only checks the wrap is correct, not the underlying logic again.

create_candidate_repair/verify_candidate_repair's own new logic -- the two-phase split via
StateStore -- is tested against fakes with run_lifecycle_self_healing/
run_verify_lifecycle_repair monkeypatched, matching tests/test_lifecycle_run_self_healing.py's
existing convention for testing composition/wiring without a real diagnose/apply/verify run.
"""

from __future__ import annotations

import pytest

import src.mcp_servers.data_ops_server as data_ops_server_module
from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.dataset_registry_tools import ToolError
from src.mcp_servers.data_ops_server import DataOpsTools
from src.mcp_servers.spark_runtime_server import SparkRuntimeTools
from src.platform_backends.pipeline_runner import RunHandle

PIPELINE_NAME = "loan_portfolio"


# --- DataOpsTools: ContextRetriever/validation wraps (real S3) ---------------------------


@pytest.fixture
def data_ops_tools(s3_storage):
    return DataOpsTools(
        storage=s3_storage,
        context_retriever=ContextRetriever(store=FileContextStore()),
        state_store=_FakeStateStore(),
        diagnosis_model_client_factory=lambda: None,
        repair_model_client_factory=lambda: None,
        spark_factory=lambda: (_ for _ in ()).throw(AssertionError("spark_factory must not be called by context/validation tools")),
    )


def test_get_data_product_context(data_ops_tools):
    result = data_ops_tools.get_data_product_context(PIPELINE_NAME)
    assert result["asset_id"] == PIPELINE_NAME
    assert result["field"] == "pipeline_metadata"


def test_get_metric_context_unknown_pipeline_raises(data_ops_tools):
    with pytest.raises(ToolError):
        data_ops_tools.get_metric_context("not_a_real_pipeline", "some_metric")


def test_get_lineage(data_ops_tools):
    result = data_ops_tools.get_lineage(PIPELINE_NAME)
    assert result["field"] == "lineage"


def test_get_runtime_health(data_ops_tools):
    result = data_ops_tools.get_runtime_health(PIPELINE_NAME)
    assert result["field"] == "runtime_health"


def test_get_relevant_pipeline_code(data_ops_tools):
    result = data_ops_tools.get_relevant_pipeline_code(PIPELINE_NAME)
    assert result["value"]["file"] == "src/etl_spark_loan_portfolio.py"
    assert "compute_loan_portfolio" in result["value"]["functions"]


def test_run_data_product_validation(data_ops_tools):
    result = data_ops_tools.run_data_product_validation(PIPELINE_NAME)
    assert "overall_status" in result
    assert "checks" in result


def test_get_pr_ready_artifact_when_nothing_pending(data_ops_tools):
    result = data_ops_tools.get_pr_ready_artifact(PIPELINE_NAME)
    assert result == {"pipeline_name": PIPELINE_NAME, "pending": False, "pr_artifact": None}


def test_get_pr_ready_artifact_when_pending(data_ops_tools, s3_storage):
    key = data_ops_server_module._pending_repair_key(PIPELINE_NAME)
    s3_storage.write_json(key, {"pipeline_name": PIPELINE_NAME, "status": "pending_review", "pr_artifact": {"branch": "repair/abc"}})
    try:
        result = data_ops_tools.get_pr_ready_artifact(PIPELINE_NAME)
        assert result["pending"] is True
        assert result["pr_artifact"]["branch"] == "repair/abc"
    finally:
        s3_storage.delete(key)


# --- DataOpsTools: create_candidate_repair / verify_candidate_repair (fakes) --------------


class _FakeStateStore:
    def __init__(self) -> None:
        self._data: dict = {}

    def get(self, key: str):
        return self._data.get(key)

    def set(self, key: str, value: dict) -> None:
        self._data[key] = value

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def list_keys(self, prefix: str = ""):
        return [k for k in self._data if k.startswith(prefix)]


class _FakeRepairStorage:
    def __init__(self) -> None:
        self._json: dict = {}

    def read_json(self, path: str):
        if path == "context/business_rules.json":
            return {"rule": "value"}
        if path == "context/validations/loan_portfolio.json":
            return {"rules": []}
        return self._json[path]

    def write_json(self, path: str, value) -> None:
        self._json[path] = value

    def exists(self, path: str) -> bool:
        return path in self._json

    def delete(self, path: str) -> None:
        self._json.pop(path, None)


def _repair_tools(state_store=None, storage=None):
    return DataOpsTools(
        storage=storage or _FakeRepairStorage(),
        context_retriever=None,
        state_store=state_store or _FakeStateStore(),
        diagnosis_model_client_factory=lambda: None,
        repair_model_client_factory=lambda: None,
        spark_factory=lambda: "fake-spark",
    )


def test_create_candidate_repair_persists_state_and_returns_repair_id(monkeypatch):
    monkeypatch.setattr(
        data_ops_server_module,
        "PIPELINE_REGISTRY",
        {PIPELINE_NAME: type("Spec", (), {"validation_rules_key": "context/validations/loan_portfolio.json", "run_validate": staticmethod(lambda *a: {"overall_status": "FAIL", "checks": []})})},
    )
    monkeypatch.setattr(
        data_ops_server_module,
        "run_lifecycle_self_healing",
        lambda pipeline_name, spark, storage, diag_factory, repair_factory, mode: {
            "run_id": "abc123",
            "diagnosis": {"diagnosis_status": "DIAGNOSED", "root_cause_category": "SOURCE_CONTRACT_CHANGE"},
            "repair_plan": {"repair_decision": "PROPOSE_REPAIR"},
            "repair_result": {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "x.py"},
        },
    )

    state_store = _FakeStateStore()
    tools = _repair_tools(state_store=state_store)
    result = tools.create_candidate_repair(PIPELINE_NAME)

    assert result["repair_id"] == "abc123"
    assert result["repair_status"] == "APPLIED"
    stored = state_store.get("repairs/abc123")
    assert stored["pipeline_name"] == PIPELINE_NAME
    assert stored["status"] == "AWAITING_VERIFICATION"
    assert stored["repair_result"]["repair_status"] == "APPLIED"


def test_create_candidate_repair_unknown_pipeline_raises():
    tools = _repair_tools()
    with pytest.raises(ToolError):
        tools.create_candidate_repair("not_a_real_pipeline")


def test_verify_candidate_repair_unknown_repair_id_raises():
    tools = _repair_tools()
    with pytest.raises(ToolError):
        tools.verify_candidate_repair("does-not-exist")


def test_verify_candidate_repair_persists_pending_repair_on_verified_pending_pr(monkeypatch):
    monkeypatch.setattr(
        data_ops_server_module,
        "run_verify_lifecycle_repair",
        lambda pipeline_name, spark, storage, br, vr, vb, rr, **kwargs: {
            "verification_status": "VERIFIED_PENDING_PR",
            "summary": "all checks passed",
            "failed_checks_before": ["x"],
            "failed_checks_after": [],
            "tests": {"targeted": "PASS", "full_relevant_suite": "PASS"},
            "pr_artifact": {"branch": "repair/xyz"},
        },
    )

    storage = _FakeRepairStorage()
    state_store = _FakeStateStore()
    state_store.set(
        "repairs/abc123",
        {
            "repair_id": "abc123",
            "pipeline_name": PIPELINE_NAME,
            "status": "AWAITING_VERIFICATION",
            "diagnosis": {"diagnosis_status": "DIAGNOSED"},
            "repair_plan": {"repair_decision": "PROPOSE_REPAIR"},
            "repair_result": {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "x.py"},
            "validation_before": {"overall_status": "FAIL", "checks": []},
            "business_rules": {},
            "validation_rules": {},
        },
    )
    tools = _repair_tools(state_store=state_store, storage=storage)

    result = tools.verify_candidate_repair("abc123")

    assert result["verification_status"] == "VERIFIED_PENDING_PR"
    assert result["branch"] == "repair/xyz"
    pending_key = data_ops_server_module._pending_repair_key(PIPELINE_NAME)
    assert storage.exists(pending_key)
    assert storage.read_json(pending_key)["pr_artifact"]["branch"] == "repair/xyz"
    assert state_store.get("repairs/abc123")["status"] == "VERIFIED_PENDING_PR"


def test_verify_candidate_repair_does_not_persist_pending_repair_on_not_verified(monkeypatch):
    monkeypatch.setattr(
        data_ops_server_module,
        "run_verify_lifecycle_repair",
        lambda pipeline_name, spark, storage, br, vr, vb, rr, **kwargs: {
            "verification_status": "NOT_VERIFIED",
            "summary": "a check failed",
            "failed_checks_before": ["x"],
            "failed_checks_after": ["x"],
            "tests": {"targeted": "FAIL", "full_relevant_suite": "FAIL"},
        },
    )
    storage = _FakeRepairStorage()
    state_store = _FakeStateStore()
    state_store.set(
        "repairs/abc123",
        {
            "repair_id": "abc123", "pipeline_name": PIPELINE_NAME, "status": "AWAITING_VERIFICATION",
            "diagnosis": {}, "repair_plan": {}, "repair_result": {"repair_status": "APPLIED", "workspace_dir": "/tmp/x", "target_file": "x.py"},
            "validation_before": {"overall_status": "FAIL", "checks": []}, "business_rules": {}, "validation_rules": {},
        },
    )
    tools = _repair_tools(state_store=state_store, storage=storage)

    result = tools.verify_candidate_repair("abc123")

    assert result["verification_status"] == "NOT_VERIFIED"
    assert not storage.exists(data_ops_server_module._pending_repair_key(PIPELINE_NAME))


# --- SparkRuntimeTools (fakes) -------------------------------------------------------------


class _FakePipelineRunner:
    def __init__(self) -> None:
        self.submitted: list = []

    def submit(self, pipeline_name: str) -> RunHandle:
        if pipeline_name == "not_a_real_pipeline":
            raise ValueError("unknown pipeline")
        self.submitted.append(pipeline_name)
        return RunHandle(pipeline_name=pipeline_name, run_id="run-1", backend="local")

    def get_status(self, handle: RunHandle) -> dict:
        return {"status": "SUCCEEDED", "run_id": handle.run_id}

    def await_completion(self, handle: RunHandle, timeout_seconds: float = 300.0) -> dict:
        return self.get_status(handle)


class _FakeInspector:
    def get_run_summary(self, run_id: str) -> dict:
        return {"run_id": run_id, "job_count": 3}

    def get_failed_stages(self, run_id: str) -> list:
        return [{"stage_id": 1}]

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = 100) -> str:
        return "\n".join(f"line {i}" for i in range(max_lines))

    def get_pod_status(self, run_id: str) -> dict:
        return {"available": True, "phase": "Running"}


def test_submit_spark_pipeline_and_get_status():
    tools = SparkRuntimeTools(pipeline_runner=_FakePipelineRunner(), runtime_inspector=_FakeInspector())
    submitted = tools.submit_spark_pipeline(PIPELINE_NAME)
    assert submitted["run_id"] == "run-1"

    status = tools.get_spark_application_status(submitted["run_id"])
    assert status["status"] == "SUCCEEDED"


def test_submit_spark_pipeline_unknown_pipeline_raises():
    tools = SparkRuntimeTools(pipeline_runner=_FakePipelineRunner(), runtime_inspector=_FakeInspector())
    with pytest.raises(ToolError):
        tools.submit_spark_pipeline("not_a_real_pipeline")


def test_get_status_before_submit_raises():
    tools = SparkRuntimeTools(pipeline_runner=_FakePipelineRunner(), runtime_inspector=_FakeInspector())
    with pytest.raises(ToolError):
        tools.get_spark_application_status("no-such-run")


def test_get_spark_run_summary_and_failed_stages():
    tools = SparkRuntimeTools(pipeline_runner=_FakePipelineRunner(), runtime_inspector=_FakeInspector())
    submitted = tools.submit_spark_pipeline(PIPELINE_NAME)

    summary = tools.get_spark_run_summary(submitted["run_id"])
    assert summary["job_count"] == 3

    failed = tools.get_failed_stages(submitted["run_id"])
    assert failed["failed_stages"] == [{"stage_id": 1}]


def test_get_driver_log_excerpt_is_bounded():
    tools = SparkRuntimeTools(pipeline_runner=_FakePipelineRunner(), runtime_inspector=_FakeInspector())
    submitted = tools.submit_spark_pipeline(PIPELINE_NAME)

    from src.platform_backends.runtime_inspector import MAX_LOG_LINES

    result = tools.get_driver_log_excerpt(submitted["run_id"], max_lines=10_000)

    assert result["truncated_to_lines"] == MAX_LOG_LINES
    assert len(result["excerpt"].splitlines()) == MAX_LOG_LINES


def test_get_pod_status():
    tools = SparkRuntimeTools(pipeline_runner=_FakePipelineRunner(), runtime_inspector=_FakeInspector())
    submitted = tools.submit_spark_pipeline(PIPELINE_NAME)
    assert tools.get_pod_status(submitted["run_id"])["available"] is True
