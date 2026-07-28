"""Tests for src/agents/codex_mcp_loop.py: the Codex-drives-everything-through-MCP
orchestration loop. Uses fake DataOpsTools/SparkRuntimeTools instances (no real S3/Spark/
model/cluster) wrapped in real MCPServer objects, so every tool call in these tests really
does round-trip through a live MCP ClientSession -- this is what proves the loop dispatches
through MCP transport, not a direct Python call into the tools classes.
"""

from __future__ import annotations

import pytest

from src.agents.codex_mcp_loop import REPORT_TOOL_NAME, run_codex_mcp_loop, run_and_persist_codex_mcp_loop
from src.mcp_servers.data_ops_server import build_data_ops_mcp_server
from src.mcp_servers.spark_runtime_server import build_spark_runtime_mcp_server
from src.model_client import ModelClientError, ModelResponse, ScriptedDiagnosisModelClient, ToolCall
from src.platform_backends.pipeline_runner import RunHandle

PIPELINE_NAME = "loan_portfolio"


class _FakeDataOpsTools:
    def run_data_product_validation(self, pipeline_name: str) -> dict:
        return {"pipeline_name": pipeline_name, "overall_status": "FAIL", "checks": []}

    def get_data_product_context(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name}

    def get_metric_context(self, pipeline_name: str, metric_name: str) -> dict:
        return {"asset_id": pipeline_name, "field": metric_name}

    def get_lineage(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name}

    def get_runtime_health(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name}

    def get_relevant_pipeline_code(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name}

    def create_candidate_repair(self, pipeline_name: str) -> dict:
        return {"repair_id": "fake-repair-1", "pipeline_name": pipeline_name}

    def verify_candidate_repair(self, repair_id: str) -> dict:
        return {"repair_id": repair_id, "verification_status": "VERIFIED_PENDING_PR"}

    def get_pr_ready_artifact(self, pipeline_name: str) -> dict:
        return {"pipeline_name": pipeline_name, "pending": True}


class _FakeSparkRuntimeTools:
    def submit_spark_pipeline(self, pipeline_name: str) -> dict:
        return {"run_id": "run-xyz", "pipeline_name": pipeline_name, "backend": "local"}

    def get_spark_application_status(self, run_id: str) -> dict:
        return {"status": "SUCCEEDED", "run_id": run_id}

    def get_spark_run_summary(self, run_id: str) -> dict:
        return {"run_id": run_id, "job_count": 2}

    def get_failed_stages(self, run_id: str) -> dict:
        return {"run_id": run_id, "failed_stages": []}

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = 100) -> dict:
        return {"run_id": run_id, "excerpt": "ok"}

    def get_pod_status(self, run_id: str) -> dict:
        return {"available": False}


@pytest.fixture
def servers():
    return build_data_ops_mcp_server(tools=_FakeDataOpsTools()), build_spark_runtime_mcp_server(tools=_FakeSparkRuntimeTools())


def _call(call_id, name, arguments):
    return ToolCall(id=call_id, name=name, arguments=arguments)


def test_full_loop_dispatches_across_both_servers_and_reports(servers):
    data_ops_server, spark_runtime_server = servers
    scripted = ScriptedDiagnosisModelClient(
        [
            ModelResponse(tool_calls=[_call("1", "submit_spark_pipeline", {"pipeline_name": PIPELINE_NAME})]),
            ModelResponse(tool_calls=[_call("2", "get_spark_application_status", {"run_id": "run-xyz"})]),
            ModelResponse(tool_calls=[_call("3", "run_data_product_validation", {"pipeline_name": PIPELINE_NAME})]),
            ModelResponse(tool_calls=[_call("4", "create_candidate_repair", {"pipeline_name": PIPELINE_NAME})]),
            ModelResponse(tool_calls=[_call("5", "verify_candidate_repair", {"repair_id": "fake-repair-1"})]),
            ModelResponse(
                tool_calls=[
                    _call(
                        "6",
                        REPORT_TOOL_NAME,
                        {
                            "pipelines_checked": [PIPELINE_NAME],
                            "incidents_found": [PIPELINE_NAME],
                            "repairs_verified_pending_pr": [PIPELINE_NAME],
                            "summary": "loan_portfolio failed validation; repaired and verified pending human review.",
                        },
                    )
                ]
            ),
        ]
    )

    result = run_codex_mcp_loop([PIPELINE_NAME], scripted, data_ops_server, spark_runtime_server)

    assert len(result.run_id) > 0
    assert [s["tool"] for s in result.stages] == [
        "submit_spark_pipeline", "get_spark_application_status", "run_data_product_validation",
        "create_candidate_repair", "verify_candidate_repair",
    ]
    assert result.final_report["pipelines_checked"] == [PIPELINE_NAME]
    assert result.final_report["repairs_verified_pending_pr"] == [PIPELINE_NAME]

    import json

    validation_stage = next(s for s in result.stages if s["tool"] == "run_data_product_validation")
    assert json.loads(validation_stage["result"])["overall_status"] == "FAIL"


def test_loop_raises_if_report_tool_never_called(servers):
    data_ops_server, spark_runtime_server = servers
    scripted = ScriptedDiagnosisModelClient(
        [ModelResponse(tool_calls=[_call("1", "submit_spark_pipeline", {"pipeline_name": PIPELINE_NAME})])] * 3
    )

    with pytest.raises(ModelClientError):
        run_codex_mcp_loop([PIPELINE_NAME], scripted, data_ops_server, spark_runtime_server, max_turns=3)


def test_loop_reports_unknown_tool_name_as_error_without_crashing(servers):
    data_ops_server, spark_runtime_server = servers
    scripted = ScriptedDiagnosisModelClient(
        [
            ModelResponse(tool_calls=[_call("1", "not_a_real_tool", {})]),
            ModelResponse(
                tool_calls=[
                    _call("2", REPORT_TOOL_NAME, {"pipelines_checked": [], "incidents_found": [], "repairs_verified_pending_pr": [], "summary": "n/a"})
                ]
            ),
        ]
    )

    result = run_codex_mcp_loop([PIPELINE_NAME], scripted, data_ops_server, spark_runtime_server)

    import json

    assert json.loads(result.stages[0]["result"]) == {"error": "unknown tool 'not_a_real_tool'"}


def test_run_and_persist_writes_manifest_to_storage(monkeypatch, servers):
    data_ops_server, spark_runtime_server = servers
    # run_and_persist_codex_mcp_loop imports these lazily (`from src.mcp_servers.X import
    # build_...`) at call time, so patching the source modules' attributes is what actually
    # takes effect -- patching a name on codex_mcp_loop itself would do nothing, since that
    # name is never bound at codex_mcp_loop's module scope.
    monkeypatch.setattr("src.mcp_servers.data_ops_server.build_data_ops_mcp_server", lambda: data_ops_server)
    monkeypatch.setattr("src.mcp_servers.spark_runtime_server.build_spark_runtime_mcp_server", lambda: spark_runtime_server)

    scripted = ScriptedDiagnosisModelClient(
        [ModelResponse(tool_calls=[_call("1", REPORT_TOOL_NAME, {"pipelines_checked": [PIPELINE_NAME], "incidents_found": [], "repairs_verified_pending_pr": [], "summary": "all healthy"})])]
    )

    written: dict = {}

    class _FakeStorage:
        def write_json(self, path, value):
            written[path] = value

    manifest = run_and_persist_codex_mcp_loop([PIPELINE_NAME], _FakeStorage(), lambda: scripted)

    assert manifest["backend"] == "codex_mcp"
    assert written[f"curated/demo_runs/{manifest['run_id']}.json"] == manifest
    assert written["curated/demo_run_latest.json"] == manifest
