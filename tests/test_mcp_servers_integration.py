"""One real MCP round trip per server, proving the `mcp` SDK's actual tool registration/
dispatch machinery works end-to-end (schema generation from type hints, call_tool dispatch,
error handling) -- as opposed to tests/test_mcp_servers.py, which calls the plain tools
classes directly and never touches the MCP layer at all. Both servers are built with fake
tools instances injected, so this needs no real S3/Spark/model/cluster.
"""

from __future__ import annotations

import asyncio
import json

from src.mcp_servers.data_ops_server import DataOpsTools, build_data_ops_mcp_server
from src.mcp_servers.spark_runtime_server import SparkRuntimeTools, build_spark_runtime_mcp_server
from src.platform_backends.pipeline_runner import RunHandle


def _run(coro):
    return asyncio.run(coro)


def _content_json(result):
    assert not result.is_error, result.content
    return json.loads(result.content[0].text)


class _FakeDataOpsTools:
    def run_data_product_validation(self, pipeline_name: str) -> dict:
        return {"pipeline_name": pipeline_name, "overall_status": "PASS", "checks": []}

    def get_data_product_context(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name}

    def get_metric_context(self, pipeline_name: str, metric_name: str) -> dict:
        return {"asset_id": pipeline_name, "field": metric_name}

    def get_lineage(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name, "field": "lineage"}

    def get_runtime_health(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name, "field": "runtime_health"}

    def get_relevant_pipeline_code(self, pipeline_name: str) -> dict:
        return {"asset_id": pipeline_name, "field": "relevant_code"}

    def create_candidate_repair(self, pipeline_name: str) -> dict:
        return {"repair_id": "fake-repair-1", "pipeline_name": pipeline_name}

    def verify_candidate_repair(self, repair_id: str) -> dict:
        from src.dataset_registry_tools import ToolError

        if repair_id != "fake-repair-1":
            raise ToolError(f"unknown repair_id {repair_id!r}")
        return {"repair_id": repair_id, "verification_status": "VERIFIED_PENDING_PR"}

    def get_pr_ready_artifact(self, pipeline_name: str) -> dict:
        return {"pipeline_name": pipeline_name, "pending": False, "pr_artifact": None}


def test_data_ops_mcp_server_lists_and_calls_tools():
    server = build_data_ops_mcp_server(tools=_FakeDataOpsTools())

    async def scenario():
        tools = await server.list_tools()
        names = {t.name for t in tools}
        assert names == {
            "get_data_product_context", "get_metric_context", "get_lineage", "get_runtime_health",
            "get_relevant_pipeline_code", "run_data_product_validation", "create_candidate_repair",
            "verify_candidate_repair", "get_pr_ready_artifact",
        }

        validation = await server.call_tool("run_data_product_validation", {"pipeline_name": "loan_portfolio"})
        assert _content_json(validation)["overall_status"] == "PASS"

        created = await server.call_tool("create_candidate_repair", {"pipeline_name": "loan_portfolio"})
        repair_id = _content_json(created)["repair_id"]

        verified = await server.call_tool("verify_candidate_repair", {"repair_id": repair_id})
        assert _content_json(verified)["verification_status"] == "VERIFIED_PENDING_PR"

        bad = await server.call_tool("verify_candidate_repair", {"repair_id": "not-real"})
        # ToolError is caught inside the tool wrapper and returned as a normal {"error": ...}
        # result (matching this codebase's dispatch_tool convention) -- not an MCP-level error.
        assert "error" in _content_json(bad)

    _run(scenario())


class _FakeSparkRuntimeTools:
    def __init__(self) -> None:
        self._handle = None

    def submit_spark_pipeline(self, pipeline_name: str) -> dict:
        self._handle = RunHandle(pipeline_name=pipeline_name, run_id="run-xyz", backend="local")
        return {"run_id": "run-xyz", "pipeline_name": pipeline_name, "backend": "local"}

    def get_spark_application_status(self, run_id: str) -> dict:
        return {"status": "SUCCEEDED", "run_id": run_id}

    def get_spark_run_summary(self, run_id: str) -> dict:
        return {"run_id": run_id, "job_count": 2}

    def get_failed_stages(self, run_id: str) -> dict:
        return {"run_id": run_id, "failed_stages": []}

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = 100) -> dict:
        return {"run_id": run_id, "excerpt": "log line", "truncated_to_lines": max_lines}

    def get_pod_status(self, run_id: str) -> dict:
        return {"available": False}


def test_spark_runtime_mcp_server_lists_and_calls_tools():
    server = build_spark_runtime_mcp_server(tools=_FakeSparkRuntimeTools())

    async def scenario():
        tools = await server.list_tools()
        names = {t.name for t in tools}
        assert names == {
            "submit_spark_pipeline", "get_spark_application_status", "get_spark_run_summary",
            "get_failed_stages", "get_driver_log_excerpt", "get_pod_status",
        }

        submitted = await server.call_tool("submit_spark_pipeline", {"pipeline_name": "loan_portfolio"})
        run_id = _content_json(submitted)["run_id"]
        assert run_id == "run-xyz"

        status = await server.call_tool("get_spark_application_status", {"run_id": run_id})
        assert _content_json(status)["status"] == "SUCCEEDED"

        log = await server.call_tool("get_driver_log_excerpt", {"run_id": run_id, "max_lines": 5})
        assert _content_json(log)["truncated_to_lines"] == 5

    _run(scenario())
