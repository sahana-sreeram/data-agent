"""Spark Runtime MCP server: submit and inspect Spark pipeline runs through the exact same
PipelineRunner/RuntimeInspector Protocols regardless of backend (src/platform_backends/,
selected via src.config's EXECUTION_BACKEND/RUNTIME_BACKEND) -- this server's own code never
branches on local vs. RHOAI.

get_driver_log_excerpt is bounded (DEFAULT_LOG_LINES/MAX_LOG_LINES, mirroring
src.dataset_registry_tools's sample-size limits) -- entire raw logs are never sent to Codex,
only a tail excerpt.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass, field

from src.dataset_registry_tools import ToolError
from src.platform_backends.pipeline_runner import PipelineRunner, RunHandle
from src.platform_backends.runtime_inspector import DEFAULT_LOG_LINES, MAX_LOG_LINES, RuntimeInspector


@dataclass
class SparkRuntimeTools:
    pipeline_runner: PipelineRunner
    runtime_inspector: RuntimeInspector
    _handles: dict = field(default_factory=dict)  # run_id -> RunHandle, populated by submit_spark_pipeline

    def _require_handle(self, run_id: str) -> RunHandle:
        handle = self._handles.get(run_id)
        if handle is None:
            raise ToolError(f"unknown run_id {run_id!r}; call submit_spark_pipeline first")
        return handle

    def submit_spark_pipeline(self, pipeline_name: str) -> dict:
        try:
            handle = self.pipeline_runner.submit(pipeline_name)
        except ValueError as exc:
            raise ToolError(str(exc)) from exc
        self._handles[handle.run_id] = handle
        return {"run_id": handle.run_id, "pipeline_name": pipeline_name, "backend": handle.backend}

    def get_spark_application_status(self, run_id: str) -> dict:
        handle = self._require_handle(run_id)
        return self.pipeline_runner.get_status(handle)

    def get_spark_run_summary(self, run_id: str) -> dict:
        self._require_handle(run_id)
        return self.runtime_inspector.get_run_summary(run_id)

    def get_failed_stages(self, run_id: str) -> dict:
        self._require_handle(run_id)
        return {"run_id": run_id, "failed_stages": self.runtime_inspector.get_failed_stages(run_id)}

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = DEFAULT_LOG_LINES) -> dict:
        self._require_handle(run_id)
        bounded_max_lines = min(max_lines, MAX_LOG_LINES)
        excerpt = self.runtime_inspector.get_driver_log_excerpt(run_id, max_lines=bounded_max_lines)
        return {"run_id": run_id, "excerpt": excerpt, "truncated_to_lines": bounded_max_lines}

    def get_pod_status(self, run_id: str) -> dict:
        self._require_handle(run_id)
        return self.runtime_inspector.get_pod_status(run_id)


def build_default_spark_runtime_tools() -> SparkRuntimeTools:
    from src.config import get_pipeline_runner, get_runtime_inspector

    return SparkRuntimeTools(pipeline_runner=get_pipeline_runner(), runtime_inspector=get_runtime_inspector())


def _tool_error_to_dict(fn):
    """See src.mcp_servers.data_ops_server._tool_error_to_dict -- identical convention."""

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ToolError as exc:
            return {"error": str(exc)}

    return wrapped


def build_spark_runtime_mcp_server(tools: SparkRuntimeTools | None = None):
    """Build the real MCP server object (uses the `mcp` SDK). `tools` is injectable for
    tests; production/deployment code leaves it unset (build_default_spark_runtime_tools())."""
    from mcp.server.mcpserver import MCPServer

    tools = tools or build_default_spark_runtime_tools()
    server = MCPServer(
        name="spark-runtime",
        description="Submit and inspect Spark pipeline runs (local or RHOAI/Spark Operator) as runtime evidence.",
    )

    server.add_tool(
        _tool_error_to_dict(tools.submit_spark_pipeline),
        name="submit_spark_pipeline",
        description="Submit a lifecycle pipeline's ETL+validation for execution (local Spark, or a SparkApplication on RHOAI). Returns a run_id.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_spark_application_status),
        name="get_spark_application_status",
        description="Return the current application-level status (e.g. SUCCEEDED/FAILED/RUNNING) for a run_id from submit_spark_pipeline.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_spark_run_summary),
        name="get_spark_run_summary",
        description="Return a job/stage-count summary of Spark execution evidence for a run -- whether the Spark job itself completed normally, independent of data-quality validation.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_failed_stages),
        name="get_failed_stages",
        description="Return any Spark stages that had failed tasks during this run.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_driver_log_excerpt),
        name="get_driver_log_excerpt",
        description="Return a bounded tail excerpt (never the entire raw log) of this run's driver log.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_pod_status),
        name="get_pod_status",
        description="Return pod-level status for this run (RHOAI/OpenShift only -- local mode has no pods).",
    )
    return server


def main() -> None:
    """Run this server standalone over streamable-HTTP -- how it runs as a deployed RHOAI
    service (see deploy/rhoai/mcp-spark-runtime-deployment.yaml). Locally/in tests, callers
    use build_spark_runtime_mcp_server() directly (in-process, no network) instead of this."""
    import os

    server = build_spark_runtime_mcp_server()
    server.run(transport="streamable-http", host=os.environ.get("MCP_HOST", "0.0.0.0"), port=int(os.environ.get("MCP_PORT", "8000")))


if __name__ == "__main__":
    main()
