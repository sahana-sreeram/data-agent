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

    def _runtime_evidence_id(self, run_id: str) -> str:
        """Our own run_id (the SparkApplication CR's suffix) is NOT the same id Spark's own
        History Server indexes by -- confirmed live: the CR is named
        loan-portfolio-<run_id>, but Spark assigns its OWN internal application id (e.g.
        spark-4a6e74fd6ed2419bac0b5622329b65df), only knowable once the job has actually
        started, via the CR's own status.sparkApplicationId. RHOAISparkRunner.get_status()
        surfaces this in its "raw" field; local mode's LocalSparkRunner.get_status() has no
        such field, so .get("raw") is simply absent there and this transparently falls back
        to run_id unchanged -- exactly local mode's existing, already-correct behavior (its
        LocalRuntimeInspector is keyed by the same run_id used as the Spark job group tag)."""
        handle = self._require_handle(run_id)
        status = self.pipeline_runner.get_status(handle)
        raw = status.get("raw") or {}
        return raw.get("sparkApplicationId") or run_id

    def _driver_pod_name(self, run_id: str) -> str:
        """Pod status/logs need the actual k8s pod name, which is neither our run_id nor
        Spark's application id -- RHOAISparkRunner names the SparkApplication CR (and the
        Spark Operator names its driver pod after it) as `<backend_ref name>-driver`
        (confirmed live). Local mode's RunHandle has no backend_ref, so this falls back to
        run_id unchanged -- LocalRuntimeInspector's stubs don't use it for a real lookup
        anyway (there are no pods in local mode)."""
        handle = self._require_handle(run_id)
        if handle.backend_ref and "name" in handle.backend_ref:
            return f"{handle.backend_ref['name']}-driver"
        return run_id

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
        return self.runtime_inspector.get_run_summary(self._runtime_evidence_id(run_id))

    def get_failed_stages(self, run_id: str) -> dict:
        return {"run_id": run_id, "failed_stages": self.runtime_inspector.get_failed_stages(self._runtime_evidence_id(run_id))}

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = DEFAULT_LOG_LINES) -> dict:
        bounded_max_lines = min(max_lines, MAX_LOG_LINES)
        excerpt = self.runtime_inspector.get_driver_log_excerpt(self._driver_pod_name(run_id), max_lines=bounded_max_lines)
        return {"run_id": run_id, "excerpt": excerpt, "truncated_to_lines": bounded_max_lines}

    def get_pod_status(self, run_id: str) -> dict:
        return self.runtime_inspector.get_pod_status(self._driver_pod_name(run_id))


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
