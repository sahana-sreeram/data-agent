"""The Codex/MCP orchestration loop: "a user defines the loop; RHOAI exposes the platform
capabilities; MCP makes them available; Codex gathers the context and manages the workflow."

Unlike every other agent loop in this codebase (src/lifecycle_diagnosis_agent.py,
src/diagnosis_agent.py), which dispatches tool calls via a plain in-process Python
function (dispatch_tool), this loop dispatches every tool call through a real MCP
ClientSession -- the same DiagnosisModelClient Protocol and tool-calling loop shape
(model_client.send(messages, tools) -> ModelResponse, forced tool_choice, a designated
"report" tool that ends the loop -- see src/lifecycle_diagnosis_agent.py), but every tool
result comes back over an actual MCP call_tool() round trip, never a direct Python call into
src.mcp_servers.data_ops_server.DataOpsTools/SparkRuntimeTools.

_InProcessMCPClient connects to an in-process MCPServer over real in-memory MCP transport
streams (mcp.shared.memory) rather than a subprocess/HTTP hop -- the fastest, most reliable
way to exercise a genuine client/server round trip locally. It reaches into MCPServer's
private `_lowlevel_server` attribute (the only way to drive the SDK's lowlevel Server.run()
over an arbitrary stream pair; MCPServer.run() itself only offers stdio/sse/streamable-http).
On RHOAI, both servers are deployed processes reachable over streamable-HTTP instead (see
deploy/rhoai/) -- swap _InProcessMCPClient for mcp.client.streamable_http's client, and
nothing else in this module changes, since the loop only ever talks to the ClientSession
interface (list_tools/call_tool), never to the server object directly.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from src.diagnosis_agent import _serialize_tool_call
from src.model_client import DiagnosisModelClient, ModelClientError, ToolCall


# Requiring one submission at a time (confirm a pipeline's job has left SUBMITTED before
# submitting the next -- see SYSTEM_PROMPT) costs more turns than the old back-to-back
# submission behavior: confirmed live (2026-07-29) that a real 6-pipeline run exhausted 16
# turns with every model call succeeding, purely from the extra status-check turns the
# sequential-submission requirement adds, not from any looping or model confusion.
DEFAULT_MAX_TURNS = 40
REPORT_TOOL_NAME = "report_morning_loop_summary"

SYSTEM_PROMPT = """You are Codex, running the morning data-operations loop for a lending data platform, entirely through MCP tools exposed by two servers: "spark-runtime" (submit_spark_pipeline, get_spark_application_status, get_spark_run_summary, get_failed_stages, get_driver_log_excerpt, get_pod_status) and "data-ops" (get_data_product_context, get_metric_context, get_lineage, get_runtime_health, get_relevant_pipeline_code, run_data_product_validation, create_candidate_repair, verify_candidate_repair, get_pr_ready_artifact).

You will be given a list of data products (pipelines) to check this morning. For each one:

1. Submit it for execution: submit_spark_pipeline(pipeline_name). This proves the Spark job itself runs -- independent of whether its output is trustworthy. IMPORTANT: submit pipelines one at a time, never back-to-back -- confirm via get_spark_application_status that a pipeline's job has left the SUBMITTED state (RUNNING, SUCCEEDED, or FAILED) before calling submit_spark_pipeline for the next pipeline. Submitting multiple pipelines in rapid succession can overwhelm the Spark Operator's own submission controller and leave later jobs stuck indefinitely.
2. Check the run's own health: get_spark_application_status and get_spark_run_summary (and get_failed_stages if anything looks wrong). A SUCCEEDED Spark run means only that it executed without error -- it does NOT mean the data it produced is correct.
3. Check whether the data product is actually trustworthy: run_data_product_validation(pipeline_name). This is the real trust signal, separate from Spark's own execution status. A pipeline can run to completion and still be silently wrong.
4. If validation fails, gather evidence before concluding anything: get_data_product_context, get_lineage, get_relevant_pipeline_code, and get_metric_context for the specific metric(s) that failed. Combine this with the Spark runtime evidence from steps 1-2 to tell a semantic failure (the job ran fine; the business logic or an upstream contract is wrong) apart from an infrastructure failure (the job itself failed or a stage had failed tasks).
5. If you have enough evidence that this is a genuine, repairable incident, generate a bounded candidate repair: create_candidate_repair(pipeline_name). This diagnoses and applies a patch inside an isolated sandbox -- it never touches the real repository or curated data.
6. Verify the candidate: verify_candidate_repair(repair_id). This reruns Spark against the isolated candidate output and runs deterministic validators/tests. On a full pass it produces a local, unpushed VERIFIED_PENDING_PR artifact -- it does NOT merge or promote anything.
7. Call get_pr_ready_artifact(pipeline_name) to confirm the final artifact (branch, diff, before/after checks) is what you expect.

You are never able to push, merge, or promote anything yourself -- the loop's job ends at a local, verified, human-reviewable artifact. A human decides separately whether to accept it.

You are not required to call every tool for every pipeline -- a pipeline that is already trustworthy needs only steps 1-3. Work through pipelines strictly one at a time (finish or at least submit-and-confirm-running for one before starting the next) -- never submit_spark_pipeline for multiple pipelines back-to-back.

When you have checked every pipeline you were given, and only then, call the report_morning_loop_summary tool exactly once with your full summary. You are not finished until you call it."""


def _report_tool_spec() -> dict:
    return {
        "type": "function",
        "function": {
            "name": REPORT_TOOL_NAME,
            "description": "Submit your final summary of this morning loop run. Call this exactly once, when every pipeline you were given has been checked.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipelines_checked": {"type": "array", "items": {"type": "string"}},
                    "incidents_found": {"type": "array", "items": {"type": "string"}, "description": "Pipelines found untrustworthy."},
                    "repairs_verified_pending_pr": {"type": "array", "items": {"type": "string"}, "description": "Pipelines with a VERIFIED_PENDING_PR candidate ready for human review."},
                    "summary": {"type": "string"},
                },
                "required": ["pipelines_checked", "incidents_found", "repairs_verified_pending_pr", "summary"],
            },
        },
    }


def _mcp_tool_to_spec(tool) -> dict:
    """Convert an MCP SDK Tool object (name/description/input_schema) into the same
    chat.completions-shaped tool spec every agent loop in this codebase already builds --
    see src/model_client.py's OpenAIResponsesModelClient, which does the reverse translation
    for the Responses API. This is the one place an MCP-native schema crosses into this
    codebase's own tool-spec convention."""
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": tool.input_schema,
        },
    }


@asynccontextmanager
async def _in_process_mcp_client(server):
    """Connect a real MCP ClientSession to an in-process MCPServer over in-memory transport
    streams -- see module docstring for why this reaches into MCPServer's private
    `_lowlevel_server` attribute."""
    from mcp.client.session import ClientSession
    from mcp.shared.memory import create_client_server_memory_streams

    async with create_client_server_memory_streams() as (client_streams, server_streams):
        client_read, client_write = client_streams
        server_read, server_write = server_streams
        init_options = server._lowlevel_server.create_initialization_options()
        server_task = asyncio.create_task(server._lowlevel_server.run(server_read, server_write, init_options))
        try:
            async with ClientSession(client_read, client_write) as session:
                await session.initialize()
                yield session
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


def _tool_result_to_content_str(result) -> str:
    """Extract the tool's JSON payload as a string for the `tool`-role message content --
    exactly what dispatch_tool's callers already do with json.dumps(result) for direct
    Python calls (see src/lifecycle_diagnosis_agent.py); here the payload already arrived as
    MCP TextContent, so we just recover the raw text (or synthesize an error string on an
    MCP-level failure, which none of this codebase's own tools raise -- see
    src.mcp_servers.*._tool_error_to_dict -- but a malformed argument caught by the SDK's own
    schema validation could)."""
    if result.is_error:
        parts = [block.text for block in result.content if hasattr(block, "text")]
        return json.dumps({"error": " ".join(parts) or "tool call failed"})
    parts = [block.text for block in result.content if hasattr(block, "text")]
    return "\n".join(parts) if parts else "{}"


@dataclass
class CodexMcpLoopResult:
    run_id: str
    stages: list = field(default_factory=list)
    final_report: dict | None = None

    def to_manifest(self) -> dict:
        # Matches demo.enterprise_incident's {"run_id", ..., "stages": [...]} shape --
        # the audit-artifact convention every existing demo run manifest already follows.
        return {"run_id": self.run_id, "backend": "codex_mcp", "stages": self.stages, "final_report": self.final_report}


async def run_codex_mcp_loop_async(
    pipeline_names: list[str],
    model_client: DiagnosisModelClient,
    data_ops_server,
    spark_runtime_server,
    *,
    max_turns: int = DEFAULT_MAX_TURNS,
) -> CodexMcpLoopResult:
    """Run the morning loop for `pipeline_names`, dispatching every tool call through a real
    MCP ClientSession connected to each of the two given (already-built) MCPServer objects.
    Returns a CodexMcpLoopResult recording every tool call made (for the Run Details view)
    and the model's final structured report. Raises ModelClientError if the model does not
    call report_morning_loop_summary within max_turns."""
    run_id = uuid.uuid4().hex[:12]
    result = CodexMcpLoopResult(run_id=run_id)

    async with _in_process_mcp_client(data_ops_server) as data_ops_client, _in_process_mcp_client(
        spark_runtime_server
    ) as spark_runtime_client:
        data_ops_tools = (await data_ops_client.list_tools()).tools
        spark_runtime_tools = (await spark_runtime_client.list_tools()).tools

        client_by_tool_name = {tool.name: data_ops_client for tool in data_ops_tools}
        client_by_tool_name.update({tool.name: spark_runtime_client for tool in spark_runtime_tools})
        tool_specs = [_mcp_tool_to_spec(t) for t in (*data_ops_tools, *spark_runtime_tools)] + [_report_tool_spec()]

        messages: list[dict] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps({"pipelines_to_check": pipeline_names})},
        ]

        for _ in range(max_turns):
            response = model_client.send(messages, tool_specs)
            messages.append(
                {"role": "assistant", "tool_calls": [_serialize_tool_call(call) for call in response.tool_calls]}
            )

            report: ToolCall | None = None
            for call in response.tool_calls:
                if call.name == REPORT_TOOL_NAME:
                    report = call
                    messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps({"received": True})})
                    continue

                client = client_by_tool_name.get(call.name)
                if client is None:
                    content = json.dumps({"error": f"unknown tool {call.name!r}"})
                else:
                    mcp_result = await client.call_tool(call.name, call.arguments)
                    content = _tool_result_to_content_str(mcp_result)
                result.stages.append({"tool": call.name, "arguments": call.arguments, "result": content})
                messages.append({"role": "tool", "tool_call_id": call.id, "content": content})

            if report is not None:
                result.final_report = report.arguments
                return result

    raise ModelClientError(f"Codex/MCP loop did not call {REPORT_TOOL_NAME} within {max_turns} turns")


def run_codex_mcp_loop(
    pipeline_names: list[str],
    model_client: DiagnosisModelClient,
    data_ops_server,
    spark_runtime_server,
    *,
    max_turns: int = DEFAULT_MAX_TURNS,
) -> CodexMcpLoopResult:
    """Synchronous convenience wrapper around run_codex_mcp_loop_async -- every other agent
    loop and CLI entry point in this codebase is synchronous, so callers (including
    src/api.py, a future Run Details endpoint, and this module's own main()) don't need to
    know this loop is async internally."""
    return asyncio.run(
        run_codex_mcp_loop_async(pipeline_names, model_client, data_ops_server, spark_runtime_server, max_turns=max_turns)
    )


def run_and_persist_codex_mcp_loop(pipeline_names: list[str], storage, model_client_factory, *, max_turns: int = DEFAULT_MAX_TURNS) -> dict:
    """Production entry point: builds both real MCP servers, runs the loop, and persists the
    run manifest exactly the way demo.enterprise_incident already persists a demo run --
    locally addressable at curated/demo_runs/<run_id>.json plus a curated/demo_run_latest.json
    convenience copy -- so the Run Details view can show a codex_mcp run the same way it shows
    the existing direct-call demo runs."""
    from src.mcp_servers.data_ops_server import build_data_ops_mcp_server
    from src.mcp_servers.spark_runtime_server import build_spark_runtime_mcp_server

    data_ops_server = build_data_ops_mcp_server()
    spark_runtime_server = build_spark_runtime_mcp_server()
    result = run_codex_mcp_loop(pipeline_names, model_client_factory(), data_ops_server, spark_runtime_server, max_turns=max_turns)

    manifest = result.to_manifest()
    storage.write_json(f"curated/demo_runs/{result.run_id}.json", manifest)
    storage.write_json("curated/demo_run_latest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None):
    import argparse

    from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY

    parser = argparse.ArgumentParser(description="Run the Codex/MCP morning data-operations loop.")
    parser.add_argument("--pipeline", action="append", dest="pipelines", choices=sorted(PIPELINE_REGISTRY), help="Repeatable. Defaults to every registered pipeline.")
    parser.add_argument("--scripted-model", action="store_true", help="Use a free, offline scripted model client instead of a real OpenAI call.")
    parser.add_argument("--max-turns", type=int, default=DEFAULT_MAX_TURNS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
    from src.model_client import OpenAIDiagnosisModelClient
    from src.storage import S3Storage

    args = parse_args(argv)
    pipeline_names = args.pipelines or sorted(PIPELINE_REGISTRY)

    if args.scripted_model:
        raise SystemExit("--scripted-model requires programmatically supplying a ScriptedDiagnosisModelClient's responses; use run_codex_mcp_loop directly (see tests/test_codex_mcp_loop.py) rather than this CLI for a scripted run.")

    def model_client_factory():
        return OpenAIDiagnosisModelClient()

    manifest = run_and_persist_codex_mcp_loop(pipeline_names, S3Storage(), model_client_factory, max_turns=args.max_turns)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
