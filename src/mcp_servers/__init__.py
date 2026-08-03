"""MCP servers exposing this codebase's existing context/validation/repair machinery as MCP
tools -- "RHOAI exposes the platform capabilities; MCP makes them available; Codex gathers
the context and manages the workflow." Every tool here wraps an existing, unmodified function
or class (ContextRetriever, PipelineSpec.run_validate, run_lifecycle_self_healing,
run_verify_lifecycle_repair, src.platform_backends) -- no new diagnosis, repair, validation,
or sandboxing logic lives in this package.

Each module exposes a plain, undecorated tools class (e.g. DataOpsTools) alongside its MCP
registration -- tests call the plain class directly, bypassing MCP transport entirely, the
same way this codebase already tests LifecycleDiagnosticTools/dispatch_tool. A server built
with build_*_mcp_server() is the thing actually deployed (locally over stdio, or on RHOAI over
streamable HTTP -- see deploy/rhoai/).
"""
