"""Data Operations MCP server: context, validation, and the two-phase candidate-repair flow.

DataOpsTools wraps existing, unmodified functions directly:
- get_data_product_context/get_metric_context/get_lineage/get_runtime_health/
  get_relevant_pipeline_code are one-line ContextRetriever wraps (src/context_retriever.py
  already has get_pipeline_metadata/get_metric/get_lineage/get_runtime_health/get_relevant_code).
- run_data_product_validation is a fresh call to the pipeline's own run_validate closure
  (the same first few lines src/lifecycle_diagnose_pipeline.run_diagnose_pipeline already does).
- create_candidate_repair / verify_candidate_repair split
  src.lifecycle_run_self_healing.run_lifecycle_self_healing's existing mode="propose_patch"
  (diagnose + apply, stop before verify) from its verify step, handing the resumable state
  in between to a StateStore (src/platform_backends/state_store.py) keyed by repair_id. The
  sandbox backend used for apply is TempDirSandbox (stateless -- mode="propose_patch" always
  selects it, see run_lifecycle_self_healing), so it is trivially reconstructed fresh in
  verify_candidate_repair without needing to persist any sandbox object state. Verifying with
  mode="create_pr" against a TempDirSandbox is an already-supported path: src.pr_artifact
  builds its own throwaway branch/commit when no GitWorktreeSandbox branch is available (see
  src.lifecycle_verify_repair._commit_patch_and_keep_branch) -- exactly what a plain
  mode="create_pr" call without GitWorktreeSandbox already did before that wiring existed.
- get_pr_ready_artifact reads one curated/pending_repairs/<pipeline>.json record -- the same
  file src.data_ops.list_pending_repairs already reads, just for one named pipeline instead
  of "all pending".

Nothing here promotes to the real repository/curated data automatically: create_candidate_repair
never verifies, and verify_candidate_repair only ever reaches VERIFIED_PENDING_PR (a local,
unpushed branch) or NOT_VERIFIED/BLOCKED -- promotion still requires the existing, separate
human accept action (src.data_ops.accept_repair).
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Callable

from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.dataset_registry_tools import ToolError
from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.lifecycle_run_self_healing import run_lifecycle_self_healing
from src.lifecycle_verify_repair import run_verify_lifecycle_repair
from src.model_client import DiagnosisModelClient
from src.platform_backends.state_store import StateStore
from src.sandbox.backend import TempDirSandbox
from src.storage import S3Storage

REPAIR_STATE_KEY_PREFIX = "repairs/"


def _pending_repair_key(pipeline_name: str) -> str:
    # Mirrors src.data_ops._pending_repair_key exactly -- the same file the console's
    # Repairs/Run Details view and src.data_ops.accept_repair/reject_repair already read.
    return f"curated/pending_repairs/{pipeline_name}.json"


def _require_known_pipeline(pipeline_name: str) -> None:
    if pipeline_name not in PIPELINE_REGISTRY:
        raise ToolError(f"unknown pipeline_name {pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")


def _fact_dict(fact) -> dict:
    return fact.model_dump(mode="json")


@dataclass
class DataOpsTools:
    storage: S3Storage
    context_retriever: ContextRetriever
    state_store: StateStore
    diagnosis_model_client_factory: Callable[[], DiagnosisModelClient]
    repair_model_client_factory: Callable[[], DiagnosisModelClient]
    spark_factory: Callable[[], "SparkSession"]  # noqa: F821 -- only ever invoked lazily, inside create/verify

    def get_data_product_context(self, pipeline_name: str) -> dict:
        _require_known_pipeline(pipeline_name)
        return _fact_dict(self.context_retriever.get_pipeline_metadata(pipeline_name, self.storage))

    def get_metric_context(self, pipeline_name: str, metric_name: str) -> dict:
        _require_known_pipeline(pipeline_name)
        return _fact_dict(self.context_retriever.get_metric(pipeline_name, metric_name, self.storage))

    def get_lineage(self, pipeline_name: str) -> dict:
        _require_known_pipeline(pipeline_name)
        return _fact_dict(self.context_retriever.get_lineage(pipeline_name, self.storage))

    def get_runtime_health(self, pipeline_name: str) -> dict:
        _require_known_pipeline(pipeline_name)
        return _fact_dict(self.context_retriever.get_runtime_health(pipeline_name, self.storage))

    def get_relevant_pipeline_code(self, pipeline_name: str) -> dict:
        _require_known_pipeline(pipeline_name)
        return _fact_dict(self.context_retriever.get_relevant_code(pipeline_name, self.storage))

    def run_data_product_validation(self, pipeline_name: str) -> dict:
        _require_known_pipeline(pipeline_name)
        spec = PIPELINE_REGISTRY[pipeline_name]
        business_rules = self.storage.read_json("context/business_rules.json")
        validation_rules = self.storage.read_json(spec.validation_rules_key)
        return spec.run_validate(self.storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

    def create_candidate_repair(self, pipeline_name: str) -> dict:
        """Diagnose + apply a bounded repair in an isolated sandbox, stop before verifying.
        Returns a repair_id that verify_candidate_repair later resumes from -- nothing here
        touches the real repository or curated data."""
        _require_known_pipeline(pipeline_name)
        spec = PIPELINE_REGISTRY[pipeline_name]
        business_rules = self.storage.read_json("context/business_rules.json")
        validation_rules = self.storage.read_json(spec.validation_rules_key)
        validation_before = spec.run_validate(self.storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

        spark = self.spark_factory()
        result = run_lifecycle_self_healing(
            pipeline_name,
            spark,
            self.storage,
            self.diagnosis_model_client_factory,
            self.repair_model_client_factory,
            mode="propose_patch",
        )
        repair_id = result["run_id"]
        self.state_store.set(
            f"{REPAIR_STATE_KEY_PREFIX}{repair_id}",
            {
                "repair_id": repair_id,
                "pipeline_name": pipeline_name,
                "status": "AWAITING_VERIFICATION",
                "diagnosis": result["diagnosis"],
                "repair_plan": result["repair_plan"],
                "repair_result": result["repair_result"],
                "validation_before": validation_before,
                "business_rules": business_rules,
                "validation_rules": validation_rules,
            },
        )
        return {
            "repair_id": repair_id,
            "pipeline_name": pipeline_name,
            "diagnosis": result["diagnosis"],
            "repair_plan": result["repair_plan"],
            "repair_status": (result["repair_result"] or {}).get("repair_status"),
        }

    def verify_candidate_repair(self, repair_id: str) -> dict:
        """Resume a candidate created by create_candidate_repair: rerun Spark against the
        isolated candidate output, run deterministic validators/tests, and -- only on a full
        pass -- produce a local, unpushed VERIFIED_PENDING_PR artifact (never promotes)."""
        record = self.state_store.get(f"{REPAIR_STATE_KEY_PREFIX}{repair_id}")
        if record is None:
            raise ToolError(f"unknown repair_id {repair_id!r}; call create_candidate_repair first")

        pipeline_name = record["pipeline_name"]
        spark = self.spark_factory()
        verification = run_verify_lifecycle_repair(
            pipeline_name,
            spark,
            self.storage,
            record["business_rules"],
            record["validation_rules"],
            record["validation_before"],
            record["repair_result"],
            run_id=repair_id,
            mode="create_pr",
            diagnosis=record["diagnosis"],
            repair_plan=record["repair_plan"],
            sandbox_backend=TempDirSandbox(),
        )
        record["repair_verification"] = verification
        record["status"] = verification["verification_status"]
        self.state_store.set(f"{REPAIR_STATE_KEY_PREFIX}{repair_id}", record)

        if verification.get("verification_status") == "VERIFIED_PENDING_PR":
            self.storage.write_json(
                _pending_repair_key(pipeline_name),
                {
                    "pipeline_name": pipeline_name,
                    "status": "pending_review",
                    "pr_artifact": verification.get("pr_artifact"),
                    "diagnosis": record["diagnosis"],
                },
            )

        return {
            "repair_id": repair_id,
            "pipeline_name": pipeline_name,
            "verification_status": verification["verification_status"],
            "summary": verification["summary"],
            "failed_checks_before": verification["failed_checks_before"],
            "failed_checks_after": verification["failed_checks_after"],
            "tests": verification["tests"],
            "branch": (verification.get("pr_artifact") or {}).get("branch"),
        }

    def get_pr_ready_artifact(self, pipeline_name: str) -> dict:
        _require_known_pipeline(pipeline_name)
        key = _pending_repair_key(pipeline_name)
        if not self.storage.exists(key):
            return {"pipeline_name": pipeline_name, "pending": False, "pr_artifact": None}
        record = self.storage.read_json(key)
        return {"pipeline_name": pipeline_name, "pending": True, **record}


def _default_diagnosis_model_client_factory() -> DiagnosisModelClient:
    from src.data_ops import _default_model_client_factory

    return _default_model_client_factory()


def _default_repair_model_client_factory() -> DiagnosisModelClient:
    from src.ask_lifecycle import _repair_model_client_factory

    return _repair_model_client_factory()


def build_default_data_ops_tools() -> DataOpsTools:
    from src.config import get_state_store
    from src.spark_session import get_spark_session

    return DataOpsTools(
        storage=S3Storage(),
        context_retriever=ContextRetriever(store=FileContextStore()),
        state_store=get_state_store(),
        diagnosis_model_client_factory=_default_diagnosis_model_client_factory,
        repair_model_client_factory=_default_repair_model_client_factory,
        spark_factory=lambda: get_spark_session("mcp-data-ops"),
    )


def _tool_error_to_dict(fn):
    """Wraps a bound tool method so a ToolError becomes a small structured error dict instead
    of an exception -- matching this codebase's existing dispatch_tool convention
    (src/lifecycle_diagnostic_tools.py). functools.wraps preserves fn's real signature (via
    __wrapped__) so the MCP SDK can still build an accurate JSON schema from it."""

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ToolError as exc:
            return {"error": str(exc)}

    return wrapped


def build_data_ops_mcp_server(tools: DataOpsTools | None = None):
    """Build the real MCP server object (uses the `mcp` SDK). `tools` is injectable for
    tests; production/deployment code leaves it unset (build_default_data_ops_tools())."""
    from mcp.server.mcpserver import MCPServer

    tools = tools or build_default_data_ops_tools()
    server = MCPServer(
        name="data-ops",
        description="Context, validation, and candidate-repair tools for the lending data-operations demo.",
    )

    server.add_tool(
        _tool_error_to_dict(tools.get_data_product_context),
        name="get_data_product_context",
        description="Return this data product's pipeline metadata (ETL source file, functions) and its context provenance.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_metric_context),
        name="get_metric_context",
        description="Return one metric's definition, provenance, review status, and any unresolved conflict with the code.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_lineage),
        name="get_lineage",
        description="Return this data product's lineage (producer, upstream dependencies).",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_runtime_health),
        name="get_runtime_health",
        description="Return this data product's last known ETL/validation health status.",
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_relevant_pipeline_code),
        name="get_relevant_pipeline_code",
        description="Return the source of every ETL function that computes this data product's curated output(s).",
    )
    server.add_tool(
        _tool_error_to_dict(tools.run_data_product_validation),
        name="run_data_product_validation",
        description="Run this data product's independent validator fresh and return the full result (all checks, not just failures).",
    )
    server.add_tool(
        _tool_error_to_dict(tools.create_candidate_repair),
        name="create_candidate_repair",
        description=(
            "Diagnose this data product's failure and apply a bounded repair inside an isolated sandbox. "
            "Stops before verifying -- the real repository and curated data are never touched. "
            "Returns a repair_id; call verify_candidate_repair(repair_id) next."
        ),
    )
    server.add_tool(
        _tool_error_to_dict(tools.verify_candidate_repair),
        name="verify_candidate_repair",
        description=(
            "Rerun Spark against the isolated candidate output from create_candidate_repair and run deterministic "
            "validators/tests. On a full pass, produces a local, unpushed VERIFIED_PENDING_PR artifact -- never "
            "promotes automatically."
        ),
    )
    server.add_tool(
        _tool_error_to_dict(tools.get_pr_ready_artifact),
        name="get_pr_ready_artifact",
        description="Return the pending PR-ready review artifact for this data product, if one exists (branch, diff, diagnosis, before/after checks).",
    )
    return server


def main() -> None:
    """Run this server standalone over streamable-HTTP -- how it runs as a deployed RHOAI
    service (see deploy/rhoai/mcp-data-ops-deployment.yaml). Locally/in tests, callers use
    build_data_ops_mcp_server() directly (in-process, no network) instead of this."""
    import os

    server = build_data_ops_mcp_server()
    server.run(transport="streamable-http", host=os.environ.get("MCP_HOST", "0.0.0.0"), port=int(os.environ.get("MCP_PORT", "8000")))


if __name__ == "__main__":
    main()
