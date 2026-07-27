"""Deterministic repair application for any of the 5 lifecycle pipelines: eligibility gate
-> repair agent planning -> policy validation -> isolated-workspace patch application.
Parallel to src/apply_repair.py (left completely unmodified) for the S3-backed lifecycle
model. Generalized (via src/lifecycle_pipeline_registry.py) rather than hardcoded to
loan_portfolio -- each pipeline still has exactly one repairable target file (its ETL
source), so no manifest-file abstraction is needed.

Reuses the fully generic pieces of apply_repair.py directly (pure functions, zero
modification): _create_isolated_workspace, _validate_and_apply_patch, _workspace_path,
_sha256_of_file, PatchApplyError, load_repair_targets. Only the tool-surface/data-loading
front end differs (S3-backed business rules/lineage/metrics instead of local scenario files).

This function is pure (returns data, does not write to S3) -- src/lifecycle_run_self_healing.py
is responsible for persisting repair artifacts as part of a full self-healing run; this
module's own main() persists a "latest" convenience copy only when run standalone.

This module never decides whether a repair SUCCEEDED -- only that it was safely applied to
an isolated COPY of its target file. src/lifecycle_verify_repair.py is the only thing that
may mark a repair VERIFIED and promote it into the real repository.
"""

from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
from typing import Callable

from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.legacy.apply_repair import (
    DEFAULT_REPAIR_TARGETS_FILE,
    PatchApplyError,
    _create_isolated_workspace,
    _sha256_of_file,
    _validate_and_apply_patch,
    _workspace_path,
    load_repair_targets,
)
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.lifecycle_repair_agent import RepairAgentError, run_lifecycle_repair_planning
from src.lifecycle_repair_tools import LifecycleRepairTools
from src.model_client import (
    DiagnosisModelClient,
    ModelClientError,
    OpenAIDiagnosisModelClient,
    OpenAIResponsesModelClient,
)
from src.sandbox.backend import SandboxBackend, TempDirSandbox
from src.legacy.repair_models import (
    RepairDecision,
    RepairEligibility,
    RepairPlanValidationError,
    build_blocked_repair_plan,
    build_no_repair_needed_plan,
    evaluate_repair_eligibility,
    repair_plan_to_dict,
)
from src.storage import S3Storage

DEFAULT_CONFIDENCE_THRESHOLD = "HIGH"
REPAIR_MODEL_ENV_VAR = "REPAIR_MODEL"


class ApplyLifecycleRepairError(Exception):
    """Application-level failure: model/API failure or malformed model output."""


def build_lifecycle_repair_tools(
    pipeline_name: str, storage: S3Storage, diagnosis: dict, validation_results: dict, allowed_targets: dict
) -> LifecycleRepairTools:
    spec = PIPELINE_REGISTRY[pipeline_name]
    business_rules = storage.read_json("context/business_rules.json")
    lineage = storage.read_json("context/lineage.json")
    metrics = storage.read_json(spec.metrics_key)
    module_name = spec.etl_source_file.replace("/", ".").removesuffix(".py")
    etl_module = importlib.import_module(module_name)
    etl_functions = {name: getattr(etl_module, name) for name in spec.etl_function_names}
    return LifecycleRepairTools(
        diagnosis=diagnosis,
        validation_results=validation_results,
        business_rules=business_rules,
        lineage=lineage,
        metrics=metrics,
        allowed_repair_targets=allowed_targets,
        test_inventory=[spec.test_file],
        lineage_key=spec.lineage_key,
        etl_source_file=spec.etl_source_file,
        etl_functions=etl_functions,
        pipeline_name=pipeline_name,
        storage=storage,
        context_retriever=ContextRetriever(store=FileContextStore()),
    )


def _outcome_result(reason: str, *, repair_status: str) -> dict:
    return {
        "repair_status": repair_status,
        "repair_type": "NONE",
        "target_file": None,
        "changed_files": [],
        "original_hashes": {},
        "repaired_hashes": {},
        "plan_policy_status": "FAIL" if repair_status in ("BLOCKED", "FAILED") else "PASS",
        "application_details": reason,
        "error": None,
        "workspace_dir": None,
    }


def run_apply_lifecycle_repair(
    pipeline_name: str,
    storage: S3Storage,
    diagnosis: dict,
    validation_results: dict,
    model_client_factory: Callable[[], DiagnosisModelClient],
    *,
    repair_targets_file: str = DEFAULT_REPAIR_TARGETS_FILE,
    confidence_threshold: str = DEFAULT_CONFIDENCE_THRESHOLD,
    sandbox_backend: SandboxBackend = TempDirSandbox(),
) -> tuple[dict, dict]:
    """Run the full apply-repair flow for one lifecycle pipeline. Returns
    (repair_plan_dict, repair_result_dict).

    sandbox_backend defaults to TempDirSandbox -- byte-identical to this function's original
    behavior (a bare tempfile.mkdtemp() copy of just the target file). Passing a
    GitWorktreeSandbox instead (see src.lifecycle_run_self_healing's mode="create_pr" wiring)
    applies this same patch inside a real git worktree/branch, so the exact workspace this
    function produces is what src.lifecycle_verify_repair reruns Spark against and what
    eventually becomes the PR branch -- no second, redundant workspace."""
    spec = PIPELINE_REGISTRY[pipeline_name]
    diagnosis_reference = diagnosis.get("incident_summary", pipeline_name)

    eligibility = evaluate_repair_eligibility(
        diagnosis,
        allowed_target_files={spec.etl_source_file},
        confidence_threshold=confidence_threshold,
    )

    if eligibility.decision == RepairEligibility.NO_REPAIR_NEEDED:
        plan = build_no_repair_needed_plan(incident_id=pipeline_name, diagnosis_reference=diagnosis_reference)
        return repair_plan_to_dict(plan), _outcome_result("; ".join(eligibility.reasons), repair_status="NO_REPAIR")

    if eligibility.decision in (RepairEligibility.HUMAN_REVIEW_REQUIRED, RepairEligibility.INVALID_DIAGNOSIS):
        plan = build_blocked_repair_plan(
            "; ".join(eligibility.reasons), incident_id=pipeline_name, diagnosis_reference=diagnosis_reference
        )
        return repair_plan_to_dict(plan), _outcome_result("; ".join(eligibility.reasons), repair_status="BLOCKED")

    # ELIGIBLE_FOR_REPAIR: proceed to the repair model.
    try:
        allowed_targets = load_repair_targets(Path(repair_targets_file))
        tools = build_lifecycle_repair_tools(pipeline_name, storage, diagnosis, validation_results, allowed_targets)
        starting_context = {
            "diagnosis_status": diagnosis["diagnosis_status"],
            "root_cause_category": diagnosis["root_cause_category"],
            "root_cause": diagnosis["root_cause"],
            "initiating_event": diagnosis.get("initiating_event"),
            "affected_metrics": diagnosis.get("affected_metrics", []),
            "confidence": diagnosis["confidence"],
            "recommended_fix": diagnosis.get("recommended_fix"),
        }
        model_client = model_client_factory()
        plan = run_lifecycle_repair_planning(
            starting_context, tools, model_client, diagnosis=diagnosis, allowed_targets=allowed_targets
        )
    except (FileNotFoundError, ValueError, KeyError, RepairAgentError, RepairPlanValidationError, ModelClientError) as exc:
        raise ApplyLifecycleRepairError(str(exc)) from exc

    plan_dict = repair_plan_to_dict(plan)

    if plan.repair_decision != RepairDecision.PROPOSE_REPAIR:
        repair_status = "NO_REPAIR" if plan.repair_decision == RepairDecision.NO_SAFE_REPAIR else "BLOCKED"
        return plan_dict, _outcome_result(plan.change_description, repair_status=repair_status)

    # PROPOSE_REPAIR: apply in an isolated workspace, never the real file.
    original_hash = _sha256_of_file(Path(plan.target_file))
    try:
        workspace_dir = sandbox_backend.create_workspace(plan.target_file)
        # _validate_and_apply_patch (src.legacy.apply_repair) computes the target's in-workspace
        # path via the same workspace_dir / target_file.lstrip("/") formula every SandboxBackend
        # uses (see src.sandbox.backend) -- both TempDirSandbox and GitWorktreeSandbox already
        # place the target file at exactly that path, so no backend-specific wiring is needed here.
        _validate_and_apply_patch(workspace_dir, plan)
    except (PatchApplyError, OSError, json.JSONDecodeError) as exc:
        return plan_dict, _outcome_result(f"policy validation / patch application failed: {exc}", repair_status="BLOCKED")

    repaired_hash = _sha256_of_file(sandbox_backend.workspace_path(workspace_dir, plan.target_file))

    result = {
        "repair_status": "APPLIED",
        "repair_type": plan.repair_type.value,
        "target_file": plan.target_file,
        "changed_files": [plan.target_file],
        "original_hashes": {plan.target_file: original_hash},
        "repaired_hashes": {plan.target_file: repaired_hash},
        "plan_policy_status": "PASS",
        "application_details": f"Applied {plan.repair_type.value} to {plan.target_file} in isolated workspace {workspace_dir}",
        "error": None,
        "workspace_dir": str(workspace_dir),
    }
    return plan_dict, result


def print_repair_result(result: dict) -> None:
    print("Repair application")
    print(f"  repair_status:       {result['repair_status']}")
    print(f"  repair_type:         {result['repair_type']}")
    print(f"  target_file:         {result['target_file']}")
    print(f"  plan_policy_status:  {result['plan_policy_status']}")
    print(f"  application_details: {result['application_details']}")


def main(argv: list[str] | None = None) -> None:
    import argparse

    from src.lifecycle_diagnose_pipeline import run_diagnose_pipeline
    from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE

    parser = argparse.ArgumentParser(description="Diagnose-and-plan-a-repair for one lifecycle pipeline.")
    parser.add_argument("pipeline_name", choices=sorted(PIPELINE_REGISTRY))
    args = parser.parse_args(argv)

    model_name = os.environ.get(REPAIR_MODEL_ENV_VAR)

    def diagnosis_model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient()

    def repair_model_client_factory() -> DiagnosisModelClient:
        # Repair planning uses a Codex-branded model (only available via the Responses API,
        # not chat.completions -- see src/model_client.py's OpenAIResponsesModelClient
        # docstring) by default; REPAIR_MODEL can override to any other Responses-API model.
        return OpenAIResponsesModelClient(model=model_name) if model_name else OpenAIResponsesModelClient()

    storage = S3Storage()
    spec = PIPELINE_REGISTRY[args.pipeline_name]
    diagnosis = run_diagnose_pipeline(args.pipeline_name, storage, diagnosis_model_client_factory)
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json(spec.validation_rules_key)
    validation_results = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

    try:
        plan_dict, result = run_apply_lifecycle_repair(
            args.pipeline_name, storage, diagnosis, validation_results, repair_model_client_factory
        )
    except ApplyLifecycleRepairError as exc:
        print(f"Repair application failed: {exc}")
        raise SystemExit(1)

    storage.write_json(f"curated/{args.pipeline_name}_repair_plan.json", plan_dict)
    storage.write_json(f"curated/{args.pipeline_name}_repair_result.json", result)
    print_repair_result(result)


if __name__ == "__main__":
    main()
