"""Compose diagnose -> repair-plan -> apply (isolated) -> verify for any of the 5 lifecycle
pipelines. Parallel to src/run_self_healing.py (left completely unmodified) for the
S3-backed lifecycle model. Only src/lifecycle_verify_repair.py's deterministic rerun may
mark the outcome VERIFIED and promote it into the real repository/bucket.

The 3 stage modules (diagnose/apply/verify) are pure -- they return data, they don't write
audit artifacts to S3. This module is the single place that persists them: a run-specific
copy under curated/self_heal_runs/<pipeline_name>/<run_id>/ (a real, growing audit trail
across every healing attempt, not just the latest) plus a "latest" convenience copy at
curated/<pipeline_name>_<artifact>.json for quick inspection without knowing a run_id.
"""

from __future__ import annotations

import uuid
from typing import Callable

from pyspark.sql import SparkSession

from src.lifecycle_apply_repair import run_apply_lifecycle_repair
from src.lifecycle_diagnose_pipeline import run_diagnose_pipeline
from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.lifecycle_verify_repair import run_verify_lifecycle_repair
from src.model_client import DiagnosisModelClient
from src.sandbox.backend import GitWorktreeSandbox, TempDirSandbox
from src.storage import S3Storage


def _persist_run_artifacts(storage: S3Storage, pipeline_name: str, run_id: str, artifacts: dict) -> None:
    for artifact_name, content in artifacts.items():
        storage.write_json(f"curated/self_heal_runs/{pipeline_name}/{run_id}/{artifact_name}.json", content)
        storage.write_json(f"curated/{pipeline_name}_{artifact_name}.json", content)


def run_lifecycle_self_healing(
    pipeline_name: str,
    spark: SparkSession,
    storage: S3Storage,
    diagnosis_model_client_factory: Callable[[], DiagnosisModelClient],
    repair_model_client_factory: Callable[[], DiagnosisModelClient],
    *,
    mode: str = "auto_promote",
) -> dict:
    """Diagnose, plan a repair, apply it in isolation, and verify it against real raw data.

    mode: "diagnose_only" stops after diagnosis (repair_plan/repair_result/repair_verification
    are all None). "propose_patch" stops after apply_repair (repair_verification is None).
    "create_pr" runs the full flow but has verify build a local PR artifact instead of
    promoting on a full pass (see src.lifecycle_verify_repair's mode parameter).
    "auto_promote" (the default, and every existing call site's behavior) runs the full flow
    and promotes directly on a full pass -- unchanged from before mode existed.

    Returns {"run_id":..., "diagnosis":..., "repair_plan":..., "repair_result":...,
    "repair_verification":...}. Raises whatever the underlying stages raise
    (DiagnosePipelineError, ApplyLifecycleRepairError) on a genuine application-level
    failure -- a BLOCKED or NOT_VERIFIED outcome is a normal, successful return, not an
    exception.
    """
    if mode not in ("diagnose_only", "propose_patch", "create_pr", "auto_promote"):
        raise ValueError(f"unknown mode {mode!r}")

    spec = PIPELINE_REGISTRY[pipeline_name]
    run_id = uuid.uuid4().hex[:12]
    # create_pr is the only mode that needs a real git identity for the candidate -- every
    # other mode (including the default, auto_promote) keeps today's exact TempDirSandbox
    # behavior. The SAME backend instance is threaded into apply and verify below so the
    # workspace apply patches is the one verify reruns Spark against and the one that becomes
    # the PR branch, not two disconnected ones.
    sandbox_backend = GitWorktreeSandbox() if mode == "create_pr" else TempDirSandbox()

    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json(spec.validation_rules_key)
    validation_before = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

    diagnosis = run_diagnose_pipeline(pipeline_name, storage, diagnosis_model_client_factory)
    if mode == "diagnose_only":
        artifacts = {"diagnosis": diagnosis, "repair_plan": None, "repair_result": None, "repair_verification": None}
        _persist_run_artifacts(storage, pipeline_name, run_id, {"diagnosis": diagnosis})
        return {"run_id": run_id, **artifacts}

    repair_plan, repair_result = run_apply_lifecycle_repair(
        pipeline_name, storage, diagnosis, validation_before, repair_model_client_factory, sandbox_backend=sandbox_backend
    )
    if mode == "propose_patch":
        artifacts = {"diagnosis": diagnosis, "repair_plan": repair_plan, "repair_result": repair_result, "repair_verification": None}
        _persist_run_artifacts(storage, pipeline_name, run_id, {"diagnosis": diagnosis, "repair_plan": repair_plan, "repair_result": repair_result})
        return {"run_id": run_id, **artifacts}

    verify_kwargs = {"run_id": run_id, "sandbox_backend": sandbox_backend}
    if mode == "create_pr":
        verify_kwargs.update(mode="create_pr", diagnosis=diagnosis, repair_plan=repair_plan)
    repair_verification = run_verify_lifecycle_repair(
        pipeline_name, spark, storage, business_rules, validation_rules, validation_before, repair_result, **verify_kwargs
    )

    artifacts = {
        "diagnosis": diagnosis,
        "repair_plan": repair_plan,
        "repair_result": repair_result,
        "repair_verification": repair_verification,
    }
    _persist_run_artifacts(storage, pipeline_name, run_id, artifacts)

    return {"run_id": run_id, **artifacts}
