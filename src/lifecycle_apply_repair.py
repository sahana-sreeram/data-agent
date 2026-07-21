"""Deterministic repair application for the loan_portfolio lifecycle pipeline: eligibility
gate -> repair agent planning -> policy validation -> isolated-workspace patch application.
Parallel to src/apply_repair.py (left completely unmodified) for the S3-backed lifecycle
model, which has exactly one repairable pipeline/target file (no manifest-file abstraction
needed for a single scenario).

Reuses the fully generic pieces of apply_repair.py directly (pure functions, zero
modification): _create_isolated_workspace, _validate_and_apply_patch, _workspace_path,
_sha256_of_file, PatchApplyError, load_repair_targets. Only the tool-surface/data-loading
front end differs (S3-backed business rules/lineage/metrics instead of local scenario files).

This module never decides whether a repair SUCCEEDED -- only that it was safely applied to
an isolated COPY of its target file. src/lifecycle_verify_repair.py is the only thing that
may mark a repair VERIFIED and promote it into the real repository.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable

from src.apply_repair import (
    DEFAULT_REPAIR_TARGETS_FILE,
    PatchApplyError,
    _create_isolated_workspace,
    _sha256_of_file,
    _validate_and_apply_patch,
    _workspace_path,
    load_repair_targets,
)
from src.lifecycle_repair_agent import RepairAgentError, run_lifecycle_repair_planning
from src.lifecycle_repair_tools import ETL_SOURCE_FILE, LifecycleRepairTools
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.repair_models import (
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
INCIDENT_ID = "loan_portfolio"
TEST_INVENTORY = ["tests/test_etl_spark_loan_portfolio.py"]


class ApplyLifecycleRepairError(Exception):
    """Application-level failure: model/API failure or malformed model output."""


def build_lifecycle_repair_tools(
    storage: S3Storage, diagnosis: dict, validation_results: dict, allowed_targets: dict
) -> LifecycleRepairTools:
    business_rules = storage.read_json("context/business_rules.json")
    lineage = storage.read_json("context/lineage.json")
    metrics = storage.read_json("context/metrics/loan_portfolio.json")
    return LifecycleRepairTools(
        diagnosis=diagnosis,
        validation_results=validation_results,
        business_rules=business_rules,
        lineage=lineage,
        metrics=metrics,
        allowed_repair_targets=allowed_targets,
        test_inventory=TEST_INVENTORY,
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
    storage: S3Storage,
    diagnosis: dict,
    validation_results: dict,
    model_client_factory: Callable[[], DiagnosisModelClient],
    *,
    repair_targets_file: str = DEFAULT_REPAIR_TARGETS_FILE,
    confidence_threshold: str = DEFAULT_CONFIDENCE_THRESHOLD,
) -> tuple[dict, dict]:
    """Run the full apply-repair flow for the loan_portfolio pipeline. Returns
    (repair_plan_dict, repair_result_dict)."""
    diagnosis_reference = diagnosis.get("incident_summary", INCIDENT_ID)

    eligibility = evaluate_repair_eligibility(
        diagnosis,
        allowed_target_files={ETL_SOURCE_FILE},
        confidence_threshold=confidence_threshold,
    )

    if eligibility.decision == RepairEligibility.NO_REPAIR_NEEDED:
        plan = build_no_repair_needed_plan(incident_id=INCIDENT_ID, diagnosis_reference=diagnosis_reference)
        return repair_plan_to_dict(plan), _outcome_result("; ".join(eligibility.reasons), repair_status="NO_REPAIR")

    if eligibility.decision in (RepairEligibility.HUMAN_REVIEW_REQUIRED, RepairEligibility.INVALID_DIAGNOSIS):
        plan = build_blocked_repair_plan(
            "; ".join(eligibility.reasons), incident_id=INCIDENT_ID, diagnosis_reference=diagnosis_reference
        )
        return repair_plan_to_dict(plan), _outcome_result("; ".join(eligibility.reasons), repair_status="BLOCKED")

    # ELIGIBLE_FOR_REPAIR: proceed to the repair model.
    try:
        allowed_targets = load_repair_targets(Path(repair_targets_file))
        tools = build_lifecycle_repair_tools(storage, diagnosis, validation_results, allowed_targets)
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
        workspace_dir = _create_isolated_workspace(plan.target_file)
        _validate_and_apply_patch(workspace_dir, plan)
    except (PatchApplyError, OSError, json.JSONDecodeError) as exc:
        return plan_dict, _outcome_result(f"policy validation / patch application failed: {exc}", repair_status="BLOCKED")

    repaired_hash = _sha256_of_file(_workspace_path(workspace_dir, plan.target_file))

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
    print("Repair application (loan_portfolio)")
    print(f"  repair_status:       {result['repair_status']}")
    print(f"  repair_type:         {result['repair_type']}")
    print(f"  target_file:         {result['target_file']}")
    print(f"  plan_policy_status:  {result['plan_policy_status']}")
    print(f"  application_details: {result['application_details']}")


def main(argv: list[str] | None = None) -> None:
    from src.lifecycle_diagnose_loan_portfolio import run_diagnose_loan_portfolio
    from src.validate_loan_portfolio import validate_loan_portfolio

    model_name = os.environ.get(REPAIR_MODEL_ENV_VAR)

    def diagnosis_model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient()

    def repair_model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=model_name) if model_name else OpenAIDiagnosisModelClient()

    storage = S3Storage()
    diagnosis = run_diagnose_loan_portfolio(storage, diagnosis_model_client_factory)
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json("context/validations/loan_portfolio.json")
    validation_results = validate_loan_portfolio(storage, business_rules, validation_rules)

    try:
        plan_dict, result = run_apply_lifecycle_repair(
            storage, diagnosis, validation_results, repair_model_client_factory
        )
    except ApplyLifecycleRepairError as exc:
        print(f"Repair application failed: {exc}")
        raise SystemExit(1)

    storage.write_json("curated/loan_portfolio_repair_plan.json", plan_dict)
    storage.write_json("curated/loan_portfolio_repair_result.json", result)
    print_repair_result(result)


if __name__ == "__main__":
    main()
