"""Deterministic repair application: eligibility gate -> repair agent planning
-> policy validation -> isolated-workspace patch application.

This module never decides whether a repair SUCCEEDED -- only that it was
safely and correctly APPLIED to an isolated COPY of its target file, never
the real repository file. verify_repair.py is the only thing that may mark a
repair VERIFIED, by rerunning tests/ETL/validation against that isolated
workspace and comparing to the pre-repair baseline.

A blocked or not-needed outcome (HUMAN_REVIEW_REQUIRED, NO_REPAIR_NEEDED,
NO_SAFE_REPAIR) is a normal, successful run of this module, not an error --
only a genuine application failure (missing artifacts, model/API failure,
malformed model output) raises ApplyRepairError.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Callable

from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.legacy.repair_agent import RepairAgentError, run_repair_planning
from src.legacy.repair_models import (
    RepairDecision,
    RepairEligibility,
    RepairPlan,
    RepairPlanValidationError,
    RepairType,
    build_blocked_repair_plan,
    build_no_repair_needed_plan,
    evaluate_repair_eligibility,
    repair_plan_to_dict,
)
from src.legacy.repair_tools import RepairTools

DEFAULT_REPAIR_TARGETS_FILE = "context/repair_targets.json"
DEFAULT_CONFIDENCE_THRESHOLD = "HIGH"
REPAIR_MODEL_ENV_VAR = "REPAIR_MODEL"


class ApplyRepairError(Exception):
    """Application-level failure: missing artifacts, model failure, or malformed model output."""


class PatchApplyError(Exception):
    """Raised when a unified diff or structured edit cannot be safely, unambiguously applied.

    Caught by run_apply_repair and treated as a policy BLOCK, never a crash.
    """


def _find_subsequence(haystack: list, needle: list, start: int = 0):
    """Return the index where needle occurs as a contiguous subsequence of haystack, at/after start, else None."""
    n, m = len(haystack), len(needle)
    if m == 0:
        return start
    for i in range(start, n - m + 1):
        if haystack[i : i + m] == needle:
            return i
    return None


def apply_unified_diff(original_text: str, diff_text: str) -> str:
    """Apply a single-file unified diff, returning the patched text.

    Each hunk is located by the CONTENT of its context/removed lines, not by
    the line numbers declared in its "@@ ... @@" header -- models frequently
    omit those numbers (a bare "@@") or miscount them. This is deliberately
    more like a content-anchored patch than a strict line-oriented one: every
    context/removed line must still exactly match somewhere in the original,
    in order, or this raises PatchApplyError. No fuzzy/approximate matching
    beyond exact line content. Supports multiple hunks. Pure Python -- no
    subprocess, no dependency on a system `patch` binary.
    """
    original_lines = original_text.splitlines()
    trailing_newline = original_text.endswith("\n")

    hunks: list = []
    in_hunk = False
    for line in diff_text.splitlines():
        if line.startswith("@@"):
            hunks.append([])
            in_hunk = True
            continue
        if line.startswith("---") or line.startswith("+++"):
            continue
        if in_hunk:
            hunks[-1].append(line)

    if not hunks:
        raise PatchApplyError("no hunks found in unified diff (expected at least one '@@ ... @@' hunk header)")

    result: list = []
    cursor = 0  # 0-indexed position in original_lines

    for body in hunks:
        anchor = [ln[1:] for ln in body if ln and ln[0] in (" ", "-")]
        if anchor:
            match_index = _find_subsequence(original_lines, anchor, start=cursor)
            if match_index is None:
                raise PatchApplyError(f"could not locate hunk context in original file (starting {anchor[:2]!r})")
        else:
            match_index = cursor  # pure-insertion hunk: applies wherever the cursor currently is

        result.extend(original_lines[cursor:match_index])
        cursor = match_index

        for body_line in body:
            if body_line.startswith("\\"):
                continue  # "\ No newline at end of file" marker -- ignore
            tag, content = (body_line[0], body_line[1:]) if body_line else (" ", "")

            if tag == " ":
                if cursor >= len(original_lines) or original_lines[cursor] != content:
                    found = original_lines[cursor] if cursor < len(original_lines) else "<EOF>"
                    raise PatchApplyError(
                        f"context mismatch at original line {cursor + 1}: expected {content!r}, found {found!r}"
                    )
                result.append(content)
                cursor += 1
            elif tag == "-":
                if cursor >= len(original_lines) or original_lines[cursor] != content:
                    found = original_lines[cursor] if cursor < len(original_lines) else "<EOF>"
                    raise PatchApplyError(
                        f"removal mismatch at original line {cursor + 1}: expected {content!r}, found {found!r}"
                    )
                cursor += 1
            elif tag == "+":
                result.append(content)
            else:
                raise PatchApplyError(f"unrecognized diff line: {body_line!r}")

    result.extend(original_lines[cursor:])
    patched = "\n".join(result)
    if trailing_newline:
        patched += "\n"
    return patched


def apply_structured_config_edit(original_content: dict, operations: list) -> dict:
    """Apply a small, already-validated list of {field, value} operations to a JSON object."""
    patched = dict(original_content)
    for op in operations:
        patched[op["field"]] = op["value"]
    return patched


def load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise ApplyRepairError(f"{label} file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_scenario_manifest(path: Path) -> dict:
    return load_json(path, "scenario manifest")


def load_repair_targets(path: Path) -> dict:
    return load_json(path, "repair targets")["targets"]


def build_repair_tools(manifest: dict, diagnosis: dict, validation_results: dict, allowed_targets: dict) -> RepairTools:
    business_rules_by_alias = {
        alias: load_json(Path(path), f"business rules ({alias})")
        for alias, path in manifest["business_rules_aliases"].items()
    }
    pipeline_configuration = None
    config_file = manifest.get("pipeline_configuration_file")
    if config_file and Path(config_file).exists():
        pipeline_configuration = load_json(Path(config_file), "pipeline configuration")

    lineage = load_json(Path("context/lineage.json"), "lineage")

    return RepairTools(
        diagnosis=diagnosis,
        validation_results=validation_results,
        business_rules_by_alias=business_rules_by_alias,
        lineage=lineage,
        pipeline_configuration=pipeline_configuration,
        allowed_repair_targets=allowed_targets,
        test_inventory=manifest["test_inventory"],
        etl_function_name=manifest["etl_function_name"],
        file_hash_paths=manifest["file_hash_aliases"],
    )


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _workspace_path(workspace_dir: Path, target_file: str) -> Path:
    """Map a target_file (relative or absolute) to its mirrored location under workspace_dir.

    Joining a Path with an absolute string DISCARDS the base (a pathlib
    footgun: Path("/tmp/x") / "/abs/y" == Path("/abs/y")) -- stripping the
    leading separator guarantees the result always lands under workspace_dir,
    never silently aliasing back to the real file.
    """
    return workspace_dir / target_file.lstrip("/")


def _create_isolated_workspace(target_file: str) -> Path:
    """A temp directory containing a copy of ONLY the target file, mirroring its repo-relative path."""
    workspace_dir = Path(tempfile.mkdtemp(prefix="repair_workspace_"))
    dest_path = _workspace_path(workspace_dir, target_file)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(Path(target_file), dest_path)
    return workspace_dir


def _validate_and_apply_patch(workspace_dir: Path, plan: RepairPlan) -> None:
    """Deterministic policy validation that needs real file content, plus the actual apply.

    Raises PatchApplyError on any policy violation -- callers treat this as
    repair_status=BLOCKED, not a crash.
    """
    target_path = _workspace_path(workspace_dir, plan.target_file)

    if plan.repair_type == RepairType.CODE_CHANGE:
        original_text = target_path.read_text(encoding="utf-8")
        patched_text = apply_unified_diff(original_text, plan.patch.content)
        try:
            compile(patched_text, str(target_path), "exec")
        except SyntaxError as exc:
            raise PatchApplyError(f"patched {plan.target_file} is not valid Python: {exc}") from exc
        if patched_text == original_text:
            raise PatchApplyError("patch produced no actual change")
        target_path.write_text(patched_text, encoding="utf-8")

    elif plan.repair_type == RepairType.CONFIGURATION_CHANGE:
        original_content = json.loads(target_path.read_text(encoding="utf-8"))
        patched_content = apply_structured_config_edit(original_content, plan.patch.content["operations"])
        if patched_content == original_content:
            raise PatchApplyError("patch produced no actual change")
        with target_path.open("w", encoding="utf-8") as f:
            json.dump(patched_content, f, indent=2)
            f.write("\n")

    else:
        raise PatchApplyError(f"unsupported repair_type for application: {plan.repair_type}")


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


def run_apply_repair(
    manifest: dict,
    model_client_factory: Callable[[], DiagnosisModelClient],
    *,
    repair_targets_file: str = DEFAULT_REPAIR_TARGETS_FILE,
    confidence_threshold: str = DEFAULT_CONFIDENCE_THRESHOLD,
) -> tuple[dict, dict]:
    """Run the full apply-repair flow. Returns (repair_plan_dict, repair_result_dict)."""
    diagnosis = load_json(Path(manifest["diagnosis_file"]), "diagnosis")
    validation_results = load_json(Path(manifest["validation_results_file"]), "validation results")
    incident_id = manifest["incident_id"]
    diagnosis_reference = diagnosis.get("incident_summary", incident_id)

    eligibility = evaluate_repair_eligibility(
        diagnosis,
        allowed_target_files=set(manifest["eligibility_target_hints"]),
        confidence_threshold=confidence_threshold,
    )

    if eligibility.decision == RepairEligibility.NO_REPAIR_NEEDED:
        plan = build_no_repair_needed_plan(incident_id=incident_id, diagnosis_reference=diagnosis_reference)
        return repair_plan_to_dict(plan), _outcome_result("; ".join(eligibility.reasons), repair_status="NO_REPAIR")

    if eligibility.decision in (RepairEligibility.HUMAN_REVIEW_REQUIRED, RepairEligibility.INVALID_DIAGNOSIS):
        plan = build_blocked_repair_plan(
            "; ".join(eligibility.reasons), incident_id=incident_id, diagnosis_reference=diagnosis_reference
        )
        return repair_plan_to_dict(plan), _outcome_result("; ".join(eligibility.reasons), repair_status="BLOCKED")

    # ELIGIBLE_FOR_REPAIR: proceed to the repair model.
    try:
        allowed_targets = load_repair_targets(Path(repair_targets_file))
        tools = build_repair_tools(manifest, diagnosis, validation_results, allowed_targets)
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
        plan = run_repair_planning(
            starting_context, tools, model_client, diagnosis=diagnosis, allowed_targets=allowed_targets
        )
    except (FileNotFoundError, ValueError, KeyError, RepairAgentError, RepairPlanValidationError, ModelClientError) as exc:
        raise ApplyRepairError(str(exc)) from exc

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


def write_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def print_repair_result(result: dict) -> None:
    print("Repair application")
    print(f"  repair_status:       {result['repair_status']}")
    print(f"  repair_type:         {result['repair_type']}")
    print(f"  target_file:         {result['target_file']}")
    print(f"  plan_policy_status:  {result['plan_policy_status']}")
    print(f"  application_details: {result['application_details']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministic eligibility gate + repair-agent planning + isolated-workspace apply."
    )
    parser.add_argument("--scenario-manifest-file", type=str, required=True)
    parser.add_argument("--repair-targets-file", type=str, default=DEFAULT_REPAIR_TARGETS_FILE)
    parser.add_argument("--confidence-threshold", type=str, default=DEFAULT_CONFIDENCE_THRESHOLD, choices=["LOW", "MEDIUM", "HIGH"])
    parser.add_argument("--output-dir", type=str, default=None, help="Defaults to the manifest file's own directory.")
    parser.add_argument("--model", type=str, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    manifest = load_scenario_manifest(Path(args.scenario_manifest_file))
    output_dir = Path(args.output_dir) if args.output_dir else Path(args.scenario_manifest_file).parent

    model_name = args.model or os.environ.get(REPAIR_MODEL_ENV_VAR)

    def model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=model_name) if model_name else OpenAIDiagnosisModelClient()

    try:
        plan_dict, result = run_apply_repair(
            manifest,
            model_client_factory,
            repair_targets_file=args.repair_targets_file,
            confidence_threshold=args.confidence_threshold,
        )
    except ApplyRepairError as exc:
        print(f"Repair application failed: {exc}")
        raise SystemExit(1)

    write_json_file(output_dir / "repair_plan.json", plan_dict)
    write_json_file(output_dir / "repair_result.json", result)
    print_repair_result(result)
    # BLOCKED / NO_REPAIR are legitimate, successful outcomes -- only the
    # ApplyRepairError branch above warrants a nonzero exit.


if __name__ == "__main__":
    main()
