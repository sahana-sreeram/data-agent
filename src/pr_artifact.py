"""Builds a local PR artifact for `create_pr` mode: everything needed to open a real pull
request by hand (or, in future work, via `gh pr create`), without ever writing to GitHub.

Independently creates its own throwaway git branch/commit (via src.sandbox.backend's
GitWorktreeSandbox) purely to give the artifact a real, inspectable commit -- this is
decoupled from whatever sandbox lifecycle_apply_repair.py used to generate the candidate
patch, so wiring this in never requires touching that module. If branch creation fails for
any reason, the artifact still degrades gracefully (branch=None) rather than blocking on it --
the diff/summary/metrics are the substance; the branch is a convenience.
"""

from __future__ import annotations

import difflib
import subprocess
from pathlib import Path

from src.sandbox.backend import GitWorktreeSandbox, SandboxError

# Mirrors src.legacy.repair_models.evaluate_repair_eligibility's auto-repair-eligible set --
# duplicated as a constant (not imported) because that module's set is about what's ELIGIBLE
# for repair at all, a stricter question than "how risky is an already-applied one to review."
LOW_RISK_ROOT_CAUSE_CATEGORIES = {"ETL_LOGIC", "BUSINESS_RULE_MISMATCH", "DUPLICATION"}


def _unified_diff(target_file: str, original_content: str, patched_content: str) -> str:
    diff_lines = difflib.unified_diff(
        original_content.splitlines(keepends=True),
        patched_content.splitlines(keepends=True),
        fromfile=f"a/{target_file}",
        tofile=f"b/{target_file}",
    )
    return "".join(diff_lines)


def _create_branch_with_commit(repo_root: Path, target_file: str, patched_content: str, commit_message: str) -> str | None:
    sandbox = GitWorktreeSandbox(repo_root=repo_root)
    try:
        worktree_dir = sandbox.create_workspace(target_file)
    except SandboxError:
        return None
    try:
        (worktree_dir / target_file.lstrip("/")).write_text(patched_content)
        subprocess.run(["git", "add", target_file], cwd=worktree_dir, check=True, capture_output=True, timeout=30)
        subprocess.run(["git", "commit", "-m", commit_message], cwd=worktree_dir, check=True, capture_output=True, timeout=30)
        return sandbox.keep_branch(worktree_dir)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        sandbox.cleanup(worktree_dir)
        return None


def build_pr_artifact(
    pipeline_name: str,
    run_id: str,
    target_file: str,
    original_content: str,
    patched_content: str,
    diagnosis: dict,
    repair_plan: dict,
    validation_before: dict,
    validation_after: dict,
    metrics_before: dict,
    metrics_after: dict,
    tests_status: dict,
    repo_root: Path | str = ".",
    create_branch: bool = True,
) -> dict:
    """Pure with respect to the real repo except for the optional throwaway branch/commit
    (create_branch=True by default; set False in tests or when a real git repo isn't
    available/desired)."""
    root_cause_category = diagnosis.get("root_cause_category")
    failed_before = [c["id"] for c in validation_before.get("checks", []) if c.get("status") == "FAIL"]
    failed_after = [c["id"] for c in validation_after.get("checks", []) if c.get("status") == "FAIL"]

    branch = None
    if create_branch:
        commit_message = f"Repair {pipeline_name}: {repair_plan.get('change_summary', 'automated candidate patch')}"
        branch = _create_branch_with_commit(Path(repo_root), target_file, patched_content, commit_message)

    return {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "branch": branch,
        "target_file": target_file,
        "diff": _unified_diff(target_file, original_content, patched_content),
        "diagnosis_summary": diagnosis.get("root_cause"),
        "root_cause_category": root_cause_category,
        "failed_checks_before": failed_before,
        "failed_checks_after": failed_after,
        "tests_status": tests_status,
        "metrics_before": metrics_before,
        "metrics_after": metrics_after,
        "risk_classification": "LOW" if root_cause_category in LOW_RISK_ROOT_CAUSE_CATEGORIES else "HIGH",
        "human_review_required": root_cause_category not in LOW_RISK_ROOT_CAUSE_CATEGORIES,
    }
