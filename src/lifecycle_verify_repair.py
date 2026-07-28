"""Deterministic post-repair verification and promotion for any of the 5 lifecycle
pipelines. Parallel to src/verify_repair.py (left completely unmodified) for the S3-backed
lifecycle model. Generalized (via src/lifecycle_pipeline_registry.py) rather than hardcoded
to loan_portfolio -- including atomic-ish promotion for pipelines with more than one
curated output (underwriting_performance).

Reruns the ETL (using the PATCHED code from the isolated workspace) against real raw
Parquet data, validates via the existing, UNMODIFIED validate_*.py function -- pointed at
the freshly-computed candidate result(s) via a small storage wrapper rather than the real,
not-yet-promoted curated object(s) -- compares before/after, and ONLY on full success
promotes the patched ETL file and freshly-computed curated output(s) into the real
repository/bucket. On any failure (including a failure DURING promotion itself), every key
already written this run is rolled back and the real repository/curated data are left
exactly as they were.

This module -- not the repair agent -- is the sole authority on whether a lifecycle repair
is VERIFIED.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.pr_artifact import build_pr_artifact
from src.sandbox.backend import GitWorktreeSandbox, SandboxBackend, TempDirSandbox
from src.storage import S3Storage

PIPELINE_RUN_KEY = "curated/pipeline_run.json"


class PromotionError(Exception):
    """Raised internally when a promotion write fails partway through, after rollback.
    Caught by run_verify_lifecycle_repair and converted into a NOT_VERIFIED outcome -- a
    promotion failure is a verification outcome, not a crash, same discipline as an ETL
    rerun failure."""


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_patched_etl_module(workspace_dir: Path, target_file: str, sandbox_backend: SandboxBackend):
    """Dynamically import the patched copy of the ETL module (mirrored under workspace_dir at
    target_file's repo-relative path, per sandbox_backend's own path mapping) as an ISOLATED
    module object. Never touches the real, installed module. This is how a CODE_CHANGE's
    patched behavior gets genuinely exercised before promotion."""
    patched_path = sandbox_backend.workspace_path(workspace_dir, target_file)
    spec = importlib.util.spec_from_file_location("patched_lifecycle_etl_for_verification", patched_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _CandidateCuratedStorage:
    """Redirects reads of this pipeline's curated key(s) to in-memory candidate DataFrames,
    so run_validate (which reads fixed S3 keys internally) can be checked against a
    not-yet-promoted result without ever touching or overwriting the real curated object(s).
    Every other call (raw reads, context reads) delegates straight through to real storage."""

    def __init__(self, real_storage: S3Storage, candidate_by_key: dict) -> None:
        self._real = real_storage
        self._candidate_by_key = candidate_by_key
        self.bucket = real_storage.bucket

    def read_parquet(self, path: str) -> pd.DataFrame:
        if path in self._candidate_by_key:
            return self._candidate_by_key[path]
        return self._real.read_parquet(path)

    def read_json(self, path: str):
        return self._real.read_json(path)

    def write_json(self, path: str, value) -> None:
        self._real.write_json(path, value)

    def write_parquet(self, path: str, dataframe: pd.DataFrame) -> None:
        self._real.write_parquet(path, dataframe)

    def exists(self, path: str) -> bool:
        return self._real.exists(path)


def _run_pytest(test_files: list) -> str:
    """Runs pytest in a SEPARATE PROCESS -- never in-process via pytest.main(). A pipeline's
    own test file can monkeypatch shared module state for its own isolated S3 reads (e.g.
    tests/test_etl_spark_loan_portfolio.py patches etl_spark_loan_portfolio.s3a_path). That's
    fine for a one-shot CLI script (the process exits right after), but this function is also
    called from a long-running server process (src/api.py) across many verify cycles over its
    lifetime -- an in-process pytest.main() run risks that monkeypatch leaking into THIS
    process's shared module cache for every verify AFTER it, since _run_pytest_against_patched_
    code's own reload-based cleanup only covers a CODE_CHANGE target (an importable .py file);
    a CONFIGURATION_CHANGE target (e.g. a JSON pointer file) has no corresponding module to
    reload, so nothing undoes the leak. Observed live: a later verify's real ETL rerun failed
    reading a test-prefixed S3 path that only ever existed in a test fixture. A subprocess can
    never leak state back into this process, regardless of target type."""
    result = subprocess.run([sys.executable, "-m", "pytest", "-q", *test_files], capture_output=True, text=True, timeout=180)
    if result.returncode != 0:
        print(f"[_run_pytest] {test_files} FAILED (exit {result.returncode}):\n{result.stdout}\n{result.stderr}")
    return "PASS" if result.returncode == 0 else "FAIL"


def _run_pytest_against_patched_code(
    test_files: list, workspace_dir: Path, target_file: str, sandbox_backend: SandboxBackend
) -> str:
    """Run test_files with the REAL target_file's on-disk content temporarily replaced by
    the patched (isolated-workspace) version, then always restore the original -- so a test
    file that pins exact expected values (e.g. "loss_rate must equal 0.4") actually reflects
    the candidate fix, not the still-buggy pre-repair code. Without this, verification could
    reject a genuinely correct patch just because the real repo file hadn't been promoted
    yet (promotion only happens AFTER verification passes).

    Swapping the bytes on disk alone is not enough: if target_file's module was already
    imported earlier in this process (e.g. the diagnosis stage's get_relevant_etl_source
    imports it via importlib.import_module), Python's sys.modules cache means a plain `from
    <module> import x` in the test file returns the already-compiled, still-buggy module
    object -- disk content is only read on a module's FIRST import. So this also
    importlib.reload()s the cached module (if present) after swapping in, and again after
    restoring, so both the on-disk content and the in-memory module reflect the same code
    at every point another part of this process might import it.

    The swap (and any reload) is undone in a finally block regardless of outcome, so by the
    time this function returns -- pass or fail -- the real file and the in-memory module are
    both back to exactly what they were before this call, matching the "real repository left
    untouched until VERIFIED" invariant everywhere else in this module."""
    real_target_path = Path(target_file)
    original_bytes = real_target_path.read_bytes() if real_target_path.exists() else None
    if original_bytes is None:
        return _run_pytest(test_files)

    module_name = target_file.replace("/", ".").removesuffix(".py")
    cached_module = sys.modules.get(module_name)
    patched_bytes = sandbox_backend.workspace_path(workspace_dir, target_file).read_bytes()
    try:
        real_target_path.write_bytes(patched_bytes)
        if cached_module is not None:
            importlib.reload(cached_module)
        return _run_pytest(test_files)
    finally:
        real_target_path.write_bytes(original_bytes)
        if cached_module is not None:
            importlib.reload(cached_module)


def _resolve_rerun_inputs(spec, target_file: str, workspace_dir: Path, sandbox_backend: SandboxBackend, business_rules: dict):
    """What to rerun the candidate ETL/validation against: the ETL module and the
    business_rules dict.

    For the ordinary case (target_file is this pipeline's ETL source -- a CODE_CHANGE), this
    is exactly today's behavior: dynamically import the PATCHED module from the workspace,
    unchanged business_rules.

    For a CONFIGURATION_CHANGE target (target_file is spec.pipeline_configuration_file, e.g.
    loan_portfolio's context/pipeline_rules/loan_portfolio.json): there is no patched Python
    to import -- the ETL source itself is untouched. Instead, read the CANDIDATE'S patched
    pointer from the workspace (never from real S3, which still has the stale, not-yet-
    promoted pointer) and resolve business_rules from whichever already-approved file it now
    names, read from the real repository (that file is static and already checked in -- the
    repair only ever changes which one is pointed at, never a file's content)."""
    if target_file == getattr(spec, "pipeline_configuration_file", None):
        patched_pointer = json.loads(sandbox_backend.workspace_path(workspace_dir, target_file).read_text())
        business_rules_file = patched_pointer.get("business_rules_file", "context/business_rules.json")
        effective_business_rules = json.loads(Path(business_rules_file).read_text())
        etl_module = importlib.import_module(spec.etl_source_file.replace("/", ".").removesuffix(".py"))
        return etl_module, effective_business_rules
    return _load_patched_etl_module(workspace_dir, target_file, sandbox_backend), business_rules


def _context_provenance_for(pipeline_name: str, diagnosis: dict, storage: S3Storage) -> dict | None:
    """For each metric the diagnosis named as affected, the ContextRetriever facts (provenance,
    review_status, confidence, conflicts) that backed it -- included in the create_pr PR
    artifact for audit. Never raises: a pipeline without generated/human context populated
    yet just contributes legacy_file/no-conflict entries, which is real information too (it
    documents that this repair wasn't backed by a human-approved definition at all)."""
    affected_metrics = diagnosis.get("affected_metrics") or []
    if not affected_metrics:
        return None
    retriever = ContextRetriever(store=FileContextStore())
    provenance = {}
    for metric_name in affected_metrics:
        try:
            fact = retriever.get_metric(pipeline_name, metric_name, storage)
        except Exception:  # noqa: BLE001 -- provenance is audit information, never blocks verification
            continue
        provenance[metric_name] = {
            "provenance": fact.provenance,
            "review_status": fact.review_status.value if fact.review_status else None,
            "confidence": fact.confidence,
            "conflicts": [c.model_dump() for c in fact.conflicts],
        }
    return provenance or None


def _checks_by_id(validation_results: dict) -> dict:
    return {c["id"]: c for c in validation_results.get("checks", [])}


def _protected_file_hashes(pipeline_name: str, validation_rules_key: str) -> dict:
    paths = [f"src/validate_{pipeline_name}.py", validation_rules_key]
    return {p: _sha256_of_file(Path(p)) for p in paths if Path(p).exists()}


def _promote_atomically(
    storage: S3Storage,
    run_id: str,
    pipeline_name: str,
    repair_result: dict,
    workspace_dir: Path,
    metrics_after_by_key: dict,
    validation_after: dict,
    sandbox_backend: SandboxBackend,
) -> list:
    """Promote every changed key -- this pipeline's curated output(s), its validation
    results, pipeline_run.json, and its ETL source file -- backing each up first and
    rolling back everything already written this run if any later step fails, so a
    promotion failure never leaves a mix of old and new data across a multi-output
    pipeline (or between S3 and the local ETL source file)."""
    undo_stack: list = []
    backup_keys_made: list = []
    promoted: list = []

    def _backup_and_track(key: str) -> None:
        if storage.exists(key):
            backup_key = f"_backup/{run_id}/{key}"
            storage.copy_or_promote(key, backup_key)
            backup_keys_made.append(backup_key)
            undo_stack.append(lambda k=key, b=backup_key: storage.copy_or_promote(b, k))
        else:
            undo_stack.append(lambda k=key: storage.delete(k))

    try:
        # 1. This pipeline's curated output(s) first.
        for key, df in metrics_after_by_key.items():
            _backup_and_track(key)
            storage.write_parquet(key, df)
            promoted.append(f"s3://{storage.bucket}/{key}")

        # 2. Validation results next.
        validation_results_key = f"curated/{pipeline_name}_validation_results.json"
        _backup_and_track(validation_results_key)
        storage.write_json(validation_results_key, validation_after)
        promoted.append(f"s3://{storage.bucket}/{validation_results_key}")

        # 3. pipeline_run.json LAST of the S3 writes -- the health signal only flips to
        # PASS after the actual curated data is confirmed written.
        _backup_and_track(PIPELINE_RUN_KEY)
        pipeline_run = storage.read_json(PIPELINE_RUN_KEY) if storage.exists(PIPELINE_RUN_KEY) else {"pipelines": {}}
        pipeline_run.setdefault("pipelines", {})[pipeline_name] = {
            "etl_status": "SUCCESS",
            "etl_error": None,
            "validation_status": validation_after["overall_status"],
            "validation_error": None,
        }
        pipeline_run["overall_status"] = (
            "SUCCESS"
            if all(
                r.get("etl_status") == "SUCCESS" and r.get("validation_status") == "PASS"
                for r in pipeline_run["pipelines"].values()
            )
            else "FAILURE"
        )
        storage.write_json(PIPELINE_RUN_KEY, pipeline_run)
        promoted.append(f"s3://{storage.bucket}/{PIPELINE_RUN_KEY}")

        # 4. The ETL source file itself (local filesystem) -- only after every S3 write
        # succeeded, so a local-copy failure can still be rolled back on the S3 side too.
        target_file = repair_result["target_file"]
        real_target_path = Path(target_file)
        original_bytes = real_target_path.read_bytes() if real_target_path.exists() else None

        def _undo_local_file(path=real_target_path, original=original_bytes):
            if original is not None:
                path.write_bytes(original)

        undo_stack.append(_undo_local_file)
        shutil.copy2(sandbox_backend.workspace_path(workspace_dir, target_file), real_target_path)
        promoted.append(str(real_target_path))

    except Exception as exc:  # noqa: BLE001 -- roll back every step already taken this run
        for undo in reversed(undo_stack):
            try:
                undo()
            except Exception:  # noqa: BLE001 -- best-effort rollback; don't mask the original error
                pass
        raise PromotionError(f"promotion failed partway through and was rolled back: {exc}") from exc
    finally:
        for backup_key in backup_keys_made:
            try:
                storage.delete(backup_key)
            except Exception:  # noqa: BLE001 -- cleanup is best-effort, non-fatal
                pass

    return promoted


def _commit_patch_and_keep_branch(sandbox_backend: SandboxBackend, workspace_dir: Path, target_file: str, commit_message: str) -> str | None:
    """For create_pr mode with a GitWorktreeSandbox: commit the already-applied patch inside
    the SAME worktree run_apply_lifecycle_repair used (not a second, redundant one) and keep
    its branch for the PR artifact. Returns None for any other backend (nothing to commit --
    src.pr_artifact.build_pr_artifact falls back to creating its own throwaway branch in that
    case, exactly as it did before this wiring existed) or if git itself fails."""
    if not isinstance(sandbox_backend, GitWorktreeSandbox):
        return None
    try:
        subprocess.run(["git", "add", target_file], cwd=workspace_dir, check=True, capture_output=True, timeout=30)
        subprocess.run(["git", "commit", "-m", commit_message], cwd=workspace_dir, check=True, capture_output=True, timeout=30)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return sandbox_backend.keep_branch(workspace_dir)


def run_verify_lifecycle_repair(
    pipeline_name: str,
    spark: SparkSession,
    storage: S3Storage,
    business_rules: dict,
    validation_rules: dict,
    validation_before: dict,
    repair_result: dict,
    *,
    run_id: str | None = None,
    s3a_path_override=None,
    mode: str = "auto_promote",
    diagnosis: dict | None = None,
    repair_plan: dict | None = None,
    sandbox_backend: SandboxBackend = TempDirSandbox(),
) -> dict:
    """Run the full verification flow for one lifecycle pipeline's repair. Returns the
    repair_verification dict.

    run_id namespaces this run's temporary backup keys during promotion -- pass the same
    run_id used for this run's audit-artifact persistence (src/lifecycle_run_self_healing.py)
    to keep them correlated; a fresh one is generated if not provided.

    s3a_path_override lets tests redirect the patched module's raw-data reads to a
    TEST_PREFIX-scoped location, exactly like the `patched` fixture already used by
    tests/test_etl_spark_*.py -- production callers leave it at the default (None), which
    reads the real bucket.

    mode="auto_promote" (the default, used by every existing caller) preserves this
    function's original behavior exactly: on a full pass, promote directly into the real
    repository. mode="create_pr" instead builds a local PR artifact (src/pr_artifact.py) --
    real diff, real throwaway branch/commit, diagnosis summary, before/after metrics -- and
    leaves the real repository completely untouched, even on a full pass. diagnosis/
    repair_plan are only used for the create_pr artifact's summary fields; omit them for
    auto_promote.

    sandbox_backend MUST be the same instance/kind passed to the run_apply_lifecycle_repair
    call that produced repair_result["workspace_dir"] -- defaults to TempDirSandbox, which is
    byte-identical to this function's original (pre-sandbox_backend) behavior. Passing a
    GitWorktreeSandbox (used automatically for mode="create_pr" -- see
    src.lifecycle_run_self_healing) means the Spark rerun below happens inside the real git
    worktree that becomes the PR branch, and this function commits the patch and keeps that
    branch on a full pass instead of leaving it to src.pr_artifact to create a second,
    disconnected one.
    """
    spec = PIPELINE_REGISTRY[pipeline_name]
    run_id = run_id or uuid.uuid4().hex[:12]

    if repair_result["repair_status"] != "APPLIED":
        return {
            "verification_status": "BLOCKED",
            "diagnosis_status": None,
            "repair_status": repair_result["repair_status"],
            "tests": {"targeted": "NOT_RUN", "full_relevant_suite": "NOT_RUN"},
            "etl_status_after": "NOT_RUN",
            "validation_before": validation_before.get("overall_status"),
            "validation_after": "NOT_RUN",
            "failed_checks_before": [c["id"] for c in validation_before.get("checks", []) if c["status"] == "FAIL"],
            "failed_checks_after": [],
            "metrics_after": {},
            "changed_files": [],
            "unchanged_protected_files_verified": True,
            "rollback_performed": False,
            "summary": f"Nothing to verify -- repair_status was {repair_result['repair_status']!r}, not APPLIED.",
        }

    workspace_dir = Path(repair_result["workspace_dir"])
    protected_hashes_before = _protected_file_hashes(pipeline_name, spec.validation_rules_key)

    try:
        patched_module, effective_business_rules = _resolve_rerun_inputs(
            spec, repair_result["target_file"], workspace_dir, sandbox_backend, business_rules
        )
        if s3a_path_override is not None:
            patched_module.s3a_path = s3a_path_override
        metrics_after_by_key = spec.run_etl(patched_module, spark, effective_business_rules, DEFAULT_AS_OF_DATE)
        candidate_storage = _CandidateCuratedStorage(storage, metrics_after_by_key)
        validation_after = spec.run_validate(candidate_storage, effective_business_rules, validation_rules, DEFAULT_AS_OF_DATE)
        etl_status_after = "SUCCESS"
    except Exception as exc:  # noqa: BLE001 -- rerun failure is a verification outcome, not a crash
        sandbox_backend.cleanup(workspace_dir)
        return {
            "verification_status": "NOT_VERIFIED",
            "diagnosis_status": "DIAGNOSED",
            "repair_status": repair_result["repair_status"],
            "tests": {"targeted": "NOT_RUN", "full_relevant_suite": "NOT_RUN"},
            "etl_status_after": "FAILED",
            "validation_before": validation_before.get("overall_status"),
            "validation_after": "NOT_RUN",
            "failed_checks_before": [c["id"] for c in validation_before.get("checks", []) if c["status"] == "FAIL"],
            "failed_checks_after": [],
            "metrics_after": {},
            "changed_files": [],
            "unchanged_protected_files_verified": True,
            "rollback_performed": True,
            "summary": f"ETL rerun against the patched workspace failed: {exc}",
        }

    targeted_status = _run_pytest_against_patched_code([spec.test_file], workspace_dir, repair_result["target_file"], sandbox_backend)
    full_suite_status = targeted_status  # a single test file governs each lifecycle pipeline

    protected_hashes_after = _protected_file_hashes(pipeline_name, spec.validation_rules_key)
    protected_files_unchanged = protected_hashes_before == protected_hashes_after

    checks_before = _checks_by_id(validation_before)
    checks_after = _checks_by_id(validation_after)
    failed_before = [check_id for check_id, c in checks_before.items() if c["status"] == "FAIL"]
    failed_after = [check_id for check_id, c in checks_after.items() if c["status"] == "FAIL"]

    previously_failed_now_pass = all(checks_after.get(check_id, {}).get("status") == "PASS" for check_id in failed_before)
    previously_passed_still_pass = all(
        checks_after.get(check_id, {}).get("status") == "PASS"
        for check_id, c in checks_before.items()
        if c["status"] == "PASS"
    )

    all_checks_pass = (
        targeted_status == "PASS"
        and full_suite_status == "PASS"
        and etl_status_after == "SUCCESS"
        and validation_after["overall_status"] == "PASS"
        and previously_failed_now_pass
        and previously_passed_still_pass
        and protected_files_unchanged
    )

    metrics_after = {key: df.to_dict(orient="records") for key, df in metrics_after_by_key.items()}
    pr_artifact = None

    if all_checks_pass and mode == "create_pr":
        target_path = sandbox_backend.workspace_path(workspace_dir, repair_result["target_file"])
        patched_content = target_path.read_text()
        commit_message = f"Repair {pipeline_name}: {(repair_plan or {}).get('change_summary', 'automated candidate patch')}"
        branch = _commit_patch_and_keep_branch(sandbox_backend, workspace_dir, repair_result["target_file"], commit_message)
        pr_artifact = build_pr_artifact(
            pipeline_name=pipeline_name,
            run_id=run_id,
            target_file=repair_result["target_file"],
            original_content=Path(repair_result["target_file"]).read_text(),
            patched_content=patched_content,
            diagnosis=diagnosis or {},
            repair_plan=repair_plan or {},
            validation_before=validation_before,
            validation_after=validation_after,
            metrics_before={},
            metrics_after=metrics_after,
            tests_status={"targeted": targeted_status, "full_relevant_suite": full_suite_status},
            branch=branch,
            context_provenance=_context_provenance_for(pipeline_name, diagnosis or {}, storage),
        )
        if branch is None:
            sandbox_backend.cleanup(workspace_dir)  # not kept as a branch -- nothing left to preserve
        changed_files = []
        verification_status = "VERIFIED_PENDING_PR"
        rollback_performed = False
        summary = "All deterministic checks passed; a local PR artifact was created instead of promoting directly."
    elif all_checks_pass:
        try:
            changed_files = _promote_atomically(
                storage, run_id, pipeline_name, repair_result, workspace_dir, metrics_after_by_key, validation_after, sandbox_backend
            )
            sandbox_backend.cleanup(workspace_dir)
            verification_status = "VERIFIED"
            rollback_performed = False
            summary = "All deterministic checks passed; repair promoted to the real repository."
        except PromotionError as exc:
            sandbox_backend.cleanup(workspace_dir)
            changed_files = []
            verification_status = "NOT_VERIFIED"
            rollback_performed = True
            summary = str(exc)
    else:
        changed_files = []
        sandbox_backend.cleanup(workspace_dir)
        verification_status = "NOT_VERIFIED"
        rollback_performed = True
        summary = "One or more deterministic checks failed; isolated workspace discarded, repository left untouched."

    return {
        "verification_status": verification_status,
        "diagnosis_status": "DIAGNOSED",
        "repair_status": repair_result["repair_status"],
        "tests": {"targeted": targeted_status, "full_relevant_suite": full_suite_status},
        "etl_status_after": etl_status_after,
        "validation_before": validation_before.get("overall_status"),
        "validation_after": validation_after.get("overall_status"),
        "failed_checks_before": failed_before,
        "failed_checks_after": failed_after,
        "metrics_after": metrics_after,
        "changed_files": changed_files,
        "unchanged_protected_files_verified": protected_files_unchanged,
        "rollback_performed": rollback_performed,
        "summary": summary,
        "pr_artifact": pr_artifact,
    }


def print_verification(result: dict) -> None:
    print("Repair verification")
    print(f"  verification_status: {result['verification_status']}")
    print(f"  validation_before:    {result['validation_before']}")
    print(f"  validation_after:     {result['validation_after']}")
    print(f"  tests:                targeted={result['tests']['targeted']} full={result['tests']['full_relevant_suite']}")
    print(f"  rollback_performed:   {result['rollback_performed']}")
    print(f"  summary:              {result['summary']}")
