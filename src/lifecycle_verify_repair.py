"""Deterministic post-repair verification and promotion for the loan_portfolio lifecycle
pipeline. Parallel to src/verify_repair.py (left completely unmodified) for the S3-backed
lifecycle model, which has exactly one repairable pipeline (no rerun-kind dispatch needed).

Reruns the ETL (using the PATCHED code from the isolated workspace) against real raw
Parquet data, validates via the existing, UNMODIFIED src.validate_loan_portfolio -- pointed
at the freshly-computed candidate result via a small storage wrapper rather than the real,
not-yet-promoted curated object -- compares before/after, and ONLY on full success promotes
the patched ETL file and freshly-computed curated output into the real repository/bucket. On
any failure, the isolated workspace is discarded and the real repository/curated data are
left completely untouched.

This module -- not the repair agent -- is the sole authority on whether a lifecycle repair
is VERIFIED.
"""

from __future__ import annotations

import hashlib
import importlib.util
import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from src.apply_repair import _workspace_path
from src.storage import S3Storage
from src.validate_loan_portfolio import validate_loan_portfolio

CURATED_KEY = "curated/loan_portfolio.parquet"
VALIDATION_RESULTS_KEY = "curated/loan_portfolio_validation_results.json"
PIPELINE_RUN_KEY = "curated/pipeline_run.json"
TEST_INVENTORY = ["tests/test_etl_spark_loan_portfolio.py"]
# Protected against an accidental/out-of-scope edit by the patch -- these are never the
# repair target, so any change to them between before/after is a policy violation.
PROTECTED_LOCAL_FILES = ["src/validate_loan_portfolio.py", "context/validations/loan_portfolio.json"]


class VerifyLifecycleRepairError(Exception):
    """Application-level failure: an internal error not otherwise captured as a verification outcome."""


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_patched_loan_portfolio_module(workspace_dir: Path, target_file: str):
    """Dynamically import the patched copy of the ETL module (mirrored under workspace_dir at
    target_file's repo-relative path) as an ISOLATED module object. Never touches
    sys.modules['src.etl_spark_loan_portfolio'] -- the real, installed module is completely
    unaffected. This is how a CODE_CHANGE's patched behavior gets genuinely exercised before
    promotion. Deriving the path from target_file keeps promotion's source/destination and
    this rerun's source in sync via one field, rather than two constants that could drift."""
    patched_path = _workspace_path(workspace_dir, target_file)
    spec = importlib.util.spec_from_file_location("patched_loan_portfolio_for_verification", patched_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _CandidateCuratedStorage:
    """Redirects ONLY the loan_portfolio curated read to an in-memory candidate DataFrame, so
    validate_loan_portfolio() (which reads a fixed S3 key internally) can be checked against a
    not-yet-promoted result without ever touching or overwriting the real curated object.
    Every other call (raw reads, context reads) delegates straight through to real storage."""

    def __init__(self, real_storage: S3Storage, candidate_curated: pd.DataFrame) -> None:
        self._real = real_storage
        self._candidate_curated = candidate_curated
        self.bucket = real_storage.bucket

    def read_parquet(self, path: str) -> pd.DataFrame:
        if path == CURATED_KEY:
            return self._candidate_curated
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
    import pytest

    exit_code = pytest.main(["-q", *test_files])
    return "PASS" if int(exit_code) == 0 else "FAIL"


def _checks_by_id(validation_results: dict) -> dict:
    return {c["id"]: c for c in validation_results.get("checks", [])}


def _protected_file_hashes() -> dict:
    return {p: _sha256_of_file(Path(p)) for p in PROTECTED_LOCAL_FILES if Path(p).exists()}


def _promote(
    storage: S3Storage, repair_result: dict, workspace_dir: Path, metrics_after_df: pd.DataFrame, validation_after: dict
) -> list:
    """Copy the verified-good workspace output over the real repository/bucket. Only called
    after full verification passes."""
    promoted: list = []

    target_file = repair_result["target_file"]
    real_target_path = Path(target_file)
    shutil.copy2(_workspace_path(workspace_dir, target_file), real_target_path)
    promoted.append(str(real_target_path))

    storage.write_parquet(CURATED_KEY, metrics_after_df)
    promoted.append(f"s3://{storage.bucket}/{CURATED_KEY}")

    storage.write_json(VALIDATION_RESULTS_KEY, validation_after)
    promoted.append(f"s3://{storage.bucket}/{VALIDATION_RESULTS_KEY}")

    pipeline_run = storage.read_json(PIPELINE_RUN_KEY) if storage.exists(PIPELINE_RUN_KEY) else {"pipelines": {}}
    pipeline_run.setdefault("pipelines", {})["loan_portfolio"] = {
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

    return promoted


def run_verify_lifecycle_repair(
    spark: SparkSession,
    storage: S3Storage,
    business_rules: dict,
    validation_rules: dict,
    validation_before: dict,
    repair_result: dict,
    *,
    s3a_path_override=None,
) -> dict:
    """Run the full verification flow for a loan_portfolio repair. Returns the
    repair_verification dict.

    s3a_path_override lets tests redirect the patched module's raw-data reads to a
    TEST_PREFIX-scoped location, exactly like the `patched` fixture already used by
    tests/test_etl_spark_*.py -- production callers leave it at the default (None), which
    reads the real bucket.
    """
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
    protected_hashes_before = _protected_file_hashes()

    try:
        patched_module = _load_patched_loan_portfolio_module(workspace_dir, repair_result["target_file"])
        if s3a_path_override is not None:
            patched_module.s3a_path = s3a_path_override
        summary_df = patched_module.compute_loan_portfolio(spark, business_rules)
        metrics_after_df = summary_df.toPandas()
        candidate_storage = _CandidateCuratedStorage(storage, metrics_after_df)
        validation_after = validate_loan_portfolio(candidate_storage, business_rules, validation_rules)
        etl_status_after = "SUCCESS"
    except Exception as exc:  # noqa: BLE001 -- rerun failure is a verification outcome, not a crash
        shutil.rmtree(workspace_dir, ignore_errors=True)
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

    targeted_status = _run_pytest(TEST_INVENTORY)
    full_suite_status = targeted_status  # single test file governs this pipeline

    protected_hashes_after = _protected_file_hashes()
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

    metrics_after = metrics_after_df.iloc[0].to_dict()

    if all_checks_pass:
        changed_files = _promote(storage, repair_result, workspace_dir, metrics_after_df, validation_after)
        shutil.rmtree(workspace_dir, ignore_errors=True)
        verification_status = "VERIFIED"
        rollback_performed = False
        summary = "All deterministic checks passed; repair promoted to the real repository."
    else:
        changed_files = []
        shutil.rmtree(workspace_dir, ignore_errors=True)
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
    }


def print_verification(result: dict) -> None:
    print("Repair verification (loan_portfolio)")
    print(f"  verification_status: {result['verification_status']}")
    print(f"  validation_before:    {result['validation_before']}")
    print(f"  validation_after:     {result['validation_after']}")
    print(f"  tests:                targeted={result['tests']['targeted']} full={result['tests']['full_relevant_suite']}")
    print(f"  rollback_performed:   {result['rollback_performed']}")
    print(f"  summary:              {result['summary']}")
