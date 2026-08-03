"""Tests for deterministic post-repair verification and promotion, generalized across
lifecycle pipelines. Against a REAL local Spark session and S3-compatible endpoint (MinIO),
using a dedicated test prefix so these tests never touch real migrated raw/curated data or
the real repository's ETL source files -- promotion in these tests always targets a
tmp_path file, never a real one. Skips cleanly if Spark/S3 aren't reachable.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pandas as pd
import pytest

import src.lifecycle_verify_repair as verify_module
from src.legacy.apply_repair import _workspace_path
from src.etl_spark_delinquency_default import compute_delinquency_default
from src.etl_spark_loan_portfolio import compute_loan_portfolio
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.lifecycle_verify_repair import PIPELINE_RUN_KEY, run_verify_lifecycle_repair
from src.sandbox.backend import GitWorktreeSandbox, TempDirSandbox
from src.validate_delinquency_default import validate_delinquency_default
from src.validate_loan_portfolio import validate_loan_portfolio
from tests.conftest import PrefixedStorage

# Captured at module load, BEFORE the autouse _skip_nested_pytest fixture below ever runs
# and monkeypatches verify_module._run_pytest_against_patched_code -- the two tests that
# specifically exercise the real swap/restore behavior call this real reference directly,
# not a fresh (and by-then-monkeypatched) import.
_REAL_RUN_PYTEST_AGAINST_PATCHED_CODE = verify_module._run_pytest_against_patched_code

TEST_PREFIX = "_test_lifecycle_verify_repair/"
AS_OF_DATE = "2026-07-20"
LOAN_PORTFOLIO_SOURCE = Path(PIPELINE_REGISTRY["loan_portfolio"].etl_source_file)
DELINQUENCY_DEFAULT_SOURCE = Path(PIPELINE_REGISTRY["delinquency_default"].etl_source_file)
UNDERWRITING_PERFORMANCE_SOURCE = Path(PIPELINE_REGISTRY["underwriting_performance"].etl_source_file)


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


def _test_s3a_path(bucket: str):
    return lambda *parts: f"s3a://{bucket}/{TEST_PREFIX}" + "/".join(parts)


def _write_workspace(tmp_path: Path, target_file: str, source_text: str) -> Path:
    workspace_dir = Path(str(tmp_path)) / "workspace"
    dest = _workspace_path(workspace_dir, target_file)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(source_text, encoding="utf-8")
    return workspace_dir


@pytest.fixture(autouse=True)
def _skip_nested_pytest(monkeypatch):
    # A generalized pipeline's test_file needs the shared session-scoped spark_session
    # fixture -- invoking it via a NESTED pytest.main() call would tear down that shared
    # SparkSession at the nested run's teardown, breaking every other Spark-dependent test
    # in this same pytest session. The pytest-invocation plumbing itself (_run_pytest) is a
    # two-line pytest.main() wrapper, identical in shape to the already-proven
    # src.verify_repair._run_pytest -- stub out _run_pytest_against_patched_code (the
    # function the main verify flow actually calls) rather than _run_pytest directly, so
    # this also skips its real-file swap/restore -- these tests use their own tmp_path
    # target_file, not the real repo file, so there'd be nothing meaningful to swap anyway.
    import src.lifecycle_verify_repair as verify_module

    monkeypatch.setattr(verify_module, "_run_pytest_against_patched_code", lambda test_files, workspace_dir, target_file, sandbox_backend: "PASS")


# --- loan_portfolio: reproves the original inner-join bug through the generalized path ----

LOANS_ALL = pd.DataFrame(
    [
        {"loan_id": "L1", "application_id": "APP1", "customer_id": "C1", "principal_amount": 1000.0, "interest_rate": 0.05, "term_months": 12, "originated_at": "2024-01-01", "loan_status": "CLOSED", "scheduled_payment_amount": 83.33},
        {"loan_id": "L2", "application_id": "APP2", "customer_id": "C2", "principal_amount": 2000.0, "interest_rate": 0.10, "term_months": 24, "originated_at": "2025-07-20", "loan_status": "ACTIVE", "scheduled_payment_amount": 83.33},
        {"loan_id": "L3", "application_id": "APP3", "customer_id": "C3", "principal_amount": 1500.0, "interest_rate": 0.08, "term_months": 24, "originated_at": "2025-07-20", "loan_status": "ACTIVE", "scheduled_payment_amount": 62.50},
    ]
)
LOANS_WITHOUT_L3 = LOANS_ALL[LOANS_ALL["loan_id"] != "L3"].reset_index(drop=True)
LOAN_PORTFOLIO_PAYMENT_EVENTS = pd.DataFrame(
    [
        {"event_id": "E1", "schedule_id": "S1", "loan_id": "L1", "event_type": "PAYMENT", "payment_date": "2024-02-01", "amount": 1000.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E2", "schedule_id": "S2", "loan_id": "L2", "event_type": "PAYMENT", "payment_date": "2025-08-20", "amount": 500.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E3", "schedule_id": "S2", "loan_id": "L2", "event_type": "REVERSAL", "payment_date": "2025-08-25", "amount": -500.0, "payment_status": "REVERSED", "payment_method": "ACH"},
        {"event_id": "E4", "schedule_id": "S3", "loan_id": "L3", "event_type": "PAYMENT", "payment_date": None, "amount": 0.0, "payment_status": "MISSED", "payment_method": "ACH"},
    ]
)
LOAN_PORTFOLIO_BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "interest_accrual": {"day_count_convention": "ACT/365", "accrues_on_statuses": ["ACTIVE"]},
}
LOAN_PORTFOLIO_VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0, "rate": 0.0001},
    "rules": [
        {"id": f"{metric}_reconciliation", "type": "reconciliation", "tolerance_type": tolerance_type, "description": "d"}
        for metric, tolerance_type in [
            ("loan_count", "count"), ("active_loan_count", "count"), ("closed_loan_count", "count"),
            ("defaulted_loan_count", "count"), ("total_funded_principal", "currency"),
            ("total_outstanding_principal", "currency"), ("avg_interest_rate", "rate"),
            ("total_accrued_interest", "currency"),
        ]
    ],
}


def test_loan_portfolio_correct_patch_verifies_and_promotes(spark_session, seeded_storage, tmp_path):
    bucket = seeded_storage.bucket
    s3a = _test_s3a_path(bucket)
    curated_key = PIPELINE_REGISTRY["loan_portfolio"].curated_keys[0]

    import src.etl_spark_loan_portfolio as etl_module

    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_WITHOUT_L3)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", LOAN_PORTFOLIO_PAYMENT_EVENTS)
    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = s3a
    try:
        buggy_summary_pd = compute_loan_portfolio(spark_session, LOAN_PORTFOLIO_BUSINESS_RULES, AS_OF_DATE).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{curated_key}", buggy_summary_pd)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_ALL)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_loan_portfolio(prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES, AS_OF_DATE)
    assert validation_before["overall_status"] == "FAIL"

    target_file = str(tmp_path / "etl_spark_loan_portfolio.py")
    workspace_dir = _write_workspace(tmp_path, target_file, LOAN_PORTFOLIO_SOURCE.read_text(encoding="utf-8"))
    repair_result = {"repair_status": "APPLIED", "target_file": target_file, "workspace_dir": str(workspace_dir)}

    result = run_verify_lifecycle_repair(
        "loan_portfolio", spark_session, prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES,
        validation_before, repair_result, run_id="test-loan-portfolio-good", s3a_path_override=s3a,
    )

    assert result["verification_status"] == "VERIFIED"
    assert result["validation_after"] == "PASS"
    assert result["rollback_performed"] is False
    assert result["metrics_after"][curated_key][0]["loan_count"] == 3

    assert Path(target_file).read_text(encoding="utf-8") == LOAN_PORTFOLIO_SOURCE.read_text(encoding="utf-8")
    promoted_curated = seeded_storage.read_parquet(f"{TEST_PREFIX}{curated_key}")
    assert promoted_curated.iloc[0]["loan_count"] == 3
    pipeline_run = seeded_storage.read_json(f"{TEST_PREFIX}{PIPELINE_RUN_KEY}")
    assert pipeline_run["pipelines"]["loan_portfolio"]["validation_status"] == "PASS"
    assert not workspace_dir.exists()


def test_loan_portfolio_still_buggy_patch_does_not_verify_or_promote(spark_session, seeded_storage, tmp_path):
    bucket = seeded_storage.bucket
    s3a = _test_s3a_path(bucket)
    curated_key = PIPELINE_REGISTRY["loan_portfolio"].curated_keys[0]

    import src.etl_spark_loan_portfolio as etl_module

    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_WITHOUT_L3)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", LOAN_PORTFOLIO_PAYMENT_EVENTS)
    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = s3a
    try:
        buggy_summary_pd = compute_loan_portfolio(spark_session, LOAN_PORTFOLIO_BUSINESS_RULES, AS_OF_DATE).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{curated_key}", buggy_summary_pd)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_ALL)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_loan_portfolio(prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES, AS_OF_DATE)

    still_buggy_source = LOAN_PORTFOLIO_SOURCE.read_text(encoding="utf-8").replace('how="left"', 'how="inner"')
    assert 'how="inner"' in still_buggy_source

    target_file = str(tmp_path / "etl_spark_loan_portfolio.py")
    workspace_dir = _write_workspace(tmp_path, target_file, still_buggy_source)
    repair_result = {"repair_status": "APPLIED", "target_file": target_file, "workspace_dir": str(workspace_dir)}

    result = run_verify_lifecycle_repair(
        "loan_portfolio", spark_session, prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES,
        validation_before, repair_result, run_id="test-loan-portfolio-bad", s3a_path_override=s3a,
    )

    assert result["verification_status"] == "NOT_VERIFIED"
    assert result["rollback_performed"] is True
    assert not Path(target_file).exists()
    unchanged_curated = seeded_storage.read_parquet(f"{TEST_PREFIX}{curated_key}")
    assert unchanged_curated.iloc[0]["loan_count"] == 2
    assert not workspace_dir.exists()


def test_loan_portfolio_create_pr_mode_builds_artifact_and_never_promotes(spark_session, seeded_storage, tmp_path):
    """mode="create_pr" must reach the exact same all_checks_pass=True state as the
    auto_promote test above, but build a PR artifact instead of touching the real repo file
    or the real curated output at all."""
    bucket = seeded_storage.bucket
    s3a = _test_s3a_path(bucket)
    curated_key = PIPELINE_REGISTRY["loan_portfolio"].curated_keys[0]

    import src.etl_spark_loan_portfolio as etl_module

    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_WITHOUT_L3)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", LOAN_PORTFOLIO_PAYMENT_EVENTS)
    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = s3a
    try:
        buggy_summary_pd = compute_loan_portfolio(spark_session, LOAN_PORTFOLIO_BUSINESS_RULES, AS_OF_DATE).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{curated_key}", buggy_summary_pd)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_ALL)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_loan_portfolio(prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES, AS_OF_DATE)
    assert validation_before["overall_status"] == "FAIL"

    # Uses the REAL repo file as target_file -- safe here because create_pr mode only ever
    # READS repair_result["target_file"] (for the diff's "before" side), never writes to it.
    real_target_file = str(LOAN_PORTFOLIO_SOURCE)
    real_source_before = LOAN_PORTFOLIO_SOURCE.read_text(encoding="utf-8")
    # A harmless trailing comment -- distinct text from the original (so there's a real diff
    # to commit/branch) without changing the ETL's behavior at all.
    patched_source = real_source_before + "\n# test fixture: cosmetic-only change\n"
    workspace_dir = _write_workspace(tmp_path, real_target_file, patched_source)
    repair_result = {"repair_status": "APPLIED", "target_file": real_target_file, "workspace_dir": str(workspace_dir)}
    diagnosis = {"root_cause_category": "ETL_LOGIC", "root_cause": "test fixture", "affected_metrics": ["total_outstanding_principal"]}
    repair_plan = {"change_summary": "test fixture patch"}

    result = run_verify_lifecycle_repair(
        "loan_portfolio", spark_session, prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES,
        validation_before, repair_result, run_id="test-loan-portfolio-create-pr", s3a_path_override=s3a,
        mode="create_pr", diagnosis=diagnosis, repair_plan=repair_plan,
    )

    try:
        assert result["verification_status"] == "VERIFIED_PENDING_PR"
        assert result["changed_files"] == []
        assert result["rollback_performed"] is False
        assert result["pr_artifact"] is not None
        assert result["pr_artifact"]["pipeline_name"] == "loan_portfolio"
        assert result["pr_artifact"]["risk_classification"] == "LOW"
        assert result["pr_artifact"]["branch"] is not None
        # context_provenance is populated for real from ContextRetriever, for the metric(s)
        # diagnosis named as affected -- loan_portfolio has generated+human context populated
        # (context/human/loan_portfolio.yaml), so this is real, non-legacy provenance.
        assert result["pr_artifact"]["context_provenance"]["total_outstanding_principal"]["provenance"] in ("human", "merged", "generated")

        # the real repo file and the real (still-buggy) curated output are BOTH untouched
        assert LOAN_PORTFOLIO_SOURCE.read_text(encoding="utf-8") == real_source_before
        still_buggy_curated = seeded_storage.read_parquet(f"{TEST_PREFIX}{curated_key}")
        assert still_buggy_curated.iloc[0]["loan_count"] == 2
        assert not workspace_dir.exists()
    finally:
        branch = result.get("pr_artifact", {}).get("branch") if result.get("pr_artifact") else None
        if branch:
            import subprocess

            subprocess.run(["git", "branch", "-D", branch], capture_output=True)


def test_loan_portfolio_create_pr_mode_with_real_git_worktree_sandbox(spark_session, seeded_storage, tmp_path):
    """The GitWorktreeSandbox wiring (src.lifecycle_run_self_healing's mode="create_pr" path):
    the SAME real git worktree/branch run_apply_lifecycle_repair would have produced the patch
    in is what this function reruns Spark against and what ends up holding the committed fix
    -- not a second, disconnected worktree created just for the PR artifact. Uses a real `git
    worktree add`/`commit` against this actual repo; branch (and worktree, if left over on
    failure) is force-cleaned up afterward regardless of outcome."""
    bucket = seeded_storage.bucket
    s3a = _test_s3a_path(bucket)
    curated_key = PIPELINE_REGISTRY["loan_portfolio"].curated_keys[0]

    import src.etl_spark_loan_portfolio as etl_module

    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_WITHOUT_L3)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", LOAN_PORTFOLIO_PAYMENT_EVENTS)
    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = s3a
    try:
        buggy_summary_pd = compute_loan_portfolio(spark_session, LOAN_PORTFOLIO_BUSINESS_RULES, AS_OF_DATE).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{curated_key}", buggy_summary_pd)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_ALL)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_loan_portfolio(prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES, AS_OF_DATE)
    assert validation_before["overall_status"] == "FAIL"

    real_target_file = str(LOAN_PORTFOLIO_SOURCE)
    real_source_before = LOAN_PORTFOLIO_SOURCE.read_text(encoding="utf-8")
    marker = "# test fixture: real-git-worktree-sandbox cosmetic-only change\n"
    patched_source = real_source_before + "\n" + marker

    sandbox = GitWorktreeSandbox(repo_root=Path("."))
    workspace_dir = sandbox.create_workspace(real_target_file)
    branch_created = sandbox._branches_by_workspace.get(str(workspace_dir))
    try:
        sandbox.workspace_path(workspace_dir, real_target_file).write_text(patched_source, encoding="utf-8")
        repair_result = {"repair_status": "APPLIED", "target_file": real_target_file, "workspace_dir": str(workspace_dir)}
        diagnosis = {"root_cause_category": "ETL_LOGIC", "root_cause": "test fixture"}
        repair_plan = {"change_summary": "real-git-worktree-sandbox test fixture patch"}

        result = run_verify_lifecycle_repair(
            "loan_portfolio", spark_session, prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES,
            validation_before, repair_result, run_id="test-loan-portfolio-real-worktree", s3a_path_override=s3a,
            mode="create_pr", diagnosis=diagnosis, repair_plan=repair_plan, sandbox_backend=sandbox,
        )

        assert result["verification_status"] == "VERIFIED_PENDING_PR"
        assert result["pr_artifact"] is not None
        branch = result["pr_artifact"]["branch"]
        assert branch == branch_created  # the SAME worktree/branch apply would have used, not a second one

        # The branch really has the fix committed...
        committed_content = subprocess.run(
            ["git", "show", f"{branch}:{real_target_file}"], capture_output=True, text=True, check=True
        ).stdout
        assert marker.strip() in committed_content

        # ...while the real repo file at HEAD, and the real (still-buggy) curated output, are untouched.
        assert LOAN_PORTFOLIO_SOURCE.read_text(encoding="utf-8") == real_source_before
        still_buggy_curated = seeded_storage.read_parquet(f"{TEST_PREFIX}{curated_key}")
        assert still_buggy_curated.iloc[0]["loan_count"] == 2
        assert not workspace_dir.exists()  # worktree checkout removed; branch kept
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(workspace_dir)], capture_output=True)
        if branch_created:
            subprocess.run(["git", "branch", "-D", branch_created], capture_output=True)


def test_blocked_repair_status_short_circuits_without_touching_anything(spark_session, seeded_storage):
    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = {"overall_status": "FAIL", "checks": [{"id": "loan_count_reconciliation", "status": "FAIL"}]}
    repair_result = {"repair_status": "BLOCKED", "target_file": None, "workspace_dir": None}

    result = run_verify_lifecycle_repair(
        "loan_portfolio", spark_session, prefixed, LOAN_PORTFOLIO_BUSINESS_RULES, LOAN_PORTFOLIO_VALIDATION_RULES,
        validation_before, repair_result,
    )
    assert result["verification_status"] == "BLOCKED"
    assert result["rollback_performed"] is False


# --- delinquency_default: a structurally DIFFERENT bug shape (business-rule-mismatch, not
# a join bug) -- proves the generalized loop isn't just replaying the loan_portfolio case ---

DD_CUSTOMERS = pd.DataFrame(
    [
        {"customer_id": "C1", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "680_719", "credit_score": 700, "risk_segment": "LOW"},
        {"customer_id": "C2", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "620_679", "credit_score": 650, "risk_segment": "HIGH"},
    ]
)
DD_LOANS = pd.DataFrame(
    [
        {"loan_id": "L1", "application_id": "APP1", "customer_id": "C1", "principal_amount": 1000.0, "interest_rate": 0.05, "term_months": 12, "originated_at": "2025-01-01", "loan_status": "ACTIVE", "scheduled_payment_amount": 83.33},
        {"loan_id": "L2", "application_id": "APP2", "customer_id": "C2", "principal_amount": 2000.0, "interest_rate": 0.15, "term_months": 12, "originated_at": "2025-01-01", "loan_status": "DEFAULTED", "scheduled_payment_amount": 166.67},
    ]
)
DD_DELINQUENCY_EVENTS = pd.DataFrame(
    [{"delinquency_id": "DLQ1", "loan_id": "L2", "as_of_date": "2026-07-20", "days_past_due": 45, "bucket": "60"}]
)
DD_DEFAULTS = pd.DataFrame(
    [{"default_id": "DEF1", "loan_id": "L2", "default_date": "2026-06-01", "balance_at_default": 1500.0, "recovery_amount": 300.0, "recovery_date": "2026-07-01"}]
)
DD_BUSINESS_RULES = {"loss_rate_denominator": "total_funded_principal"}
DD_VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0, "rate": 0.0001},
    "rules": [{"id": "delinquency_default_breakdown_rows_match", "type": "reconciliation", "tolerance_type": "count", "description": "d"}],
}


def _seed_delinquency_default_raw(storage) -> None:
    storage.write_parquet(f"{TEST_PREFIX}raw/customers.parquet", DD_CUSTOMERS)
    storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", DD_LOANS)
    storage.write_parquet(f"{TEST_PREFIX}raw/delinquency_events.parquet", DD_DELINQUENCY_EVENTS)
    storage.write_parquet(f"{TEST_PREFIX}raw/defaults.parquet", DD_DEFAULTS)


def _wrong_denominator_source() -> str:
    """A patch that ignores business_rules.loss_rate_denominator and hardcodes the wrong
    column -- a business-rule-mismatch bug, structurally different from loan_portfolio's
    inner-join bug. Matches the "loss_denominator_column = ..." line by regex (not an exact
    hardcoded string) so this stays correct regardless of which equivalent access style
    (business_rules["k"] vs business_rules.get("k", default)) the real, currently-correct
    source happens to use -- a live repair is free to choose either."""
    source = DELINQUENCY_DEFAULT_SOURCE.read_text(encoding="utf-8")
    buggy_assignment = 'loss_denominator_column = "total_balance_at_default"  # ignores business_rules'
    patched, count = re.subn(
        r"^([ \t]*)loss_denominator_column = .*$",
        lambda m: m.group(1) + buggy_assignment,
        source, count=1, flags=re.MULTILINE,
    )
    assert count == 1, "expected exactly one loss_denominator_column assignment in the real source"
    return patched


def test_delinquency_default_correct_patch_verifies_and_promotes(spark_session, seeded_storage, tmp_path):
    bucket = seeded_storage.bucket
    s3a = _test_s3a_path(bucket)
    curated_key = PIPELINE_REGISTRY["delinquency_default"].curated_keys[0]

    _seed_delinquency_default_raw(seeded_storage)

    import src.etl_spark_delinquency_default as etl_module

    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = s3a
    try:
        # The pre-repair "buggy" curated snapshot: computed with the WRONG denominator.
        buggy_workspace_dir = _write_workspace(tmp_path, "buggy_marker.py", _wrong_denominator_source())
        buggy_module = None
        import importlib.util

        buggy_path = _workspace_path(buggy_workspace_dir, "buggy_marker.py")
        spec_obj = importlib.util.spec_from_file_location("buggy_delinquency_default", buggy_path)
        buggy_module = importlib.util.module_from_spec(spec_obj)
        spec_obj.loader.exec_module(buggy_module)
        buggy_module.s3a_path = s3a
        buggy_summary_pd = buggy_module.compute_delinquency_default(spark_session, DD_BUSINESS_RULES).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{curated_key}", buggy_summary_pd)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_delinquency_default(prefixed, DD_BUSINESS_RULES, DD_VALIDATION_RULES)
    assert validation_before["overall_status"] == "FAIL"

    target_file = str(tmp_path / "etl_spark_delinquency_default.py")
    workspace_dir = _write_workspace(tmp_path, target_file, DELINQUENCY_DEFAULT_SOURCE.read_text(encoding="utf-8"))
    repair_result = {"repair_status": "APPLIED", "target_file": target_file, "workspace_dir": str(workspace_dir)}

    result = run_verify_lifecycle_repair(
        "delinquency_default", spark_session, prefixed, DD_BUSINESS_RULES, DD_VALIDATION_RULES,
        validation_before, repair_result, run_id="test-dd-good", s3a_path_override=s3a,
    )

    assert result["verification_status"] == "VERIFIED"
    assert result["validation_after"] == "PASS"
    assert result["rollback_performed"] is False

    promoted = seeded_storage.read_parquet(f"{TEST_PREFIX}{curated_key}")
    high_row = promoted[promoted["breakdown_value"] == "HIGH"].iloc[0]
    assert high_row["loss_rate"] == pytest.approx(0.6, abs=1e-4)  # (1500-300)/2000, the CORRECT denominator
    assert not workspace_dir.exists()


def test_delinquency_default_still_wrong_denominator_does_not_verify(spark_session, seeded_storage, tmp_path):
    bucket = seeded_storage.bucket
    s3a = _test_s3a_path(bucket)
    curated_key = PIPELINE_REGISTRY["delinquency_default"].curated_keys[0]

    _seed_delinquency_default_raw(seeded_storage)

    import importlib.util

    wrong_source = _wrong_denominator_source()
    import src.etl_spark_delinquency_default as etl_module

    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = s3a
    try:
        buggy_workspace_dir = _write_workspace(tmp_path, "buggy_marker.py", wrong_source)
        buggy_path = _workspace_path(buggy_workspace_dir, "buggy_marker.py")
        spec_obj = importlib.util.spec_from_file_location("buggy_delinquency_default2", buggy_path)
        buggy_module = importlib.util.module_from_spec(spec_obj)
        spec_obj.loader.exec_module(buggy_module)
        buggy_module.s3a_path = s3a
        buggy_summary_pd = buggy_module.compute_delinquency_default(spark_session, DD_BUSINESS_RULES).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{curated_key}", buggy_summary_pd)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_delinquency_default(prefixed, DD_BUSINESS_RULES, DD_VALIDATION_RULES)

    # The "repair" is still wrong -- same bug, not actually fixed.
    target_file = str(tmp_path / "etl_spark_delinquency_default.py")
    workspace_dir = _write_workspace(tmp_path, target_file, wrong_source)
    repair_result = {"repair_status": "APPLIED", "target_file": target_file, "workspace_dir": str(workspace_dir)}

    result = run_verify_lifecycle_repair(
        "delinquency_default", spark_session, prefixed, DD_BUSINESS_RULES, DD_VALIDATION_RULES,
        validation_before, repair_result, run_id="test-dd-bad", s3a_path_override=s3a,
    )

    assert result["verification_status"] == "NOT_VERIFIED"
    assert result["rollback_performed"] is True
    assert not Path(target_file).exists()


# --- Atomic multi-key promotion: a failure partway through must roll back EVERY key -------

UP_CUSTOMERS = pd.DataFrame(
    [
        {"customer_id": "C1", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "680_719", "credit_score": 700, "risk_segment": "LOW"},
        {"customer_id": "C2", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "620_679", "credit_score": 650, "risk_segment": "HIGH"},
    ]
)
UP_APPLICATIONS = pd.DataFrame(
    [
        {"application_id": "APP1", "customer_id": "C1", "offer_id": None, "requested_amount": 5000.0, "submitted_at": "2025-01-01", "application_status": "DECISIONED"},
        {"application_id": "APP2", "customer_id": "C2", "offer_id": None, "requested_amount": 4000.0, "submitted_at": "2025-01-01", "application_status": "DECISIONED"},
    ]
)
UP_UNDERWRITING_DECISIONS = pd.DataFrame(
    [
        {"decision_id": "DEC1", "application_id": "APP1", "decision": "APPROVED", "rejection_reason": None, "approved_amount": 4800.0, "approved_apr": 0.06, "model_version": "uw-v1", "decided_at": "2025-01-02"},
        {"decision_id": "DEC2", "application_id": "APP2", "decision": "REJECTED", "rejection_reason": "LOW_CREDIT_SCORE", "approved_amount": None, "approved_apr": None, "model_version": "uw-v1", "decided_at": "2025-01-02"},
    ]
)
UP_VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0, "rate": 0.0001},
    "rules": [
        {"id": "underwriting_performance_breakdown_rows_match", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "underwriting_performance_rejection_distribution_matches", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
    ],
}


def test_promotion_failure_partway_through_rolls_back_every_key(spark_session, seeded_storage, tmp_path, monkeypatch):
    bucket = seeded_storage.bucket
    s3a = _test_s3a_path(bucket)
    spec = PIPELINE_REGISTRY["underwriting_performance"]
    performance_key, rejections_key = spec.curated_keys

    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/customers.parquet", UP_CUSTOMERS)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/applications.parquet", UP_APPLICATIONS)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/underwriting_decisions.parquet", UP_UNDERWRITING_DECISIONS)

    # Seed a plausible "before" state for both curated keys, and an existing pipeline_run.json
    # naming an unrelated pipeline (must survive this test untouched).
    stale_performance = pd.DataFrame([{"breakdown_type": "risk_segment", "breakdown_value": "LOW", "decision_count": 1, "approved_count": 1, "rejected_count": 0, "manual_review_count": 0, "approval_rate": 1.0, "avg_approved_amount": 1.0, "avg_approved_apr": 0.01}])
    stale_rejections = pd.DataFrame([{"rejection_reason": "LOW_CREDIT_SCORE", "count": 999}])
    seeded_storage.write_parquet(f"{TEST_PREFIX}{performance_key}", stale_performance)
    seeded_storage.write_parquet(f"{TEST_PREFIX}{rejections_key}", stale_rejections)
    seeded_storage.write_json(f"{TEST_PREFIX}{PIPELINE_RUN_KEY}", {"pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"}}, "overall_status": "SUCCESS"})

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = {
        "overall_status": "FAIL",
        "checks": [
            {"id": "underwriting_performance_breakdown_rows_match", "status": "FAIL"},
            {"id": "underwriting_performance_rejection_distribution_matches", "status": "PASS"},
        ],
    }

    # A correct, real "patch" (the unmodified source) -- every check passes, so verification
    # reaches the promotion step, where we then inject an artificial failure.
    target_file = str(tmp_path / "etl_spark_underwriting_performance.py")
    workspace_dir = _write_workspace(tmp_path, target_file, UNDERWRITING_PERFORMANCE_SOURCE.read_text(encoding="utf-8"))
    repair_result = {"repair_status": "APPLIED", "target_file": target_file, "workspace_dir": str(workspace_dir)}

    real_write_parquet = seeded_storage.write_parquet.__func__

    def _flaky_write_parquet(self, path, df):
        if path == f"{TEST_PREFIX}{rejections_key}":
            raise RuntimeError("simulated write failure partway through promotion")
        return real_write_parquet(self, path, df)

    monkeypatch.setattr(type(seeded_storage), "write_parquet", _flaky_write_parquet)

    result = run_verify_lifecycle_repair(
        "underwriting_performance", spark_session, prefixed, {}, UP_VALIDATION_RULES,
        validation_before, repair_result, run_id="test-rollback", s3a_path_override=s3a,
    )

    assert result["verification_status"] == "NOT_VERIFIED"
    assert result["rollback_performed"] is True

    # Every key -- including the one written successfully BEFORE the failure -- is back to
    # its pre-repair state. Not a mix of old and new. (read_parquet was never monkeypatched,
    # so these reads reflect real, current object content.)
    restored_performance = seeded_storage.read_parquet(f"{TEST_PREFIX}{performance_key}")
    restored_rejections = seeded_storage.read_parquet(f"{TEST_PREFIX}{rejections_key}")
    pd.testing.assert_frame_equal(restored_performance.reset_index(drop=True), stale_performance.reset_index(drop=True))
    pd.testing.assert_frame_equal(restored_rejections.reset_index(drop=True), stale_rejections.reset_index(drop=True))

    pipeline_run = seeded_storage.read_json(f"{TEST_PREFIX}{PIPELINE_RUN_KEY}")
    assert pipeline_run["pipelines"] == {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"}}

    # No leftover backup objects.
    assert seeded_storage.list_paths(f"{TEST_PREFIX}_backup/") == []
    # The real target file was never touched (promotion never reached the local-file step).
    assert not Path(target_file).exists()


# --- _run_pytest_against_patched_code: swap-in/restore behavior ---------------------------


def test_run_pytest_against_patched_code_swaps_in_the_patch_and_always_restores(tmp_path):
    real_target = tmp_path / "target.py"
    real_target.write_text("VALUE = 'original'\n")
    workspace_dir = tmp_path / "workspace"
    patched_path = workspace_dir / str(tmp_path).lstrip("/") / "target.py"
    patched_path.parent.mkdir(parents=True, exist_ok=True)
    patched_path.write_text("VALUE = 'patched'\n")

    seen_during_test = {}

    def _fake_pytest(test_files):
        seen_during_test["content"] = real_target.read_text()
        return "PASS"

    original_run_pytest = verify_module._run_pytest
    verify_module._run_pytest = _fake_pytest
    try:
        status = _REAL_RUN_PYTEST_AGAINST_PATCHED_CODE(["dummy"], workspace_dir, str(real_target), TempDirSandbox())
    finally:
        verify_module._run_pytest = original_run_pytest

    assert status == "PASS"
    assert seen_during_test["content"] == "VALUE = 'patched'\n"
    # Restored to the original after returning, regardless of outcome.
    assert real_target.read_text() == "VALUE = 'original'\n"


def test_run_pytest_against_patched_code_restores_even_when_pytest_raises(tmp_path):
    real_target = tmp_path / "target.py"
    real_target.write_text("VALUE = 'original'\n")
    workspace_dir = tmp_path / "workspace"
    patched_path = workspace_dir / str(tmp_path).lstrip("/") / "target.py"
    patched_path.parent.mkdir(parents=True, exist_ok=True)
    patched_path.write_text("VALUE = 'patched'\n")

    def _raising_pytest(test_files):
        raise RuntimeError("boom")

    original_run_pytest = verify_module._run_pytest
    verify_module._run_pytest = _raising_pytest
    try:
        with pytest.raises(RuntimeError):
            _REAL_RUN_PYTEST_AGAINST_PATCHED_CODE(["dummy"], workspace_dir, str(real_target), TempDirSandbox())
    finally:
        verify_module._run_pytest = original_run_pytest

    assert real_target.read_text() == "VALUE = 'original'\n"


# --- _resolve_rerun_inputs: CONFIGURATION_CHANGE vs CODE_CHANGE rerun -----------------------
#
# Pure file-IO logic, no Spark/S3 needed.


def _fake_spec(pipeline_configuration_file):
    return type(
        "Spec",
        (),
        {"pipeline_configuration_file": pipeline_configuration_file, "etl_source_file": "src/etl_spark_loan_portfolio.py"},
    )()


def test_resolve_rerun_inputs_for_a_code_change_target_is_unaffected(tmp_path):
    """The default (CODE_CHANGE) case must be byte-identical to before this branch existed:
    the patched module comes from the workspace, business_rules passes through unchanged."""
    spec = _fake_spec(pipeline_configuration_file="context/pipeline_rules/loan_portfolio.json")
    workspace_dir = tmp_path / "workspace"
    patched_path = workspace_dir / "src" / "etl_spark_loan_portfolio.py"
    patched_path.parent.mkdir(parents=True, exist_ok=True)
    patched_path.write_text("MARKER = 'patched-etl'\n")
    business_rules = {"successful_payment_statuses": ["PAID"]}

    module, effective_business_rules = verify_module._resolve_rerun_inputs(
        spec, "src/etl_spark_loan_portfolio.py", workspace_dir, TempDirSandbox(), business_rules
    )

    assert module.MARKER == "patched-etl"
    assert effective_business_rules is business_rules


def test_resolve_rerun_inputs_for_a_configuration_change_target_reads_the_candidate_pointer(tmp_path):
    """For loan_portfolio's registered pointer file: resolve business_rules from the
    CANDIDATE'S patched pointer in the workspace (never real S3, which still has the stale,
    not-yet-promoted value), and rerun the real, UNTOUCHED etl_spark_loan_portfolio module
    (there is no patched Python for a config-only change)."""
    import json

    target_file = "context/pipeline_rules/loan_portfolio.json"
    spec = _fake_spec(pipeline_configuration_file=target_file)
    workspace_dir = tmp_path / "workspace"
    patched_path = workspace_dir / target_file
    patched_path.parent.mkdir(parents=True, exist_ok=True)
    patched_path.write_text(json.dumps({"business_rules_file": "context/business_rules_demo.json"}))
    stale_business_rules = {"successful_payment_statuses": ["PAID"]}

    module, effective_business_rules = verify_module._resolve_rerun_inputs(
        spec, target_file, workspace_dir, TempDirSandbox(), stale_business_rules
    )

    assert module.__name__ == "src.etl_spark_loan_portfolio"
    assert effective_business_rules["successful_payment_statuses"] == ["PAID", "SETTLED"]
    assert effective_business_rules is not stale_business_rules
