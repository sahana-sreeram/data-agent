"""Tests for deterministic post-repair verification and promotion of the loan_portfolio
lifecycle pipeline. Against a REAL local Spark session and S3-compatible endpoint (MinIO),
using a dedicated test prefix so these tests never touch real migrated raw/curated data or
the real repository's src/etl_spark_loan_portfolio.py file -- promotion in these tests always
targets a tmp_path file, never the real one. Skips cleanly if Spark/S3 aren't reachable.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.apply_repair import _workspace_path
from src.etl_spark_loan_portfolio import compute_loan_portfolio
from src.lifecycle_verify_repair import CURATED_KEY, PIPELINE_RUN_KEY, run_verify_lifecycle_repair
from src.validate_loan_portfolio import validate_loan_portfolio
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_lifecycle_verify_repair/"
AS_OF_DATE = "2026-07-20"
REAL_ETL_SOURCE = Path("src/etl_spark_loan_portfolio.py")

BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "interest_accrual": {"day_count_convention": "ACT/365", "accrues_on_statuses": ["ACTIVE"]},
}
VALIDATION_RULES = {
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

# L1 (CLOSED, fully paid) and L2 (ACTIVE, one PAID+one REVERSAL netting to 0 paid) are the
# same shape as tests/test_etl_spark_loan_portfolio.py. L3 is the bug-reproduction case: an
# ACTIVE loan with a real payment_event that is neither PAID nor REVERSED -- exactly the
# "L000106/L000125" shape found in the real dataset -- so an inner join drops it entirely.
LOANS_ALL = pd.DataFrame(
    [
        {"loan_id": "L1", "application_id": "APP1", "customer_id": "C1", "principal_amount": 1000.0, "interest_rate": 0.05, "term_months": 12, "originated_at": "2024-01-01", "loan_status": "CLOSED", "scheduled_payment_amount": 83.33},
        {"loan_id": "L2", "application_id": "APP2", "customer_id": "C2", "principal_amount": 2000.0, "interest_rate": 0.10, "term_months": 24, "originated_at": "2025-07-20", "loan_status": "ACTIVE", "scheduled_payment_amount": 83.33},
        {"loan_id": "L3", "application_id": "APP3", "customer_id": "C3", "principal_amount": 1500.0, "interest_rate": 0.08, "term_months": 24, "originated_at": "2025-07-20", "loan_status": "ACTIVE", "scheduled_payment_amount": 62.50},
    ]
)
LOANS_WITHOUT_L3 = LOANS_ALL[LOANS_ALL["loan_id"] != "L3"].reset_index(drop=True)
PAYMENT_EVENTS = pd.DataFrame(
    [
        {"event_id": "E1", "schedule_id": "S1", "loan_id": "L1", "event_type": "PAYMENT", "payment_date": "2024-02-01", "amount": 1000.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E2", "schedule_id": "S2", "loan_id": "L2", "event_type": "PAYMENT", "payment_date": "2025-08-20", "amount": 500.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E3", "schedule_id": "S2", "loan_id": "L2", "event_type": "REVERSAL", "payment_date": "2025-08-25", "amount": -500.0, "payment_status": "REVERSED", "payment_method": "ACH"},
        {"event_id": "E4", "schedule_id": "S3", "loan_id": "L3", "event_type": "PAYMENT", "payment_date": None, "amount": 0.0, "payment_status": "MISSED", "payment_method": "ACH"},
    ]
)


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
    # run_verify_lifecycle_repair's test_inventory (tests/test_etl_spark_loan_portfolio.py)
    # needs the shared session-scoped spark_session fixture -- invoking it via a NESTED
    # pytest.main() call would tear down that shared SparkSession at the nested run's
    # teardown, breaking every other Spark-dependent test in this same pytest session. The
    # pytest-invocation plumbing itself (_run_pytest) is a two-line pytest.main() wrapper,
    # identical in shape to the already-proven src.verify_repair._run_pytest -- stub it out
    # here and test the surrounding verification/promotion logic instead.
    import src.lifecycle_verify_repair as verify_module

    monkeypatch.setattr(verify_module, "_run_pytest", lambda test_files: "PASS")


def test_correct_patch_verifies_and_promotes(spark_session, seeded_storage, tmp_path):
    bucket = seeded_storage.bucket
    monkeypatch_s3a = _test_s3a_path(bucket)

    import src.etl_spark_loan_portfolio as etl_module

    # Compute the BUGGY pre-repair curated snapshot (as if an inner join had already dropped
    # L3), using the real compute_loan_portfolio against a temporarily L3-less raw seed.
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_WITHOUT_L3)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", PAYMENT_EVENTS)
    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = monkeypatch_s3a
    try:
        buggy_summary_pd = compute_loan_portfolio(spark_session, BUSINESS_RULES, AS_OF_DATE).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{CURATED_KEY}", buggy_summary_pd)

    # Now seed the REAL, full raw data (all 3 loans) -- what verification reruns against.
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_ALL)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_loan_portfolio(prefixed, BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE)
    assert validation_before["overall_status"] == "FAIL"
    assert "loan_count_reconciliation" in [c["id"] for c in validation_before["checks"] if c["status"] == "FAIL"]

    target_file = str(tmp_path / "etl_spark_loan_portfolio.py")
    workspace_dir = _write_workspace(tmp_path, target_file, REAL_ETL_SOURCE.read_text(encoding="utf-8"))
    repair_result = {"repair_status": "APPLIED", "target_file": target_file, "workspace_dir": str(workspace_dir)}

    result = run_verify_lifecycle_repair(
        spark_session, prefixed, BUSINESS_RULES, VALIDATION_RULES, validation_before, repair_result,
        s3a_path_override=monkeypatch_s3a,
    )

    assert result["verification_status"] == "VERIFIED"
    assert result["validation_after"] == "PASS"
    assert result["failed_checks_after"] == []
    assert result["rollback_performed"] is False
    assert result["metrics_after"]["loan_count"] == 3

    # Promoted: the tmp target file now has the workspace's (correct) content.
    assert Path(target_file).exists()
    assert Path(target_file).read_text(encoding="utf-8") == REAL_ETL_SOURCE.read_text(encoding="utf-8")

    promoted_curated = seeded_storage.read_parquet(f"{TEST_PREFIX}{CURATED_KEY}")
    assert promoted_curated.iloc[0]["loan_count"] == 3

    pipeline_run = seeded_storage.read_json(f"{TEST_PREFIX}{PIPELINE_RUN_KEY}")
    assert pipeline_run["pipelines"]["loan_portfolio"]["validation_status"] == "PASS"

    assert not workspace_dir.exists()


def test_still_buggy_patch_does_not_verify_or_promote(spark_session, seeded_storage, tmp_path):
    bucket = seeded_storage.bucket
    monkeypatch_s3a = _test_s3a_path(bucket)

    import src.etl_spark_loan_portfolio as etl_module

    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_WITHOUT_L3)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", PAYMENT_EVENTS)
    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = monkeypatch_s3a
    try:
        buggy_summary_pd = compute_loan_portfolio(spark_session, BUSINESS_RULES, AS_OF_DATE).toPandas()
    finally:
        etl_module.s3a_path = original_s3a_path
    seeded_storage.write_parquet(f"{TEST_PREFIX}{CURATED_KEY}", buggy_summary_pd)
    seeded_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS_ALL)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = validate_loan_portfolio(prefixed, BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE)

    # A "patch" that doesn't actually fix anything -- still uses how="inner".
    still_buggy_source = REAL_ETL_SOURCE.read_text(encoding="utf-8").replace('how="left"', 'how="inner"')
    assert 'how="inner"' in still_buggy_source

    target_file = str(tmp_path / "etl_spark_loan_portfolio.py")
    workspace_dir = _write_workspace(tmp_path, target_file, still_buggy_source)
    repair_result = {"repair_status": "APPLIED", "target_file": target_file, "workspace_dir": str(workspace_dir)}

    result = run_verify_lifecycle_repair(
        spark_session, prefixed, BUSINESS_RULES, VALIDATION_RULES, validation_before, repair_result,
        s3a_path_override=monkeypatch_s3a,
    )

    assert result["verification_status"] == "NOT_VERIFIED"
    assert result["rollback_performed"] is True
    assert not Path(target_file).exists()  # promotion never happened

    # Curated data under the test prefix is untouched (still the pre-repair buggy snapshot).
    unchanged_curated = seeded_storage.read_parquet(f"{TEST_PREFIX}{CURATED_KEY}")
    assert unchanged_curated.iloc[0]["loan_count"] == 2

    assert not workspace_dir.exists()


def test_blocked_repair_status_short_circuits_without_touching_anything(spark_session, seeded_storage):
    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    validation_before = {"overall_status": "FAIL", "checks": [{"id": "loan_count_reconciliation", "status": "FAIL"}]}
    repair_result = {"repair_status": "BLOCKED", "target_file": None, "workspace_dir": None}

    result = run_verify_lifecycle_repair(
        spark_session, prefixed, BUSINESS_RULES, VALIDATION_RULES, validation_before, repair_result
    )
    assert result["verification_status"] == "BLOCKED"
    assert result["rollback_performed"] is False
