"""Tests for the payment_performance PySpark ETL + its independent pandas validator."""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl_spark_payment_performance import compute_payment_performance
from src.validate_payment_performance import validate_payment_performance
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_payment_performance/"
AS_OF_DATE = "2026-07-20"

BUSINESS_RULES = {"successful_payment_statuses": ["PAID"], "prepayment_threshold_days": 3}

# S1 due 2026-06-01 (past): PAID 5 days early (2026-05-27) -- a prepayment (>=3 days early).
# S2 due 2026-06-15 (past): PAID 1 day early (2026-06-14) -- NOT a prepayment (<3 days).
# S3 due 2026-06-20 (past): MISSED, scheduled_amount 200 -- contributes to missed_amount, not collected.
# S4 due 2026-07-01 (past): LATE.
# S5 due 2026-08-01 (FUTURE, after as_of_date) -- excluded from expected_payment_count/expected_amount_due.
PAYMENT_SCHEDULE = pd.DataFrame(
    [
        {"schedule_id": "S1", "loan_id": "L1", "installment_number": 1, "due_date": "2026-06-01", "scheduled_amount": 100.0},
        {"schedule_id": "S2", "loan_id": "L1", "installment_number": 2, "due_date": "2026-06-15", "scheduled_amount": 100.0},
        {"schedule_id": "S3", "loan_id": "L2", "installment_number": 1, "due_date": "2026-06-20", "scheduled_amount": 200.0},
        {"schedule_id": "S4", "loan_id": "L2", "installment_number": 2, "due_date": "2026-07-01", "scheduled_amount": 200.0},
        {"schedule_id": "S5", "loan_id": "L2", "installment_number": 3, "due_date": "2026-08-01", "scheduled_amount": 200.0},
    ]
)
PAYMENT_EVENTS = pd.DataFrame(
    [
        {"event_id": "E1", "schedule_id": "S1", "loan_id": "L1", "event_type": "PAYMENT", "payment_date": "2026-05-27", "amount": 100.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E2", "schedule_id": "S2", "loan_id": "L1", "event_type": "PAYMENT", "payment_date": "2026-06-14", "amount": 100.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E3", "schedule_id": "S3", "loan_id": "L2", "event_type": "PAYMENT", "payment_date": None, "amount": 0.0, "payment_status": "MISSED", "payment_method": "ACH"},
        {"event_id": "E4", "schedule_id": "S4", "loan_id": "L2", "event_type": "PAYMENT", "payment_date": "2026-07-10", "amount": 200.0, "payment_status": "LATE", "payment_method": "ACH"},
    ]
)

VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0, "rate": 0.0001},
    "rules": [
        {"id": f"{m}_reconciliation", "type": "reconciliation", "tolerance_type": t, "description": "d"}
        for m, t in [
            ("expected_payment_count", "count"), ("expected_amount_due", "currency"),
            ("successful_payment_count", "count"), ("total_collected_amount", "currency"),
            ("missed_payment_count", "count"), ("missed_amount", "currency"),
            ("late_payment_count", "count"), ("failed_payment_count", "count"),
            ("collection_rate", "rate"), ("prepayment_rate", "rate"),
        ]
    ],
}


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/payment_schedule.parquet", PAYMENT_SCHEDULE)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", PAYMENT_EVENTS)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


@pytest.fixture
def patched(seeded_storage, monkeypatch):
    import src.etl_spark_payment_performance as etl_module

    monkeypatch.setattr(
        etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts)
    )
    return seeded_storage


def test_compute_payment_performance_matches_hand_verified_numbers(spark_session, patched):
    result = compute_payment_performance(spark_session, BUSINESS_RULES, AS_OF_DATE).collect()[0].asDict()

    # S5 (future) excluded -- only S1-S4 are "expected".
    assert result["expected_payment_count"] == 4
    assert result["expected_amount_due"] == 600.0
    assert result["successful_payment_count"] == 2
    assert result["total_collected_amount"] == 200.0
    assert result["missed_payment_count"] == 1
    assert result["missed_amount"] == 200.0
    assert result["late_payment_count"] == 1
    assert result["failed_payment_count"] == 0
    assert result["collection_rate"] == pytest.approx(200.0 / 600.0, abs=1e-4)
    # Only E1 (5 days early) meets the >=3 day prepayment threshold; E2 (1 day early) doesn't.
    assert result["prepayment_rate"] == pytest.approx(0.5, abs=1e-4)


def test_validator_passes_against_real_etl_output(spark_session, patched):
    summary_df = compute_payment_performance(spark_session, BUSINESS_RULES, AS_OF_DATE)
    wrapped = PrefixedStorage(patched, TEST_PREFIX)
    wrapped.write_parquet("curated/payment_performance.parquet", summary_df.toPandas())

    result = validate_payment_performance(wrapped, BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE)
    assert result["overall_status"] == "PASS"


def test_validator_fails_on_wrong_missed_amount(spark_session, patched):
    summary_df = compute_payment_performance(spark_session, BUSINESS_RULES, AS_OF_DATE)
    wrong = summary_df.toPandas()
    wrong.loc[0, "missed_amount"] = 0.0

    wrapped = PrefixedStorage(patched, TEST_PREFIX)
    wrapped.write_parquet("curated/payment_performance.parquet", wrong)

    result = validate_payment_performance(wrapped, BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE)
    assert result["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["missed_amount_reconciliation"]["status"] == "FAIL"
