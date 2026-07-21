"""Tests for the loan_portfolio PySpark ETL + its independent pandas validator.

Against a REAL local Spark session and S3-compatible endpoint (MinIO), using a
dedicated test prefix so these tests never collide with the real migrated
raw/curated data. Skips cleanly (via the shared conftest.py fixtures) if
Spark/S3 aren't reachable in this environment.
"""

from __future__ import annotations

import copy

import pandas as pd
import pytest

from src.etl_spark_loan_portfolio import compute_loan_portfolio, write_curated
from src.validate_loan_portfolio import validate_loan_portfolio
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_loan_portfolio/"
AS_OF_DATE = "2026-07-20"

BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "interest_accrual": {"day_count_convention": "ACT/365", "accrues_on_statuses": ["ACTIVE"]},
}

VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0, "rate": 0.0001},
    "rules": [
        {"id": f"{metric}_reconciliation", "type": "reconciliation", "tolerance_type": tolerance_type, "description": "d"}
        for metric, tolerance_type in [
            ("loan_count", "count"),
            ("active_loan_count", "count"),
            ("closed_loan_count", "count"),
            ("defaulted_loan_count", "count"),
            ("total_funded_principal", "currency"),
            ("total_outstanding_principal", "currency"),
            ("avg_interest_rate", "rate"),
            ("total_accrued_interest", "currency"),
        ]
    ],
}

# Two loans: L1 is CLOSED, fully paid (principal 1000, one PAID event of 1000 -- no accrual,
# CLOSED isn't in accrues_on_statuses). L2 is ACTIVE, principal 2000, 10% rate, originated
# exactly 365 days before as_of_date (so accrued_interest = 2000 * 0.10 * 365/365 = 200.00
# exactly), with one PAID event of 500 and one REVERSAL of -500 netting to zero paid -- so
# outstanding_principal for L2 is the full 2000 (proving REVERSAL nets out PAID correctly).
LOANS = pd.DataFrame(
    [
        {
            "loan_id": "L1",
            "application_id": "APP1",
            "customer_id": "C1",
            "principal_amount": 1000.0,
            "interest_rate": 0.05,
            "term_months": 12,
            "originated_at": "2024-01-01",
            "loan_status": "CLOSED",
            "scheduled_payment_amount": 83.33,
        },
        {
            "loan_id": "L2",
            "application_id": "APP2",
            "customer_id": "C2",
            "principal_amount": 2000.0,
            "interest_rate": 0.10,
            "term_months": 24,
            "originated_at": "2025-07-20",
            "loan_status": "ACTIVE",
            "scheduled_payment_amount": 83.33,
        },
    ]
)
PAYMENT_EVENTS = pd.DataFrame(
    [
        {"event_id": "E1", "schedule_id": "S1", "loan_id": "L1", "event_type": "PAYMENT", "payment_date": "2024-02-01", "amount": 1000.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E2", "schedule_id": "S2", "loan_id": "L2", "event_type": "PAYMENT", "payment_date": "2025-08-20", "amount": 500.0, "payment_status": "PAID", "payment_method": "ACH"},
        {"event_id": "E3", "schedule_id": "S2", "loan_id": "L2", "event_type": "REVERSAL", "payment_date": "2025-08-25", "amount": -500.0, "payment_status": "REVERSED", "payment_method": "ACH"},
    ]
)


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/payment_events.parquet", PAYMENT_EVENTS)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


def test_compute_loan_portfolio_matches_hand_verified_numbers(spark_session, seeded_storage, monkeypatch):
    import src.etl_spark_loan_portfolio as etl_module

    monkeypatch.setattr(
        etl_module,
        "s3a_path",
        lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts),
    )

    result = compute_loan_portfolio(spark_session, BUSINESS_RULES, AS_OF_DATE).collect()[0].asDict()

    assert result["loan_count"] == 2
    assert result["active_loan_count"] == 1
    assert result["closed_loan_count"] == 1
    assert result["defaulted_loan_count"] == 0
    assert result["total_funded_principal"] == 3000.0
    # L1: 1000 - 1000 = 0. L2: 2000 - (500 - 500) = 2000. Total = 2000.
    assert result["total_outstanding_principal"] == 2000.0
    assert result["avg_interest_rate"] == pytest.approx(0.075, abs=1e-6)
    # Only L2 (ACTIVE) accrues: 2000 * 0.10 * 365/365 = 200.0. L1 (CLOSED) contributes 0.
    assert result["total_accrued_interest"] == pytest.approx(200.0, abs=0.5)


def test_validate_loan_portfolio_passes_against_correct_curated_output(spark_session, seeded_storage, monkeypatch):
    import src.etl_spark_loan_portfolio as etl_module

    monkeypatch.setattr(
        etl_module,
        "s3a_path",
        lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts),
    )
    summary_df = compute_loan_portfolio(spark_session, BUSINESS_RULES, AS_OF_DATE)
    seeded_storage.write_parquet(f"{TEST_PREFIX}curated/loan_portfolio.parquet", summary_df.toPandas())

    # validate_loan_portfolio reads fixed "raw/..."/"curated/..." keys -- point it at our
    # isolated test prefix (already seeded by the seeded_storage fixture) via a thin wrapper.
    result = validate_loan_portfolio(
        PrefixedStorage(seeded_storage, TEST_PREFIX), BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE
    )
    assert result["overall_status"] == "PASS"
    assert result["failed_check_count"] == 0


def test_validate_loan_portfolio_fails_on_wrong_curated_value(spark_session, seeded_storage, monkeypatch):
    import src.etl_spark_loan_portfolio as etl_module

    monkeypatch.setattr(
        etl_module,
        "s3a_path",
        lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts),
    )
    summary_df = compute_loan_portfolio(spark_session, BUSINESS_RULES, AS_OF_DATE)
    wrong_pandas = summary_df.toPandas()
    wrong_pandas.loc[0, "total_outstanding_principal"] = 999999.0
    seeded_storage.write_parquet(f"{TEST_PREFIX}curated/loan_portfolio.parquet", wrong_pandas)

    result = validate_loan_portfolio(
        PrefixedStorage(seeded_storage, TEST_PREFIX), BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE
    )
    assert result["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["total_outstanding_principal_reconciliation"]["status"] == "FAIL"
    assert checks_by_id["loan_count_reconciliation"]["status"] == "PASS"

    def write_json(self, path: str, value) -> None:
        self._storage.write_json(f"{self._prefix}{path}", value)
