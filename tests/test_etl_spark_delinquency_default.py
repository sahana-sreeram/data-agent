"""Tests for the delinquency_default PySpark ETL + its independent pandas validator."""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl_spark_delinquency_default import compute_delinquency_default
from src.validate_delinquency_default import validate_delinquency_default
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_delinquency_default/"

BUSINESS_RULES = {"loss_rate_denominator": "total_funded_principal"}

CUSTOMERS = pd.DataFrame(
    [
        {"customer_id": "C1", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "680_719", "credit_score": 700, "risk_segment": "LOW"},
        {"customer_id": "C2", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "620_679", "credit_score": 650, "risk_segment": "HIGH"},
    ]
)
# L1 (LOW, C1): no delinquency, no default. L2 (HIGH, C2): delinquent AND defaulted.
LOANS = pd.DataFrame(
    [
        {"loan_id": "L1", "application_id": "APP1", "customer_id": "C1", "principal_amount": 1000.0, "interest_rate": 0.05, "term_months": 12, "originated_at": "2025-01-01", "loan_status": "ACTIVE", "scheduled_payment_amount": 83.33},
        {"loan_id": "L2", "application_id": "APP2", "customer_id": "C2", "principal_amount": 2000.0, "interest_rate": 0.15, "term_months": 12, "originated_at": "2025-01-01", "loan_status": "DEFAULTED", "scheduled_payment_amount": 166.67},
    ]
)
DELINQUENCY_EVENTS = pd.DataFrame(
    [{"delinquency_id": "DLQ1", "loan_id": "L2", "as_of_date": "2026-07-20", "days_past_due": 45, "bucket": "60"}]
)
DEFAULTS = pd.DataFrame(
    [{"default_id": "DEF1", "loan_id": "L2", "default_date": "2026-06-01", "balance_at_default": 1500.0, "recovery_amount": 300.0, "recovery_date": "2026-07-01"}]
)

VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0, "rate": 0.0001},
    "rules": [{"id": "delinquency_default_breakdown_rows_match", "type": "reconciliation", "tolerance_type": "count", "description": "d"}],
}


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    for name, df in [
        ("customers", CUSTOMERS), ("loans", LOANS),
        ("delinquency_events", DELINQUENCY_EVENTS), ("defaults", DEFAULTS),
    ]:
        s3_storage.write_parquet(f"{TEST_PREFIX}raw/{name}.parquet", df)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


@pytest.fixture
def patched(seeded_storage, monkeypatch):
    import src.etl_spark_delinquency_default as etl_module

    monkeypatch.setattr(
        etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts)
    )
    return seeded_storage


def test_overall_and_per_segment_rows_match_hand_verified_numbers(spark_session, patched):
    result = compute_delinquency_default(spark_session, BUSINESS_RULES).toPandas()
    by_value = {r["breakdown_value"]: r for _, r in result.iterrows()}

    overall = by_value["ALL"]
    assert overall["loan_count"] == 2
    assert overall["delinquent_loan_count"] == 1
    assert overall["default_count"] == 1
    assert overall["total_balance_at_default"] == 1500.0
    assert overall["total_recovery_amount"] == 300.0
    assert overall["recovery_rate"] == pytest.approx(0.2, abs=1e-4)
    # loss_rate = (1500 - 300) / total_funded_principal(3000) = 0.4
    assert overall["loss_rate"] == pytest.approx(0.4, abs=1e-4)

    low = by_value["LOW"]
    assert low["loan_count"] == 1
    assert low["delinquent_loan_count"] == 0
    assert low["default_count"] == 0
    assert pd.isna(low["recovery_rate"])

    high = by_value["HIGH"]
    assert high["loan_count"] == 1
    assert high["delinquent_loan_count"] == 1
    assert high["default_rate"] == 1.0


def test_validator_passes_against_real_etl_output(spark_session, patched):
    result_df = compute_delinquency_default(spark_session, BUSINESS_RULES)
    wrapped = PrefixedStorage(patched, TEST_PREFIX)
    wrapped.write_parquet("curated/delinquency_default.parquet", result_df.toPandas())

    validation = validate_delinquency_default(wrapped, BUSINESS_RULES, VALIDATION_RULES)
    assert validation["overall_status"] == "PASS"


def test_validator_fails_on_wrong_loss_rate(spark_session, patched):
    result_df = compute_delinquency_default(spark_session, BUSINESS_RULES)
    wrong = result_df.toPandas()
    wrong.loc[wrong["breakdown_value"] == "ALL", "loss_rate"] = 0.99

    wrapped = PrefixedStorage(patched, TEST_PREFIX)
    wrapped.write_parquet("curated/delinquency_default.parquet", wrong)

    validation = validate_delinquency_default(wrapped, BUSINESS_RULES, VALIDATION_RULES)
    assert validation["overall_status"] == "FAIL"
