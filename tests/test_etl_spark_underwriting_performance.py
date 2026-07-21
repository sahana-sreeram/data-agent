"""Tests for the underwriting_performance PySpark ETL + its independent pandas validator."""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl_spark_underwriting_performance import compute_rejection_distribution, compute_underwriting_performance
from src.validate_underwriting_performance import validate_underwriting_performance
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_underwriting_performance/"

CUSTOMERS = pd.DataFrame(
    [
        {"customer_id": "C1", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "680_719", "credit_score": 700, "risk_segment": "LOW"},
        {"customer_id": "C2", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "620_679", "credit_score": 650, "risk_segment": "HIGH"},
        {"customer_id": "C3", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "680_719", "credit_score": 690, "risk_segment": "LOW"},
    ]
)
APPLICATIONS = pd.DataFrame(
    [
        {"application_id": "APP1", "customer_id": "C1", "offer_id": None, "requested_amount": 5000.0, "submitted_at": "2025-01-01", "application_status": "DECISIONED"},
        {"application_id": "APP2", "customer_id": "C2", "offer_id": None, "requested_amount": 4000.0, "submitted_at": "2025-01-01", "application_status": "DECISIONED"},
        {"application_id": "APP3", "customer_id": "C3", "offer_id": None, "requested_amount": 3000.0, "submitted_at": "2025-01-01", "application_status": "DECISIONED"},
    ]
)
# LOW segment: APP1 approved, APP3 approved (2/2 = 100%). HIGH segment: APP2 rejected (0/1 = 0%).
# model_version uw-model-v1: APP1, APP2 (1 approved of 2). uw-model-v2: APP3 (1 approved of 1).
UNDERWRITING_DECISIONS = pd.DataFrame(
    [
        {"decision_id": "DEC1", "application_id": "APP1", "decision": "APPROVED", "rejection_reason": None, "approved_amount": 4800.0, "approved_apr": 0.06, "model_version": "uw-model-v1", "decided_at": "2025-01-02"},
        {"decision_id": "DEC2", "application_id": "APP2", "decision": "REJECTED", "rejection_reason": "LOW_CREDIT_SCORE", "approved_amount": None, "approved_apr": None, "model_version": "uw-model-v1", "decided_at": "2025-01-02"},
        {"decision_id": "DEC3", "application_id": "APP3", "decision": "APPROVED", "rejection_reason": None, "approved_amount": 2900.0, "approved_apr": 0.05, "model_version": "uw-model-v2", "decided_at": "2025-01-02"},
    ]
)

VALIDATION_RULES = {
    "tolerance": {"currency": 0.01, "count": 0, "rate": 0.0001},
    "rules": [
        {"id": "underwriting_performance_breakdown_rows_match", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
        {"id": "underwriting_performance_rejection_distribution_matches", "type": "reconciliation", "tolerance_type": "count", "description": "d"},
    ],
}


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    for name, df in [
        ("customers", CUSTOMERS),
        ("applications", APPLICATIONS),
        ("underwriting_decisions", UNDERWRITING_DECISIONS),
    ]:
        s3_storage.write_parquet(f"{TEST_PREFIX}raw/{name}.parquet", df)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


@pytest.fixture
def patched(seeded_storage, monkeypatch):
    import src.etl_spark_underwriting_performance as etl_module

    monkeypatch.setattr(
        etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts)
    )
    return seeded_storage


def test_risk_segment_breakdown_matches_hand_verified_numbers(spark_session, patched):
    result = compute_underwriting_performance(spark_session).toPandas()
    by_segment = {r["breakdown_value"]: r for _, r in result[result["breakdown_type"] == "risk_segment"].iterrows()}

    assert by_segment["LOW"]["decision_count"] == 2
    assert by_segment["LOW"]["approved_count"] == 2
    assert by_segment["LOW"]["approval_rate"] == 1.0
    assert by_segment["HIGH"]["decision_count"] == 1
    assert by_segment["HIGH"]["rejected_count"] == 1
    assert by_segment["HIGH"]["approval_rate"] == 0.0


def test_model_version_breakdown_matches_hand_verified_numbers(spark_session, patched):
    result = compute_underwriting_performance(spark_session).toPandas()
    by_model = {r["breakdown_value"]: r for _, r in result[result["breakdown_type"] == "model_version"].iterrows()}

    assert by_model["uw-model-v1"]["decision_count"] == 2
    assert by_model["uw-model-v1"]["approved_count"] == 1
    assert by_model["uw-model-v2"]["decision_count"] == 1
    assert by_model["uw-model-v2"]["approved_count"] == 1


def test_rejection_distribution_matches_hand_verified_numbers(spark_session, patched):
    result = compute_rejection_distribution(spark_session).toPandas()
    assert dict(zip(result["rejection_reason"], result["count"])) == {"LOW_CREDIT_SCORE": 1}


def test_validator_passes_against_real_etl_output(spark_session, patched):
    performance_df = compute_underwriting_performance(spark_session)
    rejections_df = compute_rejection_distribution(spark_session)

    wrapped = PrefixedStorage(patched, TEST_PREFIX)
    wrapped.write_parquet("curated/underwriting_performance.parquet", performance_df.toPandas())
    wrapped.write_parquet("curated/underwriting_performance_rejections.parquet", rejections_df.toPandas())

    result = validate_underwriting_performance(wrapped, VALIDATION_RULES)
    assert result["overall_status"] == "PASS"


def test_validator_fails_on_wrong_approval_rate(spark_session, patched):
    performance_df = compute_underwriting_performance(spark_session)
    rejections_df = compute_rejection_distribution(spark_session)

    wrong_pandas = performance_df.toPandas()
    wrong_pandas.loc[wrong_pandas["breakdown_value"] == "LOW", "approval_rate"] = 0.1

    wrapped = PrefixedStorage(patched, TEST_PREFIX)
    wrapped.write_parquet("curated/underwriting_performance.parquet", wrong_pandas)
    wrapped.write_parquet("curated/underwriting_performance_rejections.parquet", rejections_df.toPandas())

    result = validate_underwriting_performance(wrapped, VALIDATION_RULES)
    assert result["overall_status"] == "FAIL"
