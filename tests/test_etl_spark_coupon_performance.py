"""Tests for the coupon_performance PySpark ETL + its independent pandas validator.

Against a REAL local Spark session and S3-compatible endpoint (MinIO), using a dedicated test
prefix so these tests never collide with the real migrated raw/curated data. Skips cleanly
(via the shared conftest.py fixtures) if Spark/S3 aren't reachable in this environment.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl_spark_coupon_performance import compute_coupon_performance
from src.validate_coupon_performance import validate_coupon_performance
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_coupon_performance/"
AS_OF_DATE = "2026-07-20"

BUSINESS_RULES = {}

VALIDATION_RULES = {
    "tolerance": {"count": 0, "rate": 0.0001},
    "rules": [
        {"id": f"{metric}_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"}
        for metric in ["coupon_rule_count", "currently_valid_rule_count", "offers_created", "applications_submitted", "loans_funded"]
    ]
    + [{"id": "coupon_performance_row_counts_match_per_code", "type": "reconciliation", "tolerance_type": "count", "description": "d"}],
}

# SAVE10 is reused by two coupon_rules (CPN001, still valid at AS_OF_DATE; CPN002, expired) --
# the genuine coupon_code-reuse ambiguity this pipeline's design docstring calls out.
COUPON_RULES = pd.DataFrame(
    [
        {"coupon_rule_id": "CPN001", "coupon_code": "SAVE10", "campaign_id": "C1", "discount_type": "PERCENT", "discount_value": 10.0, "valid_from": "2026-01-01", "valid_to": "2026-12-31"},
        {"coupon_rule_id": "CPN002", "coupon_code": "SAVE10", "campaign_id": "C2", "discount_type": "PERCENT", "discount_value": 10.0, "valid_from": "2025-01-01", "valid_to": "2025-06-01"},
        {"coupon_rule_id": "CPN003", "coupon_code": "WELCOME5", "campaign_id": "C1", "discount_type": "FLAT", "discount_value": 5.0, "valid_from": "2026-01-01", "valid_to": "2026-12-31"},
    ]
)

# O5 uses SAVE10 but is created AFTER as_of_date -- must be excluded (point-in-time snapshot).
# O4 has no coupon_code at all -- must never count toward any code.
PREQUAL_OFFERS = pd.DataFrame(
    [
        {"offer_id": "O1", "customer_id": "CUST1", "campaign_id": "camp1", "coupon_code": "SAVE10", "offer_amount": 1000.0, "offer_apr": 0.1, "created_at": "2026-02-01T00:00:00", "expires_at": "2026-03-01T00:00:00"},
        {"offer_id": "O2", "customer_id": "CUST2", "campaign_id": "camp1", "coupon_code": "SAVE10", "offer_amount": 1000.0, "offer_apr": 0.1, "created_at": "2026-03-01T00:00:00", "expires_at": "2026-04-01T00:00:00"},
        {"offer_id": "O3", "customer_id": "CUST3", "campaign_id": "camp1", "coupon_code": "WELCOME5", "offer_amount": 500.0, "offer_apr": 0.12, "created_at": "2026-04-01T00:00:00", "expires_at": "2026-05-01T00:00:00"},
        {"offer_id": "O4", "customer_id": "CUST4", "campaign_id": "camp1", "coupon_code": None, "offer_amount": 800.0, "offer_apr": 0.15, "created_at": "2026-04-01T00:00:00", "expires_at": "2026-05-01T00:00:00"},
        {"offer_id": "O5", "customer_id": "CUST5", "campaign_id": "camp1", "coupon_code": "SAVE10", "offer_amount": 1000.0, "offer_apr": 0.1, "created_at": "2026-08-01T00:00:00", "expires_at": "2026-09-01T00:00:00"},
    ]
)

# A4 has no offer_id at all (organic application) -- must never count toward any code.
APPLICATIONS = pd.DataFrame(
    [
        {"application_id": "A1", "customer_id": "CUST1", "offer_id": "O1", "requested_amount": 1000.0, "submitted_at": "2026-02-05T00:00:00", "application_status": "APPROVED"},
        {"application_id": "A2", "customer_id": "CUST2", "offer_id": "O2", "requested_amount": 1000.0, "submitted_at": "2026-03-05T00:00:00", "application_status": "APPROVED"},
        {"application_id": "A3", "customer_id": "CUST3", "offer_id": "O3", "requested_amount": 500.0, "submitted_at": "2026-04-05T00:00:00", "application_status": "REJECTED"},
        {"application_id": "A4", "customer_id": "CUST4", "offer_id": None, "requested_amount": 800.0, "submitted_at": "2026-04-05T00:00:00", "application_status": "APPROVED"},
    ]
)

# Only A1 (SAVE10) funded -- A2 (also SAVE10) and A3 (WELCOME5) did not.
LOANS = pd.DataFrame(
    [
        {"loan_id": "L1", "application_id": "A1", "customer_id": "CUST1", "principal_amount": 1000.0, "interest_rate": 0.1, "term_months": 12, "originated_at": "2026-02-10", "loan_status": "ACTIVE", "scheduled_payment_amount": 90.0},
    ]
)


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/coupon_rules.parquet", COUPON_RULES)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/prequal_offers.parquet", PREQUAL_OFFERS)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/applications.parquet", APPLICATIONS)
    s3_storage.write_parquet(f"{TEST_PREFIX}raw/loans.parquet", LOANS)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


def test_compute_coupon_performance_matches_hand_verified_numbers(spark_session, seeded_storage, monkeypatch):
    import src.etl_spark_coupon_performance as etl_module

    monkeypatch.setattr(etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts))

    result = compute_coupon_performance(spark_session, BUSINESS_RULES, AS_OF_DATE).toPandas().set_index("coupon_code")

    save10 = result.loc["SAVE10"]
    assert int(save10["coupon_rule_count"]) == 2  # CPN001 + CPN002 both use SAVE10
    assert int(save10["currently_valid_rule_count"]) == 1  # only CPN001 is valid at AS_OF_DATE
    assert int(save10["offers_created"]) == 2  # O1, O2 -- O5 excluded (created after as_of_date)
    assert int(save10["applications_submitted"]) == 2  # A1, A2
    assert int(save10["loans_funded"]) == 1  # L1 (via A1) -- A2 never funded
    assert save10["redemption_rate"] == pytest.approx(0.5)

    welcome5 = result.loc["WELCOME5"]
    assert int(welcome5["coupon_rule_count"]) == 1
    assert int(welcome5["currently_valid_rule_count"]) == 1
    assert int(welcome5["offers_created"]) == 1
    assert int(welcome5["applications_submitted"]) == 1
    assert int(welcome5["loans_funded"]) == 0
    assert welcome5["redemption_rate"] == pytest.approx(0.0)

    assert len(result) == 2  # exactly the coupon_rules catalog -- no phantom or missing codes


def test_validate_coupon_performance_passes_against_correct_curated_output(spark_session, seeded_storage, monkeypatch):
    import src.etl_spark_coupon_performance as etl_module

    monkeypatch.setattr(etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts))
    result_df = compute_coupon_performance(spark_session, BUSINESS_RULES, AS_OF_DATE)
    seeded_storage.write_parquet(f"{TEST_PREFIX}curated/coupon_performance.parquet", result_df.toPandas())

    result = validate_coupon_performance(
        PrefixedStorage(seeded_storage, TEST_PREFIX), BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE
    )
    assert result["overall_status"] == "PASS"
    assert result["failed_check_count"] == 0


def test_validate_coupon_performance_fails_on_wrong_curated_value(spark_session, seeded_storage, monkeypatch):
    import src.etl_spark_coupon_performance as etl_module

    monkeypatch.setattr(etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts))
    result_df = compute_coupon_performance(spark_session, BUSINESS_RULES, AS_OF_DATE)
    wrong_pandas = result_df.toPandas()
    wrong_pandas.loc[wrong_pandas["coupon_code"] == "SAVE10", "loans_funded"] = 999
    seeded_storage.write_parquet(f"{TEST_PREFIX}curated/coupon_performance.parquet", wrong_pandas)

    result = validate_coupon_performance(
        PrefixedStorage(seeded_storage, TEST_PREFIX), BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE
    )
    assert result["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["loans_funded_reconciliation"]["status"] == "FAIL"
    assert checks_by_id["coupon_performance_row_counts_match_per_code"]["status"] == "FAIL"
    assert checks_by_id["offers_created_reconciliation"]["status"] == "PASS"
