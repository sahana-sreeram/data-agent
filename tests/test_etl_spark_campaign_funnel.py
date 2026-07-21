"""Tests for the campaign_funnel PySpark ETL + its independent pandas validator.

Includes a fixture specifically constructed to catch the NULL-join bug found while
building this pipeline: Spark's equi-join treats NULL != NULL, so a naive
`.join(other, on="campaign_id")` silently drops the organic (campaign_id=NULL) row's
data. The fixture below has both a real campaign AND organic (no-campaign) activity,
so a regression of that bug would show up as a wrong organic-row count, not just a
missing row.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl_spark_campaign_funnel import compute_campaign_funnel
from src.validate_campaign_funnel import validate_campaign_funnel
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_campaign_funnel/"

CAMPAIGNS = pd.DataFrame([{"campaign_id": "CMP1", "name": "Spring", "channel": "EMAIL", "start_date": "2025-01-01", "end_date": "2025-02-01", "target_risk_segment": None}])
EMAIL_EVENTS = pd.DataFrame(
    [
        {"event_id": "E1", "campaign_id": "CMP1", "customer_id": "C1", "event_type": "SENT", "event_timestamp": "2025-01-05"},
        {"event_id": "E2", "campaign_id": "CMP1", "customer_id": "C1", "event_type": "OPENED", "event_timestamp": "2025-01-06"},
        {"event_id": "E3", "campaign_id": "CMP1", "customer_id": "C1", "event_type": "CLICKED", "event_timestamp": "2025-01-07"},
    ]
)
# OFF1 is campaign-driven (CMP1); OFF2 is organic (campaign_id None).
PREQUAL_OFFERS = pd.DataFrame(
    [
        {"offer_id": "OFF1", "customer_id": "C1", "campaign_id": "CMP1", "coupon_code": None, "offer_amount": 5000.0, "offer_apr": 0.08, "created_at": "2025-01-08", "expires_at": "2025-02-08"},
        {"offer_id": "OFF2", "customer_id": "C2", "campaign_id": None, "coupon_code": None, "offer_amount": 3000.0, "offer_apr": 0.09, "created_at": "2025-01-08", "expires_at": "2025-02-08"},
    ]
)
# APP1 -> OFF1 (campaign-attributed). APP2 -> OFF2 (organic offer). APP3 -> no offer at all (organic).
APPLICATIONS = pd.DataFrame(
    [
        {"application_id": "APP1", "customer_id": "C1", "offer_id": "OFF1", "requested_amount": 5000.0, "submitted_at": "2025-01-09", "application_status": "DECISIONED"},
        {"application_id": "APP2", "customer_id": "C2", "offer_id": "OFF2", "requested_amount": 3000.0, "submitted_at": "2025-01-09", "application_status": "DECISIONED"},
        {"application_id": "APP3", "customer_id": "C3", "offer_id": None, "requested_amount": 2000.0, "submitted_at": "2025-01-09", "application_status": "DECISIONED"},
    ]
)
UNDERWRITING_DECISIONS = pd.DataFrame(
    [
        {"decision_id": "DEC1", "application_id": "APP1", "decision": "APPROVED", "rejection_reason": None, "approved_amount": 4800.0, "approved_apr": 0.08, "model_version": "uw-model-v2", "decided_at": "2025-01-10"},
        {"decision_id": "DEC2", "application_id": "APP2", "decision": "APPROVED", "rejection_reason": None, "approved_amount": 2900.0, "approved_apr": 0.09, "model_version": "uw-model-v2", "decided_at": "2025-01-10"},
        {"decision_id": "DEC3", "application_id": "APP3", "decision": "APPROVED", "rejection_reason": None, "approved_amount": 1900.0, "approved_apr": 0.10, "model_version": "uw-model-v2", "decided_at": "2025-01-10"},
    ]
)
# L1 funds from APP1 (campaign CMP1). L2 and L3 fund from APP2/APP3 (both organic).
LOANS = pd.DataFrame(
    [
        {"loan_id": "L1", "application_id": "APP1", "customer_id": "C1", "principal_amount": 4800.0, "interest_rate": 0.08, "term_months": 12, "originated_at": "2025-01-12", "loan_status": "ACTIVE", "scheduled_payment_amount": 400.0},
        {"loan_id": "L2", "application_id": "APP2", "customer_id": "C2", "principal_amount": 2900.0, "interest_rate": 0.09, "term_months": 12, "originated_at": "2025-01-12", "loan_status": "ACTIVE", "scheduled_payment_amount": 241.67},
        {"loan_id": "L3", "application_id": "APP3", "customer_id": "C3", "principal_amount": 1900.0, "interest_rate": 0.10, "term_months": 12, "originated_at": "2025-01-12", "loan_status": "ACTIVE", "scheduled_payment_amount": 158.33},
    ]
)

VALIDATION_RULES = {
    "tolerance": {"count": 0, "rate": 0.0001},
    "rules": [
        {"id": f"{name}_reconciliation", "type": "reconciliation", "tolerance_type": "count", "description": "d"}
        for name in [
            "emails_sent", "emails_opened", "emails_clicked", "offers_created",
            "applications_submitted", "applications_approved", "loans_funded",
        ]
    ]
    + [{"id": "campaign_funnel_row_counts_match_per_campaign", "type": "reconciliation", "tolerance_type": "count", "description": "d"}],
}


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    tables = {
        "campaigns": CAMPAIGNS,
        "email_events": EMAIL_EVENTS,
        "prequal_offers": PREQUAL_OFFERS,
        "applications": APPLICATIONS,
        "underwriting_decisions": UNDERWRITING_DECISIONS,
        "loans": LOANS,
    }
    for name, df in tables.items():
        s3_storage.write_parquet(f"{TEST_PREFIX}raw/{name}.parquet", df)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


@pytest.fixture
def patched_s3a_path(seeded_storage, monkeypatch):
    import src.etl_spark_campaign_funnel as etl_module

    monkeypatch.setattr(
        etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts)
    )
    return seeded_storage


def test_organic_row_correctly_attributes_both_kinds_of_organic_activity(spark_session, patched_s3a_path):
    result = compute_campaign_funnel(spark_session).toPandas()
    organic = result[result["campaign_id"].isna()].iloc[0]

    # Organic: OFF2 (organic offer) + APP3's no-offer application both attribute here.
    assert organic["offers_created"] == 1
    assert organic["applications_submitted"] == 2
    assert organic["applications_approved"] == 2
    assert organic["loans_funded"] == 2
    # Organic never has email activity -- email_events.campaign_id is never null.
    assert organic["emails_sent"] == 0


def test_campaign_row_only_counts_its_own_attributed_activity(spark_session, patched_s3a_path):
    result = compute_campaign_funnel(spark_session).toPandas()
    cmp1 = result[result["campaign_id"] == "CMP1"].iloc[0]

    assert cmp1["emails_sent"] == 1
    assert cmp1["emails_opened"] == 1
    assert cmp1["emails_clicked"] == 1
    assert cmp1["offers_created"] == 1
    assert cmp1["applications_submitted"] == 1
    assert cmp1["applications_approved"] == 1
    assert cmp1["loans_funded"] == 1
    assert cmp1["open_rate"] == 1.0


def test_totals_across_all_rows_match_raw_counts_exactly(spark_session, patched_s3a_path):
    result = compute_campaign_funnel(spark_session).toPandas()
    assert result["offers_created"].sum() == len(PREQUAL_OFFERS)
    assert result["applications_submitted"].sum() == len(APPLICATIONS)
    assert result["loans_funded"].sum() == len(LOANS)


def test_validator_passes_against_the_real_etl_output(spark_session, patched_s3a_path):
    # Deliberately does NOT call the real write_curated() -- that always writes to the
    # fixed "curated/campaign_funnel.parquet" key on the real bucket namespace, which would
    # clobber real migrated data. Write through the prefixed wrapper instead, so this test
    # stays entirely inside _test_campaign_funnel/.
    result_df = compute_campaign_funnel(spark_session)
    wrapped = PrefixedStorage(patched_s3a_path, TEST_PREFIX)
    wrapped.write_parquet("curated/campaign_funnel.parquet", result_df.toPandas())

    validation = validate_campaign_funnel(wrapped, VALIDATION_RULES)
    assert validation["overall_status"] == "PASS"


def test_validator_catches_a_wrong_rate_even_when_all_counts_are_correct(spark_session, patched_s3a_path):
    # Regression test: a bug in the rate FORMULA itself (e.g. a swapped numerator/
    # denominator) leaves every count untouched -- only a rate-specific check catches it.
    result_df = compute_campaign_funnel(spark_session)
    wrong_pandas = result_df.toPandas()
    cmp1_index = wrong_pandas.index[wrong_pandas["campaign_id"] == "CMP1"][0]
    wrong_pandas.loc[cmp1_index, "open_rate"] = 0.01  # CMP1's real open_rate is 1.0

    wrapped = PrefixedStorage(patched_s3a_path, TEST_PREFIX)
    wrapped.write_parquet("curated/campaign_funnel.parquet", wrong_pandas)

    validation = validate_campaign_funnel(wrapped, VALIDATION_RULES)
    assert validation["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in validation["checks"]}
    assert checks_by_id["campaign_funnel_row_counts_match_per_campaign"]["status"] == "FAIL"
    # The count-only checks must still all PASS -- proving this bug is invisible to them.
    for check_id in ("emails_sent_reconciliation", "emails_opened_reconciliation"):
        assert checks_by_id[check_id]["status"] == "PASS"
