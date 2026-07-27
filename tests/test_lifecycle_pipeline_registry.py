"""Tests for the PIPELINE_REGISTRY normalization layer: every pipeline's run_etl/run_validate
closure actually calls the right real function(s) with the right arguments and produces the
same result as calling that pipeline's real ETL/validator module directly. Against real
Spark + S3-compatible storage (skips cleanly if unreachable), using a dedicated test prefix.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from tests.conftest import PrefixedStorage

TEST_PREFIX = "_test_lifecycle_pipeline_registry/"

BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "interest_accrual": {"day_count_convention": "ACT/365", "accrues_on_statuses": ["ACTIVE"]},
    "prepayment_threshold_days": 3,
    "loss_rate_denominator": "total_funded_principal",
}
AS_OF_DATE = "2026-07-20"

CUSTOMERS = pd.DataFrame(
    [{"customer_id": "C1", "created_at": "2024-01-01", "state": "CA", "income_band": "40000_60000", "credit_score_band": "680_719", "credit_score": 700, "risk_segment": "LOW"}]
)
LOANS = pd.DataFrame(
    [{"loan_id": "L1", "application_id": "APP1", "customer_id": "C1", "principal_amount": 1000.0, "interest_rate": 0.05, "term_months": 12, "originated_at": "2024-01-01", "loan_status": "CLOSED", "scheduled_payment_amount": 83.33}]
)
PAYMENT_EVENTS = pd.DataFrame(
    [{"event_id": "E1", "schedule_id": "S1", "loan_id": "L1", "event_type": "PAYMENT", "payment_date": "2024-02-01", "amount": 1000.0, "payment_status": "PAID", "payment_method": "ACH"}]
)
PAYMENT_SCHEDULE = pd.DataFrame(
    [{"schedule_id": "S1", "loan_id": "L1", "installment_number": 1, "due_date": "2024-02-01", "scheduled_amount": 1000.0}]
)
CAMPAIGNS = pd.DataFrame([{"campaign_id": "CMP1", "name": "Spring", "channel": "EMAIL", "start_date": "2025-01-01", "end_date": "2025-02-01", "target_risk_segment": None}])
EMAIL_EVENTS = pd.DataFrame([{"event_id": "EE1", "campaign_id": "CMP1", "customer_id": "C1", "event_type": "SENT", "event_timestamp": "2025-01-05"}])
PREQUAL_OFFERS = pd.DataFrame([{"offer_id": "OFF1", "customer_id": "C1", "campaign_id": "CMP1", "coupon_code": None, "offer_amount": 1000.0, "offer_apr": 0.05, "created_at": "2025-01-05", "expires_at": "2025-02-05"}])
APPLICATIONS = pd.DataFrame([{"application_id": "APP1", "customer_id": "C1", "offer_id": "OFF1", "requested_amount": 1000.0, "submitted_at": "2025-01-06", "application_status": "DECISIONED"}])
UNDERWRITING_DECISIONS = pd.DataFrame([{"decision_id": "DEC1", "application_id": "APP1", "decision": "APPROVED", "rejection_reason": None, "approved_amount": 1000.0, "approved_apr": 0.05, "model_version": "uw-v1", "decided_at": "2025-01-07"}])
DELINQUENCY_EVENTS = pd.DataFrame(columns=["delinquency_id", "loan_id", "as_of_date", "days_past_due", "bucket"])
DEFAULTS = pd.DataFrame(columns=["default_id", "loan_id", "default_date", "balance_at_default", "recovery_amount", "recovery_date"])
COUPON_RULES = pd.DataFrame([{"coupon_rule_id": "CPN1", "coupon_code": "TEST10", "campaign_id": "CMP1", "discount_type": "PERCENT", "discount_value": 10.0, "valid_from": "2025-01-01", "valid_to": "2025-12-31"}])

RAW_TABLES = {
    "customers": CUSTOMERS, "loans": LOANS, "payment_events": PAYMENT_EVENTS,
    "payment_schedule": PAYMENT_SCHEDULE, "campaigns": CAMPAIGNS, "email_events": EMAIL_EVENTS,
    "prequal_offers": PREQUAL_OFFERS, "applications": APPLICATIONS,
    "underwriting_decisions": UNDERWRITING_DECISIONS, "delinquency_events": DELINQUENCY_EVENTS,
    "defaults": DEFAULTS, "coupon_rules": COUPON_RULES,
}


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    for table_name, df in RAW_TABLES.items():
        s3_storage.write_parquet(f"{TEST_PREFIX}raw/{table_name}.parquet", df)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


@pytest.mark.parametrize("pipeline_name", list(PIPELINE_REGISTRY))
def test_run_etl_produces_every_declared_curated_key(pipeline_name, spark_session, seeded_storage, monkeypatch):
    spec = PIPELINE_REGISTRY[pipeline_name]
    import importlib

    etl_module = importlib.import_module(spec.etl_source_file.replace("/", ".").replace(".py", ""))
    monkeypatch.setattr(
        etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts)
    )

    result = spec.run_etl(etl_module, spark_session, BUSINESS_RULES, AS_OF_DATE)

    assert set(result) == set(spec.curated_keys)
    for curated_key, df in result.items():
        assert isinstance(df, pd.DataFrame)
    # At least one curated key should have real rows (the seed data has zero REJECTED
    # decisions, so underwriting_performance's rejection-distribution output is legitimately
    # empty -- an empty DataFrame there is correct, not a bug).
    assert any(len(df) >= 1 for df in result.values())


@pytest.mark.parametrize("pipeline_name", list(PIPELINE_REGISTRY))
def test_run_validate_matches_calling_the_real_validator_directly(pipeline_name, spark_session, seeded_storage, monkeypatch):
    spec = PIPELINE_REGISTRY[pipeline_name]
    import importlib

    etl_module = importlib.import_module(spec.etl_source_file.replace("/", ".").replace(".py", ""))
    monkeypatch.setattr(
        etl_module, "s3a_path", lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts)
    )
    curated = spec.run_etl(etl_module, spark_session, BUSINESS_RULES, AS_OF_DATE)

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    for curated_key, df in curated.items():
        prefixed.write_parquet(curated_key, df)

    # Confirm run_validate dispatches to the real validator with the right positional data
    # (storage/business_rules/validation_rules/as_of_date, ignoring what it doesn't need) by
    # comparing against calling that pipeline's real validate_*.py function directly --
    # exact rule-level correctness is already covered by each pipeline's own
    # tests/test_etl_spark_*.py; this only proves the normalization wiring is correct.
    real_validate = _real_validate_for(pipeline_name)
    validation_rules = _real_validation_rules_for(pipeline_name)
    expected = real_validate(prefixed, BUSINESS_RULES, validation_rules, AS_OF_DATE)
    actual = spec.run_validate(prefixed, BUSINESS_RULES, validation_rules, AS_OF_DATE)
    assert actual == expected


def _real_validate_for(pipeline_name: str):
    if pipeline_name == "loan_portfolio":
        from src.validate_loan_portfolio import validate_loan_portfolio as fn
        return lambda storage, br, vr, as_of: fn(storage, br, vr, as_of)
    if pipeline_name == "campaign_funnel":
        from src.validate_campaign_funnel import validate_campaign_funnel as fn
        return lambda storage, br, vr, as_of: fn(storage, vr)
    if pipeline_name == "underwriting_performance":
        from src.validate_underwriting_performance import validate_underwriting_performance as fn
        return lambda storage, br, vr, as_of: fn(storage, vr)
    if pipeline_name == "payment_performance":
        from src.validate_payment_performance import validate_payment_performance as fn
        return lambda storage, br, vr, as_of: fn(storage, br, vr, as_of)
    if pipeline_name == "delinquency_default":
        from src.validate_delinquency_default import validate_delinquency_default as fn
        return lambda storage, br, vr, as_of: fn(storage, br, vr)
    if pipeline_name == "coupon_performance":
        from src.validate_coupon_performance import validate_coupon_performance as fn
        return lambda storage, br, vr, as_of: fn(storage, br, vr, as_of)
    raise AssertionError(f"no direct-call wrapper for {pipeline_name!r}")


def _real_validation_rules_for(pipeline_name: str) -> dict:
    tolerance = {"currency": 0.01, "count": 0, "rate": 0.0001}
    rule_ids_by_pipeline = {
        "loan_portfolio": [
            "loan_count_reconciliation", "active_loan_count_reconciliation", "closed_loan_count_reconciliation",
            "defaulted_loan_count_reconciliation", "total_funded_principal_reconciliation",
            "total_outstanding_principal_reconciliation", "avg_interest_rate_reconciliation",
            "total_accrued_interest_reconciliation",
        ],
        "campaign_funnel": [
            f"{name}_reconciliation" for name in [
                "emails_sent", "emails_opened", "emails_clicked", "offers_created",
                "applications_submitted", "applications_approved", "loans_funded",
            ]
        ] + ["campaign_funnel_row_counts_match_per_campaign"],
        "underwriting_performance": [
            "underwriting_performance_breakdown_rows_match", "underwriting_performance_rejection_distribution_matches",
        ],
        "payment_performance": [
            f"{name}_reconciliation" for name in [
                "expected_payment_count", "expected_amount_due", "successful_payment_count", "total_collected_amount",
                "missed_payment_count", "missed_amount", "late_payment_count", "failed_payment_count",
                "collection_rate", "prepayment_rate",
            ]
        ],
        "delinquency_default": ["delinquency_default_breakdown_rows_match"],
        "coupon_performance": [
            f"{name}_reconciliation" for name in [
                "coupon_rule_count", "currently_valid_rule_count", "offers_created",
                "applications_submitted", "loans_funded",
            ]
        ] + ["coupon_performance_row_counts_match_per_code"],
    }
    return {
        "tolerance": tolerance,
        "rules": [
            {"id": rule_id, "type": "reconciliation", "tolerance_type": "count", "description": "d"}
            for rule_id in rule_ids_by_pipeline[pipeline_name]
        ],
    }
