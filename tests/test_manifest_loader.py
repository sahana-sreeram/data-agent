"""Tests for src/manifest_loader.py.

Round-trip fidelity (manifest metadata matches the real PIPELINE_REGISTRY exactly) is pure
and fast. build_generic_pipeline_spec is proven against REAL Spark/S3 for loan_portfolio --
the whole point of the generic path is that it produces a PipelineSpec that actually works
against real code, not just one that satisfies a mock.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.manifest_loader import (
    ManifestError,
    build_generic_pipeline_spec,
    load_all_manifests,
    load_manifest,
    manifest_metadata_matches_registry,
)
from tests.conftest import PrefixedStorage

MANIFESTS = load_all_manifests()


def test_every_registered_pipeline_has_a_manifest():
    assert set(MANIFESTS) == set(PIPELINE_REGISTRY)


@pytest.mark.parametrize("pipeline_name", sorted(PIPELINE_REGISTRY))
def test_manifest_metadata_matches_the_real_registry_exactly(pipeline_name):
    manifest = MANIFESTS[pipeline_name]
    spec = PIPELINE_REGISTRY[pipeline_name]
    mismatches = manifest_metadata_matches_registry(manifest, spec)
    assert mismatches == [], f"{pipeline_name}: {mismatches}"


def test_load_manifest_rejects_missing_required_fields(tmp_path):
    bad_manifest = tmp_path / "bad.yaml"
    bad_manifest.write_text("name: incomplete\n")
    with pytest.raises(ManifestError):
        load_manifest(bad_manifest)


def test_load_manifest_rejects_invalid_yaml(tmp_path):
    bad_manifest = tmp_path / "bad.yaml"
    bad_manifest.write_text("name: [unclosed\n")
    with pytest.raises(ManifestError):
        load_manifest(bad_manifest)


def test_load_all_manifests_keys_by_name(tmp_path):
    (tmp_path / "a.yaml").write_text(
        "name: a\ninputs: []\noutputs: [x]\nruntime: {source_file: a.py, functions: [f]}\nvalidation: {module: m, function: f}\n"
    )
    manifests = load_all_manifests(tmp_path)
    assert set(manifests) == {"a"}
    assert manifests["a"]["outputs"] == ["x"]


def test_build_generic_pipeline_spec_rejects_multi_function_pipelines():
    with pytest.raises(ManifestError):
        build_generic_pipeline_spec(MANIFESTS["underwriting_performance"])


def test_build_generic_pipeline_spec_produces_correct_metadata_for_loan_portfolio():
    spec = build_generic_pipeline_spec(MANIFESTS["loan_portfolio"])
    real_spec = PIPELINE_REGISTRY["loan_portfolio"]
    assert spec.raw_tables == real_spec.raw_tables
    assert spec.curated_keys == real_spec.curated_keys
    assert spec.etl_source_file == real_spec.etl_source_file
    assert spec.validation_rules_key == real_spec.validation_rules_key


# --- proof against real Spark/S3: the generic path produces genuinely working code ----------

TEST_PREFIX = "_test_manifest_loader/"
AS_OF_DATE = "2026-07-20"
LOANS = pd.DataFrame(
    [{"loan_id": "L1", "application_id": "A1", "customer_id": "C1", "principal_amount": 1000.0, "interest_rate": 0.05, "term_months": 12, "originated_at": "2024-01-01", "loan_status": "CLOSED", "scheduled_payment_amount": 83.33}]
)
PAYMENT_EVENTS = pd.DataFrame(
    [{"event_id": "E1", "schedule_id": "S1", "loan_id": "L1", "event_type": "PAYMENT", "payment_date": "2024-02-01", "amount": 1000.0, "payment_status": "PAID", "payment_method": "ACH"}]
)
BUSINESS_RULES = {"successful_payment_statuses": ["PAID"], "interest_accrual": {"day_count_convention": "ACT/365", "accrues_on_statuses": ["ACTIVE"]}}
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


@pytest.fixture
def seeded_storage(s3_storage):
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)
    yield s3_storage
    for key in s3_storage.list_paths(TEST_PREFIX):
        s3_storage._client.delete_object(Bucket=s3_storage.bucket, Key=key)


def test_generic_spec_run_etl_and_run_validate_work_against_real_spark_and_s3(spark_session, seeded_storage):
    """The actual proof point for generalized onboarding: a PipelineSpec built ENTIRELY from
    a manifest -- zero hand-written adapter code -- runs the real compute_loan_portfolio and
    the real validate_loan_portfolio against real Spark/S3 and produces a real, correct
    result. A 6th pipeline following the same (spark, business_rules, as_of_date) /
    (storage, business_rules, validation_rules, as_of_date) convention needs nothing more
    than its own manifest + ETL/validator files to work this way."""
    import src.etl_spark_loan_portfolio as etl_module

    prefixed = PrefixedStorage(seeded_storage, TEST_PREFIX)
    prefixed.write_parquet("raw/loans.parquet", LOANS)
    prefixed.write_parquet("raw/payment_events.parquet", PAYMENT_EVENTS)

    original_s3a_path = etl_module.s3a_path
    etl_module.s3a_path = lambda *parts: f"s3a://{seeded_storage.bucket}/{TEST_PREFIX}" + "/".join(parts)
    try:
        spec = build_generic_pipeline_spec(MANIFESTS["loan_portfolio"])
        outputs = spec.run_etl(etl_module, spark_session, BUSINESS_RULES, AS_OF_DATE)
        for key, df in outputs.items():
            prefixed.write_parquet(key, df)

        result = spec.run_validate(prefixed, BUSINESS_RULES, VALIDATION_RULES, AS_OF_DATE)
    finally:
        etl_module.s3a_path = original_s3a_path

    assert result["overall_status"] == "PASS"
    curated = prefixed.read_parquet("curated/loan_portfolio.parquet")
    assert curated.iloc[0]["loan_count"] == 1
