"""Tests for src/migrate_lifecycle_to_s3.py against the REAL local data/lifecycle/raw/
dataset and a REAL local S3-compatible endpoint (MinIO). Skips cleanly if either isn't
available in this environment.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.migrate_lifecycle_to_s3 import CONTEXT_FILES, migrate_context, migrate_lifecycle_tables
from src.storage import S3Storage
from src.validate_lifecycle_raw import TABLE_FILENAMES, load_lifecycle_tables

RAW_DIR = Path("data/lifecycle/raw")


def _skip_unless_local_dataset_present():
    if not all((RAW_DIR / filename).exists() for filename in TABLE_FILENAMES.values()):
        pytest.skip("data/lifecycle/raw/ not generated in this environment")


@pytest.fixture
def storage():
    _skip_unless_local_dataset_present()
    try:
        s = S3Storage()
        s.create_bucket_if_missing()
        s._client.list_buckets()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"S3-compatible storage not reachable: {exc}")
    return s


def test_migrated_row_counts_match_local_json_exactly(storage):
    local_tables = load_lifecycle_tables(RAW_DIR)
    row_counts = migrate_lifecycle_tables(storage, RAW_DIR)

    assert set(row_counts) == set(TABLE_FILENAMES)
    for table_name, expected_count in row_counts.items():
        assert expected_count == len(local_tables[table_name])
        assert storage.exists(f"raw/{table_name}.parquet")


def test_migrated_data_preserves_foreign_key_integrity(storage):
    migrate_lifecycle_tables(storage, RAW_DIR)

    loans = storage.read_parquet("raw/loans.parquet")
    applications = storage.read_parquet("raw/applications.parquet")
    customers = storage.read_parquet("raw/customers.parquet")

    application_ids = set(applications["application_id"])
    customer_ids = set(customers["customer_id"])
    assert loans["application_id"].isin(application_ids).all()
    assert loans["customer_id"].isin(customer_ids).all()


def test_migrated_nullable_columns_are_null_not_nan_string(storage):
    migrate_lifecycle_tables(storage, RAW_DIR)

    offers = storage.read_parquet("raw/prequal_offers.parquet")
    organic = offers[offers["campaign_id"].isna()]
    assert len(organic) > 0
    assert str(offers["campaign_id"].dtype) == "string"


def test_migrate_context_uploads_all_four_files(storage):
    uploaded = migrate_context(storage)
    assert set(uploaded) == set(CONTEXT_FILES.values())
    for key in uploaded:
        assert storage.exists(key)

    business_rules = storage.read_json("context/business_rules.json")
    assert "valid_channels" in business_rules
