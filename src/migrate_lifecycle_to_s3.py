"""One-shot migration: local data/lifecycle/raw/*.json -> s3://<bucket>/raw/*.parquet,
plus the context layer (business rules, data dictionary, lineage, lifecycle validation
rules) -> s3://<bucket>/context/*.json.

Reuses src.validate_lifecycle_raw's table loader rather than reimplementing JSON loading,
and its TABLE_FILENAMES as the single source of truth for which 12 tables exist.

Local data/lifecycle/raw/*.json remains the generation source of truth (produced by
src/generate_data.py) -- this script only migrates a snapshot of it to S3; it does not
delete or modify the local files, and does not touch data/raw/ or the 3 existing scenarios.

Nullable ID-like string columns (e.g. a null campaign_id on an organic offer) are cast to
pandas' nullable "string" dtype before writing Parquet, so a None round-trips as a real null
rather than risking pyarrow inferring an unexpected type for an all-null or mixed column.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.storage import S3Storage
from src.validate_lifecycle_raw import TABLE_FILENAMES, load_lifecycle_tables

DEFAULT_RAW_DIR = "data/lifecycle/raw"
S3_RAW_PREFIX = "raw"
S3_CONTEXT_PREFIX = "context"

# Columns that can legitimately be null and hold IDs/dates/strings -- cast to pandas'
# nullable "string" dtype so a null survives the Parquet round trip as a real null.
NULLABLE_STRING_COLUMNS_BY_TABLE: dict[str, list[str]] = {
    "campaigns": ["target_risk_segment"],
    "prequal_offers": ["campaign_id", "coupon_code"],
    "applications": ["offer_id"],
    "underwriting_decisions": ["rejection_reason"],
    "payment_events": ["schedule_id", "payment_date"],
    "defaults": ["recovery_date"],
}

CONTEXT_FILES = {
    "business_rules.json": "context/business_rules.json",
    "business_rules_demo.json": "context/business_rules_demo.json",
    "pipeline_rules/loan_portfolio.json": "context/pipeline_rules/loan_portfolio.json",
    "data_dictionary.json": "context/data_dictionary.json",
    "lineage.json": "context/lineage.json",
    "validations/lifecycle_raw.json": "context/validations/lifecycle_raw.json",
    "validations/loan_portfolio.json": "context/validations/loan_portfolio.json",
    "metrics/loan_portfolio.json": "context/metrics/loan_portfolio.json",
    "validations/campaign_funnel.json": "context/validations/campaign_funnel.json",
    "metrics/campaign_funnel.json": "context/metrics/campaign_funnel.json",
    "validations/underwriting_performance.json": "context/validations/underwriting_performance.json",
    "metrics/underwriting_performance.json": "context/metrics/underwriting_performance.json",
    "validations/payment_performance.json": "context/validations/payment_performance.json",
    "metrics/payment_performance.json": "context/metrics/payment_performance.json",
    "validations/delinquency_default.json": "context/validations/delinquency_default.json",
    "metrics/delinquency_default.json": "context/metrics/delinquency_default.json",
    "validations/coupon_performance.json": "context/validations/coupon_performance.json",
    "metrics/coupon_performance.json": "context/metrics/coupon_performance.json",
}


def _prepare_for_parquet(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Cast this table's known-nullable string columns to pandas' nullable string dtype."""
    df = df.copy()
    for column in NULLABLE_STRING_COLUMNS_BY_TABLE.get(table_name, []):
        if column in df.columns:
            df[column] = df[column].astype("string")
    return df


def migrate_lifecycle_tables(storage: S3Storage, raw_dir: Path) -> dict[str, int]:
    """Migrate all 12 lifecycle raw tables to s3://<bucket>/raw/<table>.parquet.

    Returns {table_name: row_count} for the caller to report/verify.
    """
    tables = load_lifecycle_tables(raw_dir)
    row_counts: dict[str, int] = {}
    for table_name in TABLE_FILENAMES:
        df = _prepare_for_parquet(table_name, tables[table_name])
        storage.write_parquet(f"{S3_RAW_PREFIX}/{table_name}.parquet", df)
        row_counts[table_name] = len(df)
    return row_counts


def migrate_context(storage: S3Storage) -> list[str]:
    """Migrate the context layer files to s3://<bucket>/context/*.json. Returns uploaded keys."""
    uploaded = []
    for local_relative_path, s3_key in CONTEXT_FILES.items():
        local_path = Path("context") / local_relative_path
        with local_path.open("r", encoding="utf-8") as f:
            value = json.load(f)
        storage.write_json(s3_key, value)
        uploaded.append(s3_key)
    return uploaded


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate local lifecycle raw data + context to S3-compatible storage.")
    parser.add_argument("--raw-dir", type=str, default=DEFAULT_RAW_DIR)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()
    created = storage.create_bucket_if_missing()

    print(f"Bucket: {storage.bucket} ({'created' if created else 'already existed'})")

    row_counts = migrate_lifecycle_tables(storage, Path(args.raw_dir))
    print("Migrated raw tables:")
    for table_name, row_count in row_counts.items():
        print(f"  s3://{storage.bucket}/{S3_RAW_PREFIX}/{table_name}.parquet  ({row_count} rows)")

    uploaded_context = migrate_context(storage)
    print("Migrated context files:")
    for key in uploaded_context:
        print(f"  s3://{storage.bucket}/{key}")


if __name__ == "__main__":
    main()
