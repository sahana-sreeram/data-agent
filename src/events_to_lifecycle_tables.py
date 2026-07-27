"""Projects the 6 upstream services' event batches into today's exact raw/*.parquet table
shapes, so migrate_lifecycle_to_s3.py and all 5 existing Spark ETL pipelines run completely
unmodified regardless of whether the data originated from direct generation or from services.

Each event's payload IS the same dict a raw-table dataclass's to_dict() produces (see
services/common/runner.py) -- reconstructing a table is just concatenating every event whose
domain event_type maps to it and dropping the envelope's own underscore-prefixed columns
(_event_id, _event_type, _schema_version, _service, _emitted_at). No business logic lives
here; this is a structural pass-through, not a transformation.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.storage import S3Storage

# Inverse of every service's TableEventSpec event_type_fn -- which raw table a domain event
# type reconstructs into. Multiple event types collapsing into one table (email_events,
# payment_events) mirrors how those tables always had more than one row "kind" even before
# services existed.
EVENT_TYPE_TO_TABLE: dict[str, str] = {
    "CustomerProfileObserved": "customers",
    "CampaignCreated": "campaigns",
    "CouponRuleDefined": "coupon_rules",
    "EmailSent": "email_events",
    "EmailOpened": "email_events",
    "EmailClicked": "email_events",
    "PrequalificationCreated": "prequal_offers",
    "ApplicationSubmitted": "applications",
    "UnderwritingDecisionMade": "underwriting_decisions",
    "LoanFunded": "loans",
    "PaymentScheduled": "payment_schedule",
    "PaymentReceived": "payment_events",
    "PaymentFailed": "payment_events",
    "LoanBecameDelinquent": "delinquency_events",
    "LoanDefaulted": "defaults",
}

SERVICE_BY_EVENT_TYPE: dict[str, str] = {
    "CustomerProfileObserved": "marketing_service",
    "CampaignCreated": "marketing_service",
    "CouponRuleDefined": "marketing_service",
    "EmailSent": "marketing_service",
    "EmailOpened": "marketing_service",
    "EmailClicked": "marketing_service",
    "PrequalificationCreated": "marketing_service",
    "ApplicationSubmitted": "application_service",
    "UnderwritingDecisionMade": "underwriting_service",
    "LoanFunded": "loan_service",
    "PaymentScheduled": "payment_service",
    "PaymentReceived": "payment_service",
    "PaymentFailed": "payment_service",
    "LoanBecameDelinquent": "risk_service",
    "LoanDefaulted": "risk_service",
}

_ENVELOPE_COLUMNS = ["_event_id", "_event_type", "_schema_version", "_service", "_emitted_at"]


def _strip_envelope(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=[c for c in _ENVELOPE_COLUMNS if c in df.columns])


def _read_event_type_from_s3(storage: S3Storage, event_type: str) -> pd.DataFrame:
    service = SERVICE_BY_EVENT_TYPE[event_type]
    prefix = f"events/{service}/{event_type}/"
    parts = [storage.read_parquet(key) for key in storage.list_paths(prefix) if key.endswith(".parquet")]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _read_event_type_from_local(local_dir: Path, event_type: str) -> pd.DataFrame:
    service = SERVICE_BY_EVENT_TYPE[event_type]
    event_dir = local_dir / "events" / service / event_type
    if not event_dir.exists():
        return pd.DataFrame()
    parts = [pd.read_parquet(path, engine="pyarrow") for path in sorted(event_dir.rglob("part-*.parquet"))]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def build_lifecycle_tables_from_events(
    storage: S3Storage | None = None, local_dir: Path | None = None
) -> dict[str, pd.DataFrame]:
    """Reads every event type's batches (from S3 if `storage` is given, else from
    `local_dir`) and returns {table_name: DataFrame} in the exact shape
    src.validate_lifecycle_raw.validate_lifecycle_raw and every etl_spark_*.py module expect."""
    if storage is None and local_dir is None:
        raise ValueError("one of storage or local_dir must be given")

    by_table: dict[str, list[pd.DataFrame]] = {}
    for event_type, table_name in EVENT_TYPE_TO_TABLE.items():
        df = _read_event_type_from_s3(storage, event_type) if storage is not None else _read_event_type_from_local(local_dir, event_type)
        if not df.empty:
            by_table.setdefault(table_name, []).append(_strip_envelope(df))

    return {
        table_name: pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0]
        for table_name, parts in by_table.items()
    }


def write_lifecycle_tables_to_s3(storage: S3Storage, tables: dict[str, pd.DataFrame]) -> None:
    """Writes each reconstructed table to raw/<table>.parquet -- exactly where every
    etl_spark_*.py module and migrate_lifecycle_to_s3.py's own output already lives."""
    for table_name, df in tables.items():
        storage.write_parquet(f"raw/{table_name}.parquet", df)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Project upstream-service events into raw/*.parquet lifecycle tables.")
    parser.add_argument("--from", dest="source", type=str, choices=["s3", "local"], default="s3")
    parser.add_argument("--local-dir", type=str, default="data/events")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()

    if args.source == "s3":
        tables = build_lifecycle_tables_from_events(storage=storage)
    else:
        tables = build_lifecycle_tables_from_events(local_dir=Path(args.local_dir))

    write_lifecycle_tables_to_s3(storage, tables)
    for table_name, df in sorted(tables.items()):
        print(f"  raw/{table_name}.parquet: {len(df)} rows")
    missing = set(EVENT_TYPE_TO_TABLE.values()) - set(tables)
    if missing:
        print(f"  (no events yet for: {sorted(missing)})")


if __name__ == "__main__":
    main()
