"""The shared producer runner: turns a service's declared table->event mapping into an
in-memory batch of Events, then writes them partitioned by event_month to either a local
directory or S3-compatible storage, at:

    events/<service>/<event_type>/event_month=YYYY-MM/part-0000.parquet

Every service's main.py is just: declare a schema_version and a list of TableEventSpecs,
then call produce_events() + write_events(). No service re-implements generation, envelope
construction, or output layout.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from services.common.envelope import Event, deterministic_event_id, events_to_dataframe
from services.common.seeding import generate_shared_dataset
from src.storage import S3Storage

DEFAULT_SEED = 42
DEFAULT_NUM_CUSTOMERS = 100
DEFAULT_AS_OF_DATE = "2026-07-20"


@dataclass(frozen=True)
class TableEventSpec:
    table_name: str
    natural_key_field: str
    # Reads emitted_at from the ALREADY-TRANSFORMED payload without needing to add a
    # synthetic field to it -- keeps the stored payload identical to the original table's
    # to_dict() shape, which src/events_to_lifecycle_tables.py depends on. No default: every
    # spec must say explicitly which (possibly computed) field is its emitted_at source.
    emitted_at_fn: Callable[[dict], str]
    event_type_fn: Callable[[dict], str]
    transform_fn: Callable[[dict], dict] = field(default=lambda record: record)


def produce_events(
    service_name: str,
    schema_version: str,
    specs: list[TableEventSpec],
    num_customers: int,
    seed: int,
    as_of_date: str,
    dataset: dict[str, list] | None = None,
) -> dict[str, list[Event]]:
    """If `dataset` is given, uses it directly instead of calling generate_shared_dataset --
    src/generate_upstream_events.py uses this to pass an already-namespaced batch (plain
    dicts, not dataclass records) through the same event-construction logic every service
    uses, so scale generation doesn't duplicate it."""
    if dataset is None:
        dataset = generate_shared_dataset(num_customers, seed, as_of_date)
    events_by_type: dict[str, list[Event]] = defaultdict(list)

    for spec in specs:
        for record in dataset[spec.table_name]:
            record_dict = record.to_dict() if hasattr(record, "to_dict") else record
            payload = spec.transform_fn(record_dict)
            event_type = spec.event_type_fn(payload)
            natural_key = str(payload[spec.natural_key_field])
            events_by_type[event_type].append(
                Event(
                    event_id=deterministic_event_id(service_name, event_type, natural_key),
                    event_type=event_type,
                    schema_version=schema_version,
                    service=service_name,
                    emitted_at=str(spec.emitted_at_fn(payload)),
                    payload=payload,
                )
            )
    return dict(events_by_type)


def _event_month(emitted_at: str) -> str:
    """YYYY-MM from an ISO date/datetime string. Partitioning by month rather than by exact
    day is a deliberate choice: at realistic data volumes, per-calendar-day partitioning
    over a multi-year date range produces mostly-empty tiny Parquet files (the classic
    "small files" anti-pattern) for anything but the highest-frequency event types --
    measured directly during development (1,000 customers produced >14,000 sub-10KB files
    under day-level partitioning). Monthly partitioning cuts partition count by ~30x while
    still being a completely standard, Spark-friendly layout."""
    return emitted_at[:7]


def write_events(
    events_by_type: dict[str, list[Event]],
    service_name: str,
    output: str,
    storage: S3Storage | None = None,
    local_dir: Path | None = None,
    part_index: int = 0,
) -> dict:
    """output is "s3" or "local". Returns a small report: {event_type: {event_month: row_count}}.

    part_index distinguishes multiple batches writing into the same
    events/<service>/<event_type>/event_month=.../ partition (e.g. src/generate_upstream_events.py
    writing one batch at a time) -- each batch gets its own part-NNNN.parquet instead of
    overwriting the previous one. A single-shot service run (the default) always uses
    part_index=0, matching every existing service's documented output path exactly."""
    report: dict[str, dict[str, int]] = {}

    for event_type, events in events_by_type.items():
        by_month: dict[str, list[Event]] = defaultdict(list)
        for event in events:
            by_month[_event_month(event.emitted_at)].append(event)

        report[event_type] = {}
        for event_month, month_events in by_month.items():
            df = events_to_dataframe(month_events)
            key = f"events/{service_name}/{event_type}/event_month={event_month}/part-{part_index:04d}.parquet"
            if output == "s3":
                (storage or S3Storage()).write_parquet(key, df)
            else:
                path = (local_dir or Path("data/events")) / key
                path.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(path, engine="pyarrow", index=False)
            report[event_type][event_month] = len(month_events)

    return report


def base_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--num-customers", type=int, default=DEFAULT_NUM_CUSTOMERS)
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    parser.add_argument("--output", type=str, choices=["s3", "local"], default="local")
    parser.add_argument("--output-dir", type=str, default="data/events", help="Used when --output=local.")
    return parser


def print_report(service_name: str, report: dict) -> None:
    total = sum(count for by_date in report.values() for count in by_date.values())
    print(f"{service_name}: {total} events across {len(report)} event type(s)")
    for event_type, by_date in sorted(report.items()):
        print(f"  {event_type}: {sum(by_date.values())} events across {len(by_date)} event_month partition(s)")
