"""Scale generation: runs all 6 services' event production in bounded-memory batches,
instead of one giant in-memory generation, so --profile large doesn't require holding
millions of Python objects in memory at once.

    python3 -m src.generate_upstream_events --profile small --output local
    python3 -m src.generate_upstream_events --profile demo --output s3 --seed 42

Each batch calls src.generate_data.generate_dataset() (unmodified) for a bounded chunk of
customers, namespaces every ID/FK column so batches never collide
(demo/services/common/seeding.generate_namespaced_batch), builds that batch's events for every
service, and writes them immediately (one Parquet part file per batch) before moving to the
next batch -- peak memory is one batch's worth of data, not the whole profile's.

Profile sizes and the default chunk size were set from measured throughput on real hardware
(10 CPU cores, single process, unvectorized per-record generation -- generate_data.py's
existing, proven logic is reused as-is per chunk rather than rewritten into raw NumPy, since
correctness here matters more than raw throughput). Two things were measured directly and
matter a lot for how this module is built:

1. generate_data.generate_dataset() scales roughly QUADRATICALLY with customer count, not
   linearly (2,000 customers: ~1.1s; 4,000: ~5.5s; 8,000: ~21s) -- calling it once for a large
   total customer count is not just slow, it's the dominant cost by far. This is exactly why
   chunking here is chunking, not just a memory optimization: every chunk calls
   generate_dataset() with a SMALL, fixed customer count (DEFAULT_CHUNK_SIZE), regardless of
   how large the overall --profile target is, so total time stays roughly LINEAR in the
   number of chunks. Do not raise the default chunk size to "go faster" -- it does the
   opposite past a few thousand customers per chunk.
2. Partitioning by exact calendar day (rather than month) produced a severe small-files
   problem at realistic volumes (1,000 customers -> 14,000+ files, most under 10KB) --
   demo/services/common/runner.py partitions by event_month instead, which cut both file count and
   write time by more than an order of magnitude in the same test.

"large" is sized to make single-node pandas genuinely impractical (the whole point) while
keeping a full run to a few minutes on this machine, NOT the 10M+ ceiling floated earlier --
see the project plan's Phase 5 notes and demo/services/README.md for the full reasoning and the
measured numbers.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from demo.services.application_service.contract import SCHEMA_VERSION as APPLICATION_SCHEMA_VERSION
from demo.services.application_service.main import SPECS as APPLICATION_SPECS
from demo.services.common.runner import produce_events, write_events
from demo.services.common.seeding import generate_namespaced_batch
from demo.services.loan_service.contract import SCHEMA_VERSION as LOAN_SCHEMA_VERSION
from demo.services.loan_service.main import SPECS as LOAN_SPECS
from demo.services.marketing_service.contract import SCHEMA_VERSION as MARKETING_SCHEMA_VERSION
from demo.services.marketing_service.main import SPECS as MARKETING_SPECS
from demo.services.payment_service.contract import SCHEMA_VERSION as PAYMENT_SCHEMA_VERSION
from demo.services.payment_service.main import _build_specs as build_payment_specs
from demo.services.risk_service.contract import SCHEMA_VERSION as RISK_SCHEMA_VERSION
from demo.services.risk_service.main import SPECS as RISK_SPECS
from demo.services.underwriting_service.contract import SCHEMA_VERSION as UNDERWRITING_SCHEMA_VERSION
from demo.services.underwriting_service.main import SPECS as UNDERWRITING_SPECS
from src.storage import S3Storage

PROFILES: dict[str, int] = {
    "small": 1_000,
    "demo": 20_000,
    "large": 100_000,
}

# Deliberately small -- see this module's docstring on generate_dataset()'s measured
# quadratic scaling. Chunk count grows with --profile size; chunk size does not.
DEFAULT_CHUNK_SIZE = 4_000


def _static_services() -> list[tuple[str, str, list]]:
    return [
        ("marketing_service", MARKETING_SCHEMA_VERSION, MARKETING_SPECS),
        ("application_service", APPLICATION_SCHEMA_VERSION, APPLICATION_SPECS),
        ("underwriting_service", UNDERWRITING_SCHEMA_VERSION, UNDERWRITING_SPECS),
        ("loan_service", LOAN_SCHEMA_VERSION, LOAN_SPECS),
        ("risk_service", RISK_SCHEMA_VERSION, RISK_SPECS),
    ]


def run_scale_generation(
    profile: str,
    seed: int,
    as_of_date: str,
    output: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    local_dir: Path | None = None,
    contract_version: str = "v1",
) -> dict:
    """Runs every service across enough batches to cover `profile`'s customer count. Returns
    a scale report: customer/event counts by service+type, partition count, and durations."""
    total_customers = PROFILES[profile]
    storage = S3Storage() if output == "s3" else None
    local_dir = local_dir or Path("data/events")

    report = {
        "profile": profile,
        "target_customers": total_customers,
        "chunk_size": chunk_size,
        "seed": seed,
        "as_of_date": as_of_date,
        "events_by_service": {},
        "generation_seconds": 0.0,
        "write_seconds": 0.0,
    }

    remaining = total_customers
    batch_index = 0
    while remaining > 0:
        batch_size = min(chunk_size, remaining)
        batch_prefix = f"B{batch_index:05d}"
        batch_seed = seed + batch_index  # a different seed per batch -- distinct records, not a repeated batch

        gen_start = time.time()
        dataset = generate_namespaced_batch(batch_size, batch_seed, as_of_date, batch_prefix)
        report["generation_seconds"] += time.time() - gen_start

        write_start = time.time()
        for service_name, schema_version, specs in _static_services():
            events_by_type = produce_events(service_name, schema_version, specs, batch_size, batch_seed, as_of_date, dataset=dataset)
            batch_report = write_events(events_by_type, service_name, output, storage=storage, local_dir=local_dir, part_index=batch_index)
            _merge_report(report["events_by_service"], service_name, batch_report)

        payment_specs = build_payment_specs(contract_version, batch_size, batch_seed, as_of_date, dataset=dataset)
        events_by_type = produce_events("payment_service", PAYMENT_SCHEMA_VERSION, payment_specs, batch_size, batch_seed, as_of_date, dataset=dataset)
        batch_report = write_events(events_by_type, "payment_service", output, storage=storage, local_dir=local_dir, part_index=batch_index)
        _merge_report(report["events_by_service"], "payment_service", batch_report)
        report["write_seconds"] += time.time() - write_start

        remaining -= batch_size
        batch_index += 1

    report["batch_count"] = batch_index
    report["total_events"] = sum(
        count
        for by_type in report["events_by_service"].values()
        for by_date in by_type.values()
        for count in by_date.values()
    )
    return report


def _merge_report(accumulator: dict, service_name: str, batch_report: dict) -> None:
    service_acc = accumulator.setdefault(service_name, {})
    for event_type, by_date in batch_report.items():
        type_acc = service_acc.setdefault(event_type, {})
        for event_month, count in by_date.items():
            type_acc[event_month] = type_acc.get(event_month, 0) + count


def print_scale_report(report: dict) -> None:
    print(f"profile={report['profile']} target_customers={report['target_customers']} batches={report['batch_count']}")
    print(f"generation: {report['generation_seconds']:.1f}s, write: {report['write_seconds']:.1f}s")
    print(f"total events: {report['total_events']}")
    for service_name, by_type in sorted(report["events_by_service"].items()):
        service_total = sum(count for by_date in by_type.values() for count in by_date.values())
        print(f"  {service_name}: {service_total} events across {len(by_type)} event type(s)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a large, seeded upstream-event dataset in bounded-memory batches.")
    parser.add_argument("--profile", type=str, choices=sorted(PROFILES), default="small")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--as-of-date", type=str, default="2026-07-20")
    parser.add_argument("--output", type=str, choices=["s3", "local"], default="local")
    parser.add_argument("--output-dir", type=str, default="data/events")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--contract-version", type=str, choices=["v1", "v2"], default="v1")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = run_scale_generation(
        args.profile,
        args.seed,
        args.as_of_date,
        args.output,
        chunk_size=args.chunk_size,
        local_dir=Path(args.output_dir),
        contract_version=args.contract_version,
    )
    print_scale_report(report)

    if args.output == "s3":
        S3Storage().write_json(f"curated/scale_reports/{int(time.time())}.json", report)


if __name__ == "__main__":
    main()
