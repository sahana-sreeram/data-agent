"""Simulate a controlled upstream data-quality regression.

An upstream system starts reporting some already-settled payments with a new
status, "SETTLED", instead of "PAID" -- the exact scenario described in the
project's roadmap. This script does NOT touch data/raw/ (the clean baseline
every other test and demo depends on). It reads the clean payments.json and
writes a corrupted copy to a scenario directory, so the clean and broken
states exist side by side and can be run through the same pipeline for
comparison.

This module intentionally does not repair anything -- it only produces the
"changed data" half of the roadmap's "clean data -> validation passes" /
"changed data -> validation fails" contrast. See README.md for scope.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

DEFAULT_PAYMENTS_FILE = "data/raw/payments.json"
DEFAULT_OUTPUT_FILE = "data/scenarios/settled_bug/payments.json"
DEFAULT_SEED = 99
DEFAULT_FRACTION = 0.2
RELABELED_STATUS = "SETTLED"
SOURCE_STATUS = "PAID"


def load_payment_records(path: Path) -> list[dict]:
    """Load payments.json as a plain list of dicts (no pandas round-trip)."""
    if not path.exists():
        raise FileNotFoundError(f"payments file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise ValueError(f"payments file must contain a JSON array: {path}")
    return records


def relabel_paid_to_settled(
    payments: list[dict], rng: random.Random, fraction: float
) -> tuple[list[dict], list[str]]:
    """Relabel a deterministic, seeded subset of PAID payments to SETTLED.

    amount_paid and payment_date are left untouched -- the money was still
    received, only the status label upstream changed. Returns the updated
    records plus the sorted list of payment_ids that were relabeled.
    """
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be within (0, 1]")

    paid_ids = sorted(p["payment_id"] for p in payments if p["payment_status"] == SOURCE_STATUS)
    if not paid_ids:
        return list(payments), []

    num_to_flip = min(len(paid_ids), max(1, round(len(paid_ids) * fraction)))
    flipped_ids = set(rng.sample(paid_ids, num_to_flip))

    updated = [
        {**p, "payment_status": RELABELED_STATUS} if p["payment_id"] in flipped_ids else p
        for p in payments
    ]
    return updated, sorted(flipped_ids)


def write_payment_records(path: Path, payments: list[dict]) -> None:
    """Write payments as a JSON array, sorted by payment_id, with a trailing newline."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(sorted(payments, key=lambda p: p["payment_id"]), f, indent=2)
        f.write("\n")


def print_summary(payments: list[dict], flipped_ids: list[str]) -> None:
    flipped_amount = round(
        sum(p["amount_paid"] for p in payments if p["payment_id"] in set(flipped_ids)), 2
    )
    print("Upstream change simulation")
    print(f"  payments relabeled PAID -> SETTLED: {len(flipped_ids)}")
    print(f"  dollar amount affected:             {flipped_amount:.2f}")
    if flipped_ids:
        preview = ", ".join(flipped_ids[:5])
        suffix = ", ..." if len(flipped_ids) > 5 else ""
        print(f"  example payment_ids:                {preview}{suffix}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Relabel a seeded subset of PAID payments to SETTLED in a scenario copy of payments.json."
    )
    parser.add_argument("--payments-file", type=str, default=DEFAULT_PAYMENTS_FILE)
    parser.add_argument("--output-file", type=str, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--fraction", type=float, default=DEFAULT_FRACTION)
    args = parser.parse_args(argv)

    if not 0 < args.fraction <= 1:
        parser.error("--fraction must be within (0, 1]")

    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    payments = load_payment_records(Path(args.payments_file))
    rng = random.Random(args.seed)
    updated, flipped_ids = relabel_paid_to_settled(payments, rng, args.fraction)

    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_payment_records(output_path, updated)

    print_summary(payments, flipped_ids)


if __name__ == "__main__":
    main()
