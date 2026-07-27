"""Simulate an upstream migration from one-row-per-payment to a lifecycle
event stream, with at-least-once delivery.

Reads the clean data/raw/payments.json (never modified) and writes a new,
scenario-local payment_events.json where each original payment becomes one
or more event rows:

- event_id is the unique row key.
- payment_id identifies the underlying LOGICAL payment and MAY repeat across
  several event rows for the same payment.
- event_type is one of INITIATED, PROCESSING, FAILED, SETTLED.

A seeded subset of settled payments receive an exact-replay duplicate
SETTLED event: same payment_id, same amount, a new event_id, and a later
event_timestamp. This models at-least-once redelivery -- an expected
characteristic of the new source, not a data-quality defect by itself. What
IS a defect is a downstream consumer that doesn't collapse these to one
counted event per logical payment before aggregating -- that is deliberately
left for src/transform.py's event-aware ETL function to get wrong.

This module intentionally does not decide how duplicates should be handled
downstream -- see validate_portfolio.validate_portfolio_from_payment_events
and README.md.
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from datetime import date, datetime, time, timedelta
from pathlib import Path

DEFAULT_PAYMENTS_FILE = "data/raw/payments.json"
DEFAULT_OUTPUT_FILE = "data/scenarios/payment_events_cardinality/payment_events.json"
DEFAULT_SEED = 77
DEFAULT_DUPLICATE_FRACTION = 0.05

INITIATED = "INITIATED"
PROCESSING = "PROCESSING"
FAILED = "FAILED"
SETTLED = "SETTLED"

PROCESSING_EVENT_PROBABILITY = 0.3
INITIATED_LEAD_DAYS_RANGE = (3, 10)
PROCESSING_LEAD_HOURS_RANGE = (2, 48)
DUPLICATE_REPLAY_DELAY_HOURS_RANGE = (1, 72)


def load_payment_records(path: Path) -> list[dict]:
    """Load payments.json as a plain list of dicts (no pandas round-trip)."""
    if not path.exists():
        raise FileNotFoundError(f"payments file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise ValueError(f"payments file must contain a JSON array: {path}")
    return records


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class _IdGenerator:
    """Sequential zero-padded event_id generator; always unique regardless of payment_id repeats."""

    def __init__(self, prefix: str, width: int) -> None:
        self._prefix = prefix
        self._width = width
        self._counter = 1

    def next_id(self) -> str:
        value = f"{self._prefix}{self._counter:0{self._width}d}"
        self._counter += 1
        return value


def _events_for_payment(rng: random.Random, payment: dict, id_gen: _IdGenerator) -> list[dict]:
    """Expand one clean one-row payment record into its lifecycle event rows."""
    payment_id = payment["payment_id"]
    loan_id = payment["loan_id"]
    status = payment["payment_status"]
    due_date = date.fromisoformat(payment["due_date"])
    amount_due = payment["amount_due"]

    events: list[dict] = []
    initiated_dt = datetime.combine(
        due_date - timedelta(days=rng.randint(*INITIATED_LEAD_DAYS_RANGE)), time(9, 0)
    )
    events.append(
        {
            "event_id": id_gen.next_id(),
            "payment_id": payment_id,
            "loan_id": loan_id,
            "event_type": INITIATED,
            "event_timestamp": _iso(initiated_dt),
            "amount": amount_due,
        }
    )

    if status in ("PAID", "LATE", "FAILED") and rng.random() < PROCESSING_EVENT_PROBABILITY:
        processing_dt = initiated_dt + timedelta(hours=rng.randint(*PROCESSING_LEAD_HOURS_RANGE))
        events.append(
            {
                "event_id": id_gen.next_id(),
                "payment_id": payment_id,
                "loan_id": loan_id,
                "event_type": PROCESSING,
                "event_timestamp": _iso(processing_dt),
                "amount": amount_due,
            }
        )

    if status in ("PAID", "LATE"):
        payment_date = date.fromisoformat(payment["payment_date"])
        settled_dt = datetime.combine(payment_date, time(15, 0))
        events.append(
            {
                "event_id": id_gen.next_id(),
                "payment_id": payment_id,
                "loan_id": loan_id,
                "event_type": SETTLED,
                "event_timestamp": _iso(settled_dt),
                "amount": payment["amount_paid"],
            }
        )
    elif status == "FAILED":
        payment_date = date.fromisoformat(payment["payment_date"])
        failed_dt = datetime.combine(payment_date, time(15, 0))
        events.append(
            {
                "event_id": id_gen.next_id(),
                "payment_id": payment_id,
                "loan_id": loan_id,
                "event_type": FAILED,
                "event_timestamp": _iso(failed_dt),
                "amount": amount_due,
            }
        )
    # MISSED and SCHEDULED payments never reach a terminal event -- INITIATED only.

    return events


def generate_payment_events(
    payments: list[dict], rng: random.Random, duplicate_fraction: float = DEFAULT_DUPLICATE_FRACTION
) -> tuple[list[dict], list[str]]:
    """Expand payments into events, then seed exact-replay SETTLED duplicates.

    Returns (events, sorted list of payment_ids that received a duplicate).
    """
    if not 0 < duplicate_fraction <= 1:
        raise ValueError("duplicate_fraction must be within (0, 1]")

    id_gen = _IdGenerator("E", 7)
    events: list[dict] = []
    for payment in payments:
        events.extend(_events_for_payment(rng, payment, id_gen))

    settled_events = [e for e in events if e["event_type"] == SETTLED]
    settled_payment_ids = sorted({e["payment_id"] for e in settled_events})
    if not settled_payment_ids:
        return events, []

    num_duplicates = max(1, round(len(settled_payment_ids) * duplicate_fraction))
    num_duplicates = min(num_duplicates, len(settled_payment_ids))
    duplicate_payment_ids = set(rng.sample(settled_payment_ids, num_duplicates))

    for event in settled_events:
        if event["payment_id"] in duplicate_payment_ids:
            original_dt = datetime.strptime(event["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ")
            replay_dt = original_dt + timedelta(hours=rng.randint(*DUPLICATE_REPLAY_DELAY_HOURS_RANGE))
            events.append(
                {
                    "event_id": id_gen.next_id(),
                    "payment_id": event["payment_id"],
                    "loan_id": event["loan_id"],
                    "event_type": SETTLED,
                    "event_timestamp": _iso(replay_dt),
                    "amount": event["amount"],  # exact replay: identical amount, by MVP design
                }
            )

    return events, sorted(duplicate_payment_ids)


def write_payment_events(path: Path, events: list[dict]) -> None:
    """Write events as a JSON array sorted by event_id, with a trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(sorted(events, key=lambda e: e["event_id"]), f, indent=2)
        f.write("\n")


def print_summary(events: list[dict], duplicate_payment_ids: list[str]) -> None:
    type_counts = Counter(e["event_type"] for e in events)
    settled_by_payment: dict[str, list[dict]] = {}
    for e in events:
        if e["event_type"] == SETTLED:
            settled_by_payment.setdefault(e["payment_id"], []).append(e)
    duplicate_amount_total = round(
        sum(rows[0]["amount"] for pid, rows in settled_by_payment.items() if pid in set(duplicate_payment_ids)), 2
    )

    print("Payment-events migration simulation")
    print(f"  total events:                       {len(events)}")
    for event_type in (INITIATED, PROCESSING, FAILED, SETTLED):
        print(f"    {event_type:<10}                     {type_counts.get(event_type, 0)}")
    print(f"  logical payments w/ duplicate SETTLED: {len(duplicate_payment_ids)}")
    print(f"  duplicate replay amount total:      {duplicate_amount_total:.2f}")
    if duplicate_payment_ids:
        preview = ", ".join(duplicate_payment_ids[:5])
        suffix = ", ..." if len(duplicate_payment_ids) > 5 else ""
        print(f"  example duplicated payment_ids:     {preview}{suffix}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate a payments-to-payment-events migration with at-least-once SETTLED replays."
    )
    parser.add_argument("--payments-file", type=str, default=DEFAULT_PAYMENTS_FILE)
    parser.add_argument("--output-file", type=str, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--duplicate-fraction", type=float, default=DEFAULT_DUPLICATE_FRACTION)
    args = parser.parse_args(argv)

    if not 0 < args.duplicate_fraction <= 1:
        parser.error("--duplicate-fraction must be within (0, 1]")

    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    payments = load_payment_records(Path(args.payments_file))
    rng = random.Random(args.seed)
    events, duplicate_payment_ids = generate_payment_events(payments, rng, args.duplicate_fraction)

    write_payment_events(Path(args.output_file), events)
    print_summary(events, duplicate_payment_ids)


if __name__ == "__main__":
    main()
