"""Simulate newly originated loans whose first payment is not yet due.

These loans are valid: they count toward loan_count, their principal counts
toward total_original_principal, their full principal is still outstanding,
and they legitimately have zero payment records so far. This script does NOT
touch data/raw/ (the clean baseline). It reads the clean loans.json and
customers.json and writes a new loans.json to a scenario directory -- the
original 73 loans plus a small deterministic set of new no-payment loans --
so the clean and broken states exist side by side. data/raw/payments.json is
reused directly by the scenario (unchanged: there is nothing to add, since
these loan_ids simply have no payment rows).

This module intentionally does not decide how a downstream ETL should handle
these loans -- that's exactly what src/transform.py's join-based ETL function
gets wrong, and what independent validation (see
validate_portfolio.validate_portfolio_with_join_profile) and diagnosis are
for. See README.md.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import date, timedelta
from pathlib import Path

DEFAULT_LOANS_FILE = "data/raw/loans.json"
DEFAULT_CUSTOMERS_FILE = "data/raw/customers.json"
DEFAULT_OUTPUT_FILE = "data/scenarios/incorrect_join/loans.json"
DEFAULT_SEED = 55
DEFAULT_AS_OF_DATE = "2026-07-20"
DEFAULT_NUM_NEW_LOANS = 5

# Dedicated out-of-band loan_id range -- deliberately not "continue the
# existing sequence," so there is no possible collision even if the baseline
# is later regenerated with more customers/loans.
NEW_LOAN_ID_PREFIX = "L9000"

TERM_MONTHS = 12
PRINCIPAL_AMOUNT_RANGE = (2000.0, 40000.0)
INTEREST_RATE_RANGE = (0.06, 0.18)
ORIGINATED_DAYS_AGO = 3


def load_records(path: Path, label: str) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"{label} file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise ValueError(f"{label} file must contain a JSON array: {path}")
    return records


def _customers_with_fewer_than_two_loans(loans: list[dict], customers: list[dict], count: int) -> list[str]:
    loans_per_customer: dict[str, int] = {}
    for loan in loans:
        loans_per_customer[loan["customer_id"]] = loans_per_customer.get(loan["customer_id"], 0) + 1

    eligible = sorted(
        c["customer_id"] for c in customers if loans_per_customer.get(c["customer_id"], 0) < 2
    )
    if len(eligible) < count:
        raise ValueError(f"need {count} customers with fewer than 2 loans, found only {len(eligible)}")
    return eligible[:count]


def generate_no_payment_loans(
    loans: list[dict],
    customers: list[dict],
    rng: random.Random,
    as_of_date: date,
    count: int = DEFAULT_NUM_NEW_LOANS,
) -> list[dict]:
    """Generate `count` new, valid ACTIVE loans with zero payment records.

    originated_at is set just before as_of_date so the first scheduled due
    date is safely in the future -- these loan_ids will not appear anywhere
    in payments.json, by construction, not by omission.
    """
    customer_ids = _customers_with_fewer_than_two_loans(loans, customers, count)
    originated_at = (as_of_date - timedelta(days=ORIGINATED_DAYS_AGO)).isoformat()

    new_loans = []
    for i, customer_id in enumerate(customer_ids, start=1):
        principal_amount = round(rng.uniform(*PRINCIPAL_AMOUNT_RANGE), 2)
        interest_rate = round(rng.uniform(*INTEREST_RATE_RANGE), 4)
        new_loans.append(
            {
                "loan_id": f"{NEW_LOAN_ID_PREFIX}{i:02d}",
                "customer_id": customer_id,
                "principal_amount": principal_amount,
                "interest_rate": interest_rate,
                "term_months": TERM_MONTHS,
                "originated_at": originated_at,
                "loan_status": "ACTIVE",
                "scheduled_payment_amount": round(principal_amount / TERM_MONTHS, 2),
            }
        )
    return new_loans


def write_loans(path: Path, loans: list[dict]) -> None:
    """Write the combined loans array, sorted by loan_id, with a trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(sorted(loans, key=lambda l: l["loan_id"]), f, indent=2)
        f.write("\n")


def print_summary(new_loans: list[dict]) -> None:
    total_principal = round(sum(loan["principal_amount"] for loan in new_loans), 2)
    print("Incorrect-join scenario simulation")
    print(f"  new loans with zero payment records: {len(new_loans)}")
    print(f"  total principal of new loans:        {total_principal:.2f}")
    for loan in new_loans:
        print(f"    {loan['loan_id']}  customer={loan['customer_id']}  principal={loan['principal_amount']:.2f}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a scenario copy of loans.json with a small deterministic set of new, valid, no-payment ACTIVE loans."
    )
    parser.add_argument("--loans-file", type=str, default=DEFAULT_LOANS_FILE)
    parser.add_argument("--customers-file", type=str, default=DEFAULT_CUSTOMERS_FILE)
    parser.add_argument("--output-file", type=str, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    parser.add_argument("--count", type=int, default=DEFAULT_NUM_NEW_LOANS)
    args = parser.parse_args(argv)

    if args.count <= 0:
        parser.error("--count must be a positive integer")
    try:
        args.as_of_date = date.fromisoformat(args.as_of_date)
    except ValueError as exc:
        parser.error(f"--as-of-date must be an ISO date (YYYY-MM-DD): {exc}")
        raise

    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    loans = load_records(Path(args.loans_file), "loans")
    customers = load_records(Path(args.customers_file), "customers")
    rng = random.Random(args.seed)

    new_loans = generate_no_payment_loans(loans, customers, rng, args.as_of_date, args.count)
    write_loans(Path(args.output_file), [*loans, *new_loans])

    print_summary(new_loans)


if __name__ == "__main__":
    main()
