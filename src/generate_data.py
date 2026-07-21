"""Deterministic synthetic data generator for the data-agent MVP.

Generates a coherent baseline dataset of customers, loans, and payments for
a simulated lending company. All randomness flows through a single seeded
random.Random instance, and all dates are computed relative to a fixed
--as-of-date rather than the machine clock, so a given (seed, num-customers,
as-of-date) always produces byte-for-byte identical output.

This module intentionally does not implement ETL, validation, agent, or
repair logic -- see README.md for scope.
"""

from __future__ import annotations

import argparse
import calendar
import json
import random
from collections import Counter
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

from src.schemas import (
    TERM_MONTHS_CHOICES,
    US_STATE_CODES,
    CreditScoreBand,
    Customer,
    IncomeBand,
    Loan,
    LoanStatus,
    Payment,
    PaymentMethod,
    PaymentStatus,
    RiskSegment,
)

DEFAULT_SEED = 42
DEFAULT_NUM_CUSTOMERS = 100
DEFAULT_OUTPUT_DIR = "data/raw"
DEFAULT_AS_OF_DATE = "2026-07-20"

CUSTOMER_HISTORY_DAYS = 3 * 365

RISK_SEGMENT_WEIGHTS: dict[RiskSegment, float] = {
    RiskSegment.LOW: 0.40,
    RiskSegment.MEDIUM: 0.35,
    RiskSegment.HIGH: 0.25,
}

CREDIT_BAND_WEIGHTS_BY_RISK: dict[RiskSegment, dict[CreditScoreBand, float]] = {
    RiskSegment.LOW: {
        CreditScoreBand.UNDER_620: 0.02,
        CreditScoreBand.RANGE_620_679: 0.08,
        CreditScoreBand.RANGE_680_719: 0.20,
        CreditScoreBand.RANGE_720_759: 0.35,
        CreditScoreBand.PLUS_760: 0.35,
    },
    RiskSegment.MEDIUM: {
        CreditScoreBand.UNDER_620: 0.10,
        CreditScoreBand.RANGE_620_679: 0.25,
        CreditScoreBand.RANGE_680_719: 0.30,
        CreditScoreBand.RANGE_720_759: 0.25,
        CreditScoreBand.PLUS_760: 0.10,
    },
    RiskSegment.HIGH: {
        CreditScoreBand.UNDER_620: 0.40,
        CreditScoreBand.RANGE_620_679: 0.30,
        CreditScoreBand.RANGE_680_719: 0.15,
        CreditScoreBand.RANGE_720_759: 0.10,
        CreditScoreBand.PLUS_760: 0.05,
    },
}

INCOME_BAND_WEIGHTS: dict[IncomeBand, float] = {
    IncomeBand.UNDER_40000: 0.15,
    IncomeBand.RANGE_40000_60000: 0.25,
    IncomeBand.RANGE_60000_80000: 0.25,
    IncomeBand.RANGE_80000_120000: 0.20,
    IncomeBand.OVER_120000: 0.15,
}

LOAN_PROBABILITY = 0.65
SECOND_LOAN_PROBABILITY = 0.12

LOAN_STATUS_WEIGHTS_BY_RISK: dict[RiskSegment, dict[LoanStatus, float]] = {
    RiskSegment.LOW: {LoanStatus.ACTIVE: 0.60, LoanStatus.CLOSED: 0.32, LoanStatus.DEFAULTED: 0.08},
    RiskSegment.MEDIUM: {LoanStatus.ACTIVE: 0.60, LoanStatus.CLOSED: 0.25, LoanStatus.DEFAULTED: 0.15},
    RiskSegment.HIGH: {LoanStatus.ACTIVE: 0.55, LoanStatus.CLOSED: 0.15, LoanStatus.DEFAULTED: 0.30},
}

# Biased toward shorter terms so full-schedule (CLOSED) loans don't blow up
# total payment volume; also more representative of small consumer loans.
TERM_MONTHS_WEIGHTS: dict[int, float] = {
    12: 0.65,
    24: 0.20,
    36: 0.09,
    48: 0.04,
    60: 0.02,
}

INTEREST_RATE_RANGE_BY_RISK: dict[RiskSegment, tuple[float, float]] = {
    RiskSegment.LOW: (0.04, 0.09),
    RiskSegment.MEDIUM: (0.08, 0.15),
    RiskSegment.HIGH: (0.14, 0.25),
}

PRINCIPAL_AMOUNT_RANGE = (2000.0, 40000.0)

# Recent-history/future windows for loans that don't need a full schedule.
ACTIVE_PAST_WINDOW = 3
ACTIVE_FUTURE_WINDOW = 3
DEFAULTED_HISTORY_WINDOW = 4

PAYMENT_METHOD_WEIGHTS: dict[PaymentMethod, float] = {
    PaymentMethod.ACH: 0.60,
    PaymentMethod.CARD: 0.30,
    PaymentMethod.CHECK: 0.10,
}

PAYMENT_OUTCOME_WEIGHTS_BY_RISK: dict[RiskSegment, dict[PaymentStatus, float]] = {
    RiskSegment.LOW: {
        PaymentStatus.PAID: 0.90,
        PaymentStatus.LATE: 0.07,
        PaymentStatus.MISSED: 0.02,
        PaymentStatus.FAILED: 0.01,
    },
    RiskSegment.MEDIUM: {
        PaymentStatus.PAID: 0.80,
        PaymentStatus.LATE: 0.12,
        PaymentStatus.MISSED: 0.05,
        PaymentStatus.FAILED: 0.03,
    },
    RiskSegment.HIGH: {
        PaymentStatus.PAID: 0.65,
        PaymentStatus.LATE: 0.20,
        PaymentStatus.MISSED: 0.10,
        PaymentStatus.FAILED: 0.05,
    },
}


def weighted_choice(rng: random.Random, weights: dict):
    """Pick one key from a {option: weight} mapping using the given rng."""
    options = list(weights.keys())
    probabilities = list(weights.values())
    return rng.choices(options, weights=probabilities, k=1)[0]


def add_months(base: date, months: int) -> date:
    """Return base shifted by a (possibly negative) number of months."""
    month_index = base.month - 1 + months
    year = base.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _random_date_between(rng: random.Random, start: date, end: date) -> date:
    span_days = (end - start).days
    if span_days <= 0:
        return start
    return start + timedelta(days=rng.randint(0, span_days))


def generate_customers(rng: random.Random, num_customers: int, as_of_date: date) -> list[Customer]:
    """Generate `num_customers` customers with dates anchored to as_of_date."""
    if num_customers <= 0:
        raise ValueError("num_customers must be positive")

    earliest_created_at = as_of_date - timedelta(days=CUSTOMER_HISTORY_DAYS)
    customers = []
    for i in range(1, num_customers + 1):
        risk_segment = weighted_choice(rng, RISK_SEGMENT_WEIGHTS)
        credit_score_band = weighted_choice(rng, CREDIT_BAND_WEIGHTS_BY_RISK[risk_segment])
        income_band = weighted_choice(rng, INCOME_BAND_WEIGHTS)
        created_at = _random_date_between(rng, earliest_created_at, as_of_date)
        customers.append(
            Customer(
                customer_id=f"C{i:06d}",
                created_at=created_at.isoformat(),
                state=rng.choice(US_STATE_CODES),
                income_band=income_band,
                credit_score_band=credit_score_band,
                risk_segment=risk_segment,
            )
        )
    return customers


def _originated_at_for_status(
    rng: random.Random, loan_status: LoanStatus, term_months: int, as_of_date: date
) -> date:
    """Pick an origination date consistent with the loan's lifecycle stage.

    CLOSED loans originate far enough back that the full term has elapsed.
    ACTIVE loans originate partway through their term, leaving some months
    still ahead. DEFAULTED loans originate partway through their term too,
    but generation later stops issuing payments once default is reached.
    A small backward-only jitter varies day-of-month without ever moving
    the origination date past as_of_date.
    """
    if loan_status == LoanStatus.CLOSED:
        buffer_months = rng.randint(1, 24)
        origin = add_months(as_of_date, -(term_months + buffer_months))
    elif loan_status == LoanStatus.DEFAULTED:
        max_elapsed = max(2, term_months - 1)
        elapsed = rng.randint(2, max_elapsed)
        origin = add_months(as_of_date, -elapsed)
    else:
        elapsed = rng.randint(1, max(1, term_months - 1))
        origin = add_months(as_of_date, -elapsed)

    jitter_days = rng.randint(0, 20)
    return origin - timedelta(days=jitter_days)


def generate_loans(rng: random.Random, customers: list[Customer], as_of_date: date) -> list[Loan]:
    """Generate loans for a subset of customers, most getting zero or one."""
    loans = []
    loan_counter = 1
    for customer in customers:
        num_loans = 0
        if rng.random() < LOAN_PROBABILITY:
            num_loans = 1
            if rng.random() < SECOND_LOAN_PROBABILITY:
                num_loans = 2

        for _ in range(num_loans):
            loan_status = weighted_choice(rng, LOAN_STATUS_WEIGHTS_BY_RISK[customer.risk_segment])
            term_months = weighted_choice(rng, TERM_MONTHS_WEIGHTS)
            principal_amount = round(rng.uniform(*PRINCIPAL_AMOUNT_RANGE), 2)
            rate_low, rate_high = INTEREST_RATE_RANGE_BY_RISK[customer.risk_segment]
            interest_rate = round(rng.uniform(rate_low, rate_high), 4)
            originated_at = _originated_at_for_status(rng, loan_status, term_months, as_of_date)
            scheduled_payment_amount = round(principal_amount / term_months, 2)

            loans.append(
                Loan(
                    loan_id=f"L{loan_counter:06d}",
                    customer_id=customer.customer_id,
                    principal_amount=principal_amount,
                    interest_rate=interest_rate,
                    term_months=term_months,
                    originated_at=originated_at.isoformat(),
                    loan_status=loan_status,
                    scheduled_payment_amount=scheduled_payment_amount,
                )
            )
            loan_counter += 1
    return loans


def _build_payment(
    rng: random.Random, payment_id: str, loan: Loan, due_date: date, status: PaymentStatus
) -> Payment:
    """Build one payment record consistent with the given status's rules."""
    amount_due = loan.scheduled_payment_amount

    if status == PaymentStatus.PAID:
        payment_date: date | None = due_date - timedelta(days=rng.randint(0, 5))
        amount_paid = amount_due
    elif status == PaymentStatus.LATE:
        payment_date = due_date + timedelta(days=rng.randint(1, 30))
        amount_paid = amount_due
    elif status == PaymentStatus.MISSED:
        payment_date = None
        amount_paid = 0.0
    elif status == PaymentStatus.FAILED:
        payment_date = due_date + timedelta(days=rng.randint(0, 5))
        amount_paid = 0.0
    else:
        payment_date = None
        amount_paid = 0.0

    return Payment(
        payment_id=payment_id,
        loan_id=loan.loan_id,
        due_date=due_date.isoformat(),
        payment_date=payment_date.isoformat() if payment_date else None,
        amount_due=amount_due,
        amount_paid=amount_paid,
        payment_status=status,
        payment_method=weighted_choice(rng, PAYMENT_METHOD_WEIGHTS),
    )


def _generate_closed_loan_payments(
    rng: random.Random, loan: Loan, due_dates: list[date], id_gen: "_IdGenerator"
) -> list[Payment]:
    """CLOSED loans: every scheduled payment succeeds, and the final one is
    nudged so cumulative amount_paid equals principal_amount within $0.01.
    """
    records = [
        _build_payment(rng, id_gen.next_id(), loan, due_date, PaymentStatus.PAID) for due_date in due_dates
    ]
    total_paid = round(sum(p.amount_paid for p in records), 2)
    shortfall = round(loan.principal_amount - total_paid, 2)
    if shortfall != 0 and records:
        last = records[-1]
        records[-1] = replace(last, amount_paid=round(last.amount_paid + shortfall, 2))
    return records


def _generate_defaulted_loan_payments(
    rng: random.Random,
    loan: Loan,
    due_dates: list[date],
    as_of_date: date,
    risk_segment: RiskSegment,
    id_gen: "_IdGenerator",
) -> list[Payment]:
    """DEFAULTED loans: history up to the point of default, then generation
    stops (no future scheduled payments, no collections modeling). The last
    generated payment is forced LATE or MISSED so default is evidenced.
    """
    due_dates_past = [d for d in due_dates if d <= as_of_date]
    if len(due_dates_past) < 2:
        due_dates_past = due_dates[: min(2, len(due_dates))]
    windowed = due_dates_past[-DEFAULTED_HISTORY_WINDOW:]

    forced_index = len(windowed) - 1
    records = []
    for i, due_date in enumerate(windowed):
        if i == forced_index:
            status = rng.choice([PaymentStatus.LATE, PaymentStatus.MISSED])
        else:
            status = weighted_choice(rng, PAYMENT_OUTCOME_WEIGHTS_BY_RISK[risk_segment])
        records.append(_build_payment(rng, id_gen.next_id(), loan, due_date, status))
    return records


def _generate_active_loan_payments(
    rng: random.Random,
    loan: Loan,
    due_dates: list[date],
    as_of_date: date,
    risk_segment: RiskSegment,
    id_gen: "_IdGenerator",
) -> list[Payment]:
    """ACTIVE loans: a recent window of past due dates gets realized outcomes,
    a near-term window of future due dates are SCHEDULED with no payment_date
    and no amount paid yet. Full loan history isn't generated, only what's
    recent, to keep payment volume representative rather than exhaustive.
    """
    past = [d for d in due_dates if d <= as_of_date][-ACTIVE_PAST_WINDOW:]
    future = [d for d in due_dates if d > as_of_date][:ACTIVE_FUTURE_WINDOW]

    records = []
    for due_date in past:
        status = weighted_choice(rng, PAYMENT_OUTCOME_WEIGHTS_BY_RISK[risk_segment])
        records.append(_build_payment(rng, id_gen.next_id(), loan, due_date, status))
    for due_date in future:
        records.append(_build_payment(rng, id_gen.next_id(), loan, due_date, PaymentStatus.SCHEDULED))
    return records


class _IdGenerator:
    """Sequential zero-padded ID generator, used so payment IDs are assigned
    in generation order (which is also final sorted order).
    """

    def __init__(self, prefix: str, width: int) -> None:
        self._prefix = prefix
        self._width = width
        self._counter = 1

    def next_id(self) -> str:
        value = f"{self._prefix}{self._counter:0{self._width}d}"
        self._counter += 1
        return value


def generate_payments(
    rng: random.Random,
    loans: list[Loan],
    customers_by_id: dict[str, Customer],
    as_of_date: date,
) -> list[Payment]:
    """Generate payments for every loan, branching on loan_status so that
    each loan's payment history matches its lifecycle stage.
    """
    id_gen = _IdGenerator("P", 7)
    payments: list[Payment] = []

    for loan in loans:
        customer = customers_by_id.get(loan.customer_id)
        if customer is None:
            raise ValueError(f"loan {loan.loan_id} references unknown customer {loan.customer_id}")

        origin = date.fromisoformat(loan.originated_at)
        due_dates = [add_months(origin, i) for i in range(1, loan.term_months + 1)]

        if loan.loan_status == LoanStatus.CLOSED:
            payments.extend(_generate_closed_loan_payments(rng, loan, due_dates, id_gen))
        elif loan.loan_status == LoanStatus.DEFAULTED:
            payments.extend(
                _generate_defaulted_loan_payments(
                    rng, loan, due_dates, as_of_date, customer.risk_segment, id_gen
                )
            )
        else:
            payments.extend(
                _generate_active_loan_payments(
                    rng, loan, due_dates, as_of_date, customer.risk_segment, id_gen
                )
            )

    return payments


def write_json(path: Path, records: list[dict]) -> None:
    """Write records as a JSON array with 2-space indentation and a trailing newline."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
        f.write("\n")


def print_summary(customers: list[Customer], loans: list[Loan], payments: list[Payment]) -> None:
    """Print a human-readable generation summary."""
    loan_status_counts = Counter(loan.loan_status.value for loan in loans)
    payment_status_counts = Counter(payment.payment_status.value for payment in payments)
    customers_with_loans = len({loan.customer_id for loan in loans})
    total_principal = round(sum(loan.principal_amount for loan in loans), 2)
    total_paid = round(sum(payment.amount_paid for payment in payments), 2)

    print("Generation summary")
    print(f"  customers:                {len(customers)}")
    print(f"  loans:                    {len(loans)}")
    print(f"  payments:                 {len(payments)}")
    print(f"  customers with loans:     {customers_with_loans}")
    print(f"  active loans:             {loan_status_counts.get(LoanStatus.ACTIVE.value, 0)}")
    print(f"  closed loans:             {loan_status_counts.get(LoanStatus.CLOSED.value, 0)}")
    print(f"  defaulted loans:          {loan_status_counts.get(LoanStatus.DEFAULTED.value, 0)}")
    print("  payment status counts:")
    for status in PaymentStatus:
        print(f"    {status.value:<10} {payment_status_counts.get(status.value, 0)}")
    print(f"  total original principal: {total_principal:.2f}")
    print(f"  total amount paid:        {total_paid:.2f}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate CLI arguments."""
    parser = argparse.ArgumentParser(description="Generate synthetic customers/loans/payments data.")
    parser.add_argument("--num-customers", type=int, default=DEFAULT_NUM_CUSTOMERS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    args = parser.parse_args(argv)

    if args.num_customers <= 0:
        parser.error("--num-customers must be a positive integer")

    try:
        as_of_date = date.fromisoformat(args.as_of_date)
    except ValueError as exc:
        parser.error(f"--as-of-date must be an ISO date (YYYY-MM-DD): {exc}")
        raise
    args.as_of_date = as_of_date

    return args


def generate_dataset(
    num_customers: int, seed: int, as_of_date: date
) -> tuple[list[Customer], list[Loan], list[Payment]]:
    """Generate the full customers/loans/payments dataset for given inputs."""
    rng = random.Random(seed)
    customers = generate_customers(rng, num_customers, as_of_date)
    loans = generate_loans(rng, customers, as_of_date)
    customers_by_id = {customer.customer_id: customer for customer in customers}
    payments = generate_payments(rng, loans, customers_by_id, as_of_date)
    return customers, loans, payments


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    customers, loans, payments = generate_dataset(args.num_customers, args.seed, args.as_of_date)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    write_json(
        output_dir / "customers.json",
        sorted((c.to_dict() for c in customers), key=lambda r: r["customer_id"]),
    )
    write_json(
        output_dir / "loans.json",
        sorted((l.to_dict() for l in loans), key=lambda r: r["loan_id"]),
    )
    write_json(
        output_dir / "payments.json",
        sorted((p.to_dict() for p in payments), key=lambda r: r["payment_id"]),
    )

    print_summary(customers, loans, payments)


if __name__ == "__main__":
    main()
