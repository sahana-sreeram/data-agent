"""Pandas transformation from raw loans/payments into a trusted portfolio summary.

Reads data/raw/loans.json and data/raw/payments.json and aggregates them into
a single portfolio-wide summary: total original principal, total successful
payments, and the resulting outstanding balance. Which payment statuses count
as "successful" is not hardcoded here -- it's loaded from
context/business_rules.json, so the business rule lives as data, not code.

For the MVP, only PAID is treated as a successfully settled principal
payment. LATE is retained as a behavioral status but excluded from the
portfolio balance calculation -- see context/business_rules.json for why.

This module intentionally does not validate data quality, call any agent, or
handle repair -- see README.md for scope.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd

from src.schemas import LoanStatus

DEFAULT_LOANS_FILE = "data/raw/loans.json"
DEFAULT_PAYMENTS_FILE = "data/raw/payments.json"
DEFAULT_OUTPUT_DIR = "data/processed"
DEFAULT_AS_OF_DATE = "2026-07-20"
DEFAULT_BUSINESS_RULES_FILE = "context/business_rules.json"

REQUIRED_LOAN_COLUMNS = {"loan_id", "principal_amount", "loan_status"}
REQUIRED_PAYMENT_COLUMNS = {"payment_id", "loan_id", "amount_paid", "payment_status"}
REQUIRED_PAYMENT_EVENT_COLUMNS = {"event_id", "payment_id", "loan_id", "event_type", "event_timestamp", "amount"}


def load_business_rules(path: Path) -> dict:
    """Load the shared business-rules config (e.g. which payment statuses count as successful)."""
    if not path.exists():
        raise FileNotFoundError(f"business rules file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_records(path: Path, label: str) -> pd.DataFrame:
    """Load a JSON array of records from disk into a DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"{label} file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    if not isinstance(records, list):
        raise ValueError(f"{label} file must contain a JSON array: {path}")

    return pd.DataFrame(records)


def load_loans(path: Path) -> pd.DataFrame:
    """Load loans.json into a DataFrame."""
    return _load_records(path, "loans")


def load_payments(path: Path) -> pd.DataFrame:
    """Load payments.json into a DataFrame."""
    return _load_records(path, "payments")


def load_payment_events(path: Path) -> pd.DataFrame:
    """Load payment_events.json into a DataFrame."""
    return _load_records(path, "payment events")


def _validate_columns(df: pd.DataFrame, required: set[str], label: str) -> None:
    if df.empty:
        return
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{label} data is missing required columns: {sorted(missing)}")


def _count_status(df: pd.DataFrame, column: str, value: str) -> int:
    if df.empty:
        return 0
    return int((df[column] == value).sum())


def compute_portfolio_summary(
    loans_df: pd.DataFrame, payments_df: pd.DataFrame, as_of_date: str, business_rules: dict
) -> dict:
    """Aggregate loans and payments into a single portfolio summary.

    total_outstanding_balance = total_original_principal - total_successful_payments,
    where "successful" means payment_status is in
    business_rules["successful_payment_statuses"] (PAID only, for the MVP --
    see context/business_rules.json).
    """
    _validate_columns(loans_df, REQUIRED_LOAN_COLUMNS, "loans")
    _validate_columns(payments_df, REQUIRED_PAYMENT_COLUMNS, "payments")

    success_statuses = business_rules["successful_payment_statuses"]

    total_original_principal = (
        round(float(loans_df["principal_amount"].sum()), 2) if not loans_df.empty else 0.0
    )

    successful_payments = (
        payments_df[payments_df["payment_status"].isin(success_statuses)]
        if not payments_df.empty
        else payments_df
    )
    total_successful_payments = (
        round(float(successful_payments["amount_paid"].sum()), 2) if not successful_payments.empty else 0.0
    )

    total_outstanding_balance = round(total_original_principal - total_successful_payments, 2)

    return {
        "as_of_date": as_of_date,
        "loan_count": int(len(loans_df)),
        "active_loan_count": _count_status(loans_df, "loan_status", LoanStatus.ACTIVE.value),
        "closed_loan_count": _count_status(loans_df, "loan_status", LoanStatus.CLOSED.value),
        "defaulted_loan_count": _count_status(loans_df, "loan_status", LoanStatus.DEFAULTED.value),
        "payment_count": int(len(payments_df)),
        "successful_payment_count": int(len(successful_payments)),
        "total_original_principal": total_original_principal,
        "total_successful_payments": total_successful_payments,
        "total_outstanding_balance": total_outstanding_balance,
    }


def compute_portfolio_summary_from_payment_events(
    loans_df: pd.DataFrame, payment_events_df: pd.DataFrame, as_of_date: str, business_rules: dict
) -> dict:
    """Aggregate loans and a payment-EVENT stream into a portfolio summary.

    THIS IS THE DELIBERATELY BUGGY, additive sibling to
    compute_portfolio_summary. It represents a plausible migration mistake:
    the payments source moved from one row per logical payment to an
    at-least-once lifecycle event stream, and this function was ported from
    the old one-row-per-payment logic without adding a collapse-to-one-
    row-per-payment_id step.

    It filters to the successful terminal event type and sums/counts
    matching ROWS directly -- so a replayed SETTLED event (same payment_id,
    same amount, a different event_id) is counted twice. The source grain
    (one row per event) and the business entity grain (one row per logical
    payment) are silently conflated here.

    See validate_portfolio.validate_portfolio_from_payment_events for the
    correct, entity-grain computation used to independently catch this.
    """
    _validate_columns(loans_df, REQUIRED_LOAN_COLUMNS, "loans")
    _validate_columns(payment_events_df, REQUIRED_PAYMENT_EVENT_COLUMNS, "payment events")

    successful_terminal_event = business_rules["payment_event_rules"]["successful_terminal_event"]

    total_original_principal = (
        round(float(loans_df["principal_amount"].sum()), 2) if not loans_df.empty else 0.0
    )

    settled_events = (
        payment_events_df[payment_events_df["event_type"] == successful_terminal_event]
        if not payment_events_df.empty
        else payment_events_df
    )
    # Collapse duplicate SETTLED events to one per payment_id (latest-only), enforcing amount consistency
    if not settled_events.empty:
        amount_uniques = settled_events.groupby("payment_id")["amount"].nunique()
        conflicting = amount_uniques[amount_uniques > 1]
        if not conflicting.empty:
            raise ValueError(
                f"Conflicting SETTLED amounts across events for payment_ids: {list(conflicting.index)}"
            )
        # Keep only the latest event per payment_id by event_timestamp
        settled_events_sorted = settled_events.sort_values("event_timestamp")
        latest_idx = settled_events_sorted.groupby("payment_id").tail(1).index
        settled_collapsed = settled_events_sorted.loc[latest_idx]
    else:
        settled_collapsed = settled_events
    total_successful_payments = (
        round(float(settled_collapsed["amount"].sum()), 2) if not settled_collapsed.empty else 0.0
    )
    total_outstanding_balance = round(total_original_principal - total_successful_payments, 2)

    return {
        "as_of_date": as_of_date,
        "loan_count": int(len(loans_df)),
        "active_loan_count": _count_status(loans_df, "loan_status", LoanStatus.ACTIVE.value),
        "closed_loan_count": _count_status(loans_df, "loan_status", LoanStatus.CLOSED.value),
        "defaulted_loan_count": _count_status(loans_df, "loan_status", LoanStatus.DEFAULTED.value),
        "payment_count": int(len(payment_events_df)),
        "successful_payment_count": int(len(settled_collapsed)),
        "total_original_principal": total_original_principal,
        "total_successful_payments": total_successful_payments,
        "total_outstanding_balance": total_outstanding_balance,
    }


def compute_portfolio_summary_with_payment_join(
    loans_df: pd.DataFrame, payments_df: pd.DataFrame, as_of_date: str, business_rules: dict
) -> dict:
    """Aggregate loans and payments by first summing successful payments per loan, then
    joining those totals onto loans.

    THIS IS THE DELIBERATELY BUGGY, additive sibling to compute_portfolio_summary. It
    represents a plausible implementation mistake: aggregating successful payments by
    loan_id necessarily produces a row only for loan_ids that have at least one successful
    payment, and this function joins that aggregate onto loans with how="inner" -- so a
    valid loan with zero successful payments (e.g. newly originated, first payment not yet
    due) is silently dropped from the entire portfolio, not just from the payment total.

    See validate_portfolio.validate_portfolio_with_join_profile for the independent,
    join-free computation used to catch this.
    """
    _validate_columns(loans_df, REQUIRED_LOAN_COLUMNS, "loans")
    _validate_columns(payments_df, REQUIRED_PAYMENT_COLUMNS, "payments")

    success_statuses = business_rules["successful_payment_statuses"]

    successful_payments = (
        payments_df[payments_df["payment_status"].isin(success_statuses)]
        if not payments_df.empty
        else payments_df
    )
    payments_by_loan = (
        successful_payments.groupby("loan_id")["amount_paid"].sum()
        if not successful_payments.empty
        else pd.Series(dtype=float, name="amount_paid").rename_axis("loan_id")
    )
    # BUG: how="inner" keeps only loan_ids present in payments_by_loan -- a loan with no
    # successful payments at all (not even a MISSED/SCHEDULED row aggregated away, just
    # genuinely zero rows in payments_by_loan) disappears from `portfolio` entirely, along
    # with its principal.
    portfolio = loans_df.merge(
        payments_by_loan.rename("total_paid"), on="loan_id", how="left"
    )
    # Preserve loans with no successful payments; treat missing totals as zero.
    if "total_paid" in portfolio.columns:
        portfolio["total_paid"] = portfolio["total_paid"].fillna(0.0)
    else:
        portfolio["total_paid"] = 0.0

    total_original_principal = (
        round(float(portfolio["principal_amount"].sum()), 2) if not portfolio.empty else 0.0
    )
    total_successful_payments = round(float(portfolio["total_paid"].sum()), 2) if not portfolio.empty else 0.0
    total_outstanding_balance = round(total_original_principal - total_successful_payments, 2)

    return {
        "as_of_date": as_of_date,
        "loan_count": int(len(portfolio)),
        "active_loan_count": _count_status(portfolio, "loan_status", LoanStatus.ACTIVE.value),
        "closed_loan_count": _count_status(portfolio, "loan_status", LoanStatus.CLOSED.value),
        "defaulted_loan_count": _count_status(portfolio, "loan_status", LoanStatus.DEFAULTED.value),
        "payment_count": int(len(payments_df)),
        "successful_payment_count": int(len(successful_payments)),
        "total_original_principal": total_original_principal,
        "total_successful_payments": total_successful_payments,
        "total_outstanding_balance": total_outstanding_balance,
    }


def write_summary(path: Path, summary: dict) -> None:
    """Write the portfolio summary as a single JSON object with a trailing newline."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")


def print_summary(summary: dict) -> None:
    """Print a human-readable rendering of the portfolio summary."""
    print("Portfolio summary")
    for key, value in summary.items():
        print(f"  {key:<26} {value}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Transform raw loans/payments data into a portfolio summary."
    )
    parser.add_argument("--loans-file", type=str, default=DEFAULT_LOANS_FILE)
    parser.add_argument("--payments-file", type=str, default=DEFAULT_PAYMENTS_FILE)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    parser.add_argument("--business-rules-file", type=str, default=DEFAULT_BUSINESS_RULES_FILE)
    args = parser.parse_args(argv)

    try:
        args.as_of_date = date.fromisoformat(args.as_of_date).isoformat()
    except ValueError as exc:
        parser.error(f"--as-of-date must be an ISO date (YYYY-MM-DD): {exc}")
        raise

    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    loans_df = load_loans(Path(args.loans_file))
    payments_df = load_payments(Path(args.payments_file))
    business_rules = load_business_rules(Path(args.business_rules_file))
    summary = compute_portfolio_summary(loans_df, payments_df, args.as_of_date, business_rules)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_summary(output_dir / "portfolio_summary.json", summary)

    print_summary(summary)


if __name__ == "__main__":
    main()
