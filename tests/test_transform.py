"""Tests for the pandas loans/payments -> portfolio summary transformation."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.transform import compute_portfolio_summary, compute_portfolio_summary_with_payment_join, load_loans, load_payments, main

AS_OF_DATE = "2026-07-20"
BUSINESS_RULES = {"successful_payment_statuses": ["PAID"]}

LOANS = [
    {"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"},
    {"loan_id": "L000002", "customer_id": "C000002", "principal_amount": 2000.0, "loan_status": "ACTIVE"},
    {"loan_id": "L000003", "customer_id": "C000003", "principal_amount": 3000.0, "loan_status": "DEFAULTED"},
]

PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 1000.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000002", "amount_paid": 500.0, "payment_status": "PAID"},
    {"payment_id": "P0000003", "loan_id": "L000002", "amount_paid": 0.0, "payment_status": "SCHEDULED"},
    {"payment_id": "P0000004", "loan_id": "L000003", "amount_paid": 300.0, "payment_status": "LATE"},
    {"payment_id": "P0000005", "loan_id": "L000003", "amount_paid": 0.0, "payment_status": "MISSED"},
]


@pytest.fixture()
def loans_df():
    return pd.DataFrame(LOANS)


@pytest.fixture()
def payments_df():
    return pd.DataFrame(PAYMENTS)


def test_summary_arithmetic_matches_hand_calculation(loans_df, payments_df):
    summary = compute_portfolio_summary(loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)
    # Only the two PAID payments (1000.0 + 500.0) count as successful.
    assert summary["total_original_principal"] == 6000.0
    assert summary["total_successful_payments"] == 1500.0
    assert summary["total_outstanding_balance"] == 4500.0


def test_summary_excludes_late_missed_and_scheduled_from_successful_total(loans_df, payments_df):
    summary = compute_portfolio_summary(loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["successful_payment_count"] == 2
    assert summary["payment_count"] == 5


def test_summary_loan_status_counts(loans_df, payments_df):
    summary = compute_portfolio_summary(loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["loan_count"] == 3
    assert summary["active_loan_count"] == 1
    assert summary["closed_loan_count"] == 1
    assert summary["defaulted_loan_count"] == 1


def test_summary_includes_as_of_date(loans_df, payments_df):
    summary = compute_portfolio_summary(loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["as_of_date"] == AS_OF_DATE


def test_summary_handles_empty_payments():
    loans_df = pd.DataFrame(LOANS)
    empty_payments_df = pd.DataFrame([])
    summary = compute_portfolio_summary(loans_df, empty_payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["total_successful_payments"] == 0.0
    assert summary["total_outstanding_balance"] == summary["total_original_principal"]
    assert summary["payment_count"] == 0


def test_summary_handles_empty_loans():
    empty_loans_df = pd.DataFrame([])
    payments_df = pd.DataFrame(PAYMENTS)
    summary = compute_portfolio_summary(empty_loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["total_original_principal"] == 0.0
    assert summary["loan_count"] == 0


def test_missing_required_loan_column_raises():
    bad_loans_df = pd.DataFrame([{"loan_id": "L1", "loan_status": "ACTIVE"}])  # no principal_amount
    payments_df = pd.DataFrame(PAYMENTS)
    with pytest.raises(ValueError):
        compute_portfolio_summary(bad_loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)


def test_missing_required_payment_column_raises():
    loans_df = pd.DataFrame(LOANS)
    bad_payments_df = pd.DataFrame([{"payment_id": "P1", "loan_id": "L1"}])  # no amount_paid/payment_status
    with pytest.raises(ValueError):
        compute_portfolio_summary(loans_df, bad_payments_df, AS_OF_DATE, BUSINESS_RULES)


def test_load_loans_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_loans(tmp_path / "does_not_exist.json")


def test_load_payments_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_payments(tmp_path / "does_not_exist.json")


def test_cli_writes_single_json_object(tmp_path):
    loans_path = tmp_path / "loans.json"
    payments_path = tmp_path / "payments.json"
    loans_path.write_text(json.dumps(LOANS))
    payments_path.write_text(json.dumps(PAYMENTS))

    output_dir = tmp_path / "processed"
    main(
        [
            "--loans-file", str(loans_path),
            "--payments-file", str(payments_path),
            "--output-dir", str(output_dir),
            "--as-of-date", AS_OF_DATE,
        ]
    )

    output_path = output_dir / "portfolio_summary.json"
    assert output_path.exists()
    data = json.loads(output_path.read_text())
    assert isinstance(data, dict)
    assert data["total_outstanding_balance"] == 4500.0


def test_cli_same_inputs_produce_identical_output(tmp_path):
    loans_path = tmp_path / "loans.json"
    payments_path = tmp_path / "payments.json"
    loans_path.write_text(json.dumps(LOANS))
    payments_path.write_text(json.dumps(PAYMENTS))

    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    for out in (out1, out2):
        main(
            [
                "--loans-file", str(loans_path),
                "--payments-file", str(payments_path),
                "--output-dir", str(out),
                "--as-of-date", AS_OF_DATE,
            ]
        )
    assert (out1 / "portfolio_summary.json").read_bytes() == (out2 / "portfolio_summary.json").read_bytes()


# compute_portfolio_summary_with_payment_join is the incorrect_join scenario's ETL --
# deliberately buggy today (drops loans with zero successful payments via an inner join),
# and expected to be fixed live by the repair agent later. Only structurally-invariant
# properties are asserted here (true under both the buggy inner join and the eventual left
# join) so these tests don't go stale the moment the function is repaired -- matching the
# existing precedent that compute_portfolio_summary_from_payment_events also has no
# semantic-locking unit test; correctness there is proven by independent validation plus the
# live verify-repair rerun, not a pre-written assertion.


def test_join_etl_missing_required_loan_column_raises():
    bad_loans_df = pd.DataFrame([{"loan_id": "L1", "loan_status": "ACTIVE"}])  # no principal_amount
    payments_df = pd.DataFrame(PAYMENTS)
    with pytest.raises(ValueError):
        compute_portfolio_summary_with_payment_join(bad_loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)


def test_join_etl_missing_required_payment_column_raises():
    loans_df = pd.DataFrame(LOANS)
    bad_payments_df = pd.DataFrame([{"payment_id": "P1", "loan_id": "L1"}])  # no amount_paid/payment_status
    with pytest.raises(ValueError):
        compute_portfolio_summary_with_payment_join(loans_df, bad_payments_df, AS_OF_DATE, BUSINESS_RULES)


def test_join_etl_payment_totals_are_correct_regardless_of_loan_inclusion(loans_df, payments_df):
    # A loan with zero successful payments contributes exactly $0 to
    # total_successful_payments whether or not it's included in the joined portfolio -- so
    # this must hold true both before and after the join bug is fixed.
    summary = compute_portfolio_summary_with_payment_join(loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["payment_count"] == 5
    assert summary["successful_payment_count"] == 2
    assert summary["total_successful_payments"] == 1500.0


def test_join_etl_loan_with_successful_payment_is_always_included():
    loans_df = pd.DataFrame([{"loan_id": "L1", "customer_id": "C1", "principal_amount": 1000.0, "loan_status": "ACTIVE"}])
    payments_df = pd.DataFrame([{"payment_id": "P1", "loan_id": "L1", "amount_paid": 400.0, "payment_status": "PAID"}])
    summary = compute_portfolio_summary_with_payment_join(loans_df, payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["loan_count"] == 1
    assert summary["total_original_principal"] == 1000.0
    assert summary["total_outstanding_balance"] == 600.0


def test_join_etl_handles_empty_payments():
    loans_df = pd.DataFrame(LOANS)
    empty_payments_df = pd.DataFrame([])
    summary = compute_portfolio_summary_with_payment_join(loans_df, empty_payments_df, AS_OF_DATE, BUSINESS_RULES)
    assert summary["payment_count"] == 0
    assert summary["successful_payment_count"] == 0
    assert summary["total_successful_payments"] == 0.0


def test_cli_invalid_as_of_date_errors_cleanly():
    with pytest.raises(SystemExit):
        main(["--as-of-date", "not-a-date"])


def test_end_to_end_against_real_generated_data():
    from pathlib import Path

    loans_path = Path("data/raw/loans.json")
    payments_path = Path("data/raw/payments.json")
    if not loans_path.exists() or not payments_path.exists():
        pytest.skip("data/raw not generated yet")

    raw_loans = json.loads(loans_path.read_text())
    raw_payments = json.loads(payments_path.read_text())
    expected_principal = round(sum(l["principal_amount"] for l in raw_loans), 2)
    expected_paid = round(
        sum(p["amount_paid"] for p in raw_payments if p["payment_status"] == "PAID"), 2
    )

    summary = compute_portfolio_summary(load_loans(loans_path), load_payments(payments_path), AS_OF_DATE, BUSINESS_RULES)

    assert summary["total_original_principal"] == expected_principal
    assert summary["total_successful_payments"] == expected_paid
    assert summary["total_outstanding_balance"] == round(expected_principal - expected_paid, 2)
