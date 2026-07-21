"""Tests for the deterministic synthetic data generator."""

from __future__ import annotations

import json
from datetime import date

import pytest

from src.generate_data import generate_dataset, main
from src.schemas import CreditScoreBand, IncomeBand, LoanStatus, PaymentMethod, PaymentStatus, RiskSegment

AS_OF_DATE = date(2026, 7, 20)
AS_OF_DATE_STR = "2026-07-20"
DEFAULT_NUM_CUSTOMERS = 100
DEFAULT_SEED = 42


@pytest.fixture(scope="module")
def dataset_dicts():
    customers, loans, payments = generate_dataset(DEFAULT_NUM_CUSTOMERS, DEFAULT_SEED, AS_OF_DATE)
    return (
        [c.to_dict() for c in customers],
        [l.to_dict() for l in loans],
        [p.to_dict() for p in payments],
    )


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


# 1 & 2: expected files are created and contain JSON arrays
def test_generated_files_are_created_and_contain_json_arrays(tmp_path):
    main(["--num-customers", "20", "--seed", "1", "--output-dir", str(tmp_path), "--as-of-date", AS_OF_DATE_STR])
    for name in ("customers.json", "loans.json", "payments.json"):
        path = tmp_path / name
        assert path.exists()
        data = json.loads(path.read_text())
        assert isinstance(data, list)
        assert len(data) > 0


# 3: customer IDs unique
def test_customer_ids_are_unique(dataset_dicts):
    customers, _, _ = dataset_dicts
    ids = [c["customer_id"] for c in customers]
    assert len(ids) == len(set(ids))


# 4: loan IDs unique
def test_loan_ids_are_unique(dataset_dicts):
    _, loans, _ = dataset_dicts
    ids = [l["loan_id"] for l in loans]
    assert len(ids) == len(set(ids))


# 5: payment IDs unique
def test_payment_ids_are_unique(dataset_dicts):
    _, _, payments = dataset_dicts
    ids = [p["payment_id"] for p in payments]
    assert len(ids) == len(set(ids))


# 6: every loan.customer_id exists in customers
def test_every_loan_references_existing_customer(dataset_dicts):
    customers, loans, _ = dataset_dicts
    customer_ids = {c["customer_id"] for c in customers}
    for loan in loans:
        assert loan["customer_id"] in customer_ids


# 7: every payment.loan_id exists in loans
def test_every_payment_references_existing_loan(dataset_dicts):
    _, loans, payments = dataset_dicts
    loan_ids = {l["loan_id"] for l in loans}
    for payment in payments:
        assert payment["loan_id"] in loan_ids


# 8: enum values are valid
def test_customer_enum_values_are_valid(dataset_dicts):
    customers, _, _ = dataset_dicts
    for c in customers:
        assert c["income_band"] in {b.value for b in IncomeBand}
        assert c["credit_score_band"] in {b.value for b in CreditScoreBand}
        assert c["risk_segment"] in {b.value for b in RiskSegment}
        assert len(c["state"]) == 2 and c["state"].isalpha()


def test_loan_enum_values_are_valid(dataset_dicts):
    _, loans, _ = dataset_dicts
    for l in loans:
        assert l["loan_status"] in {s.value for s in LoanStatus}
        assert l["term_months"] in (12, 24, 36, 48, 60)


def test_payment_enum_values_are_valid(dataset_dicts):
    _, _, payments = dataset_dicts
    for p in payments:
        assert p["payment_status"] in {s.value for s in PaymentStatus}
        assert p["payment_method"] in {m.value for m in PaymentMethod}


# 9: principal amounts positive
def test_principal_amounts_are_positive(dataset_dicts):
    _, loans, _ = dataset_dicts
    for l in loans:
        assert l["principal_amount"] > 0


# 10: interest rates between 0 and 1
def test_interest_rates_within_range(dataset_dicts):
    _, loans, _ = dataset_dicts
    for l in loans:
        assert 0 < l["interest_rate"] < 1


# 11: scheduled payment amounts positive
def test_scheduled_payment_amounts_are_positive(dataset_dicts):
    _, loans, _ = dataset_dicts
    for l in loans:
        assert l["scheduled_payment_amount"] > 0


# 12: amount_due positive
def test_amount_due_is_positive(dataset_dicts):
    _, _, payments = dataset_dicts
    for p in payments:
        assert p["amount_due"] > 0


# 13: amount_paid nonnegative
def test_amount_paid_is_nonnegative(dataset_dicts):
    _, _, payments = dataset_dicts
    for p in payments:
        assert p["amount_paid"] >= 0


# 14: dates are valid ISO strings
def test_dates_are_valid_iso_strings(dataset_dicts):
    customers, loans, payments = dataset_dicts
    for c in customers:
        assert _is_iso_date(c["created_at"])
    for l in loans:
        assert _is_iso_date(l["originated_at"])
    for p in payments:
        assert _is_iso_date(p["due_date"])
        if p["payment_date"] is not None:
            assert _is_iso_date(p["payment_date"])


# 15: payment dates follow the expected status rules
def test_payment_dates_follow_status_rules(dataset_dicts):
    _, _, payments = dataset_dicts
    for p in payments:
        due = date.fromisoformat(p["due_date"])
        payment_date = date.fromisoformat(p["payment_date"]) if p["payment_date"] else None
        status = p["payment_status"]
        if status in ("SCHEDULED", "MISSED"):
            assert payment_date is None
            assert p["amount_paid"] == 0
        elif status == "PAID":
            assert payment_date is not None
            assert payment_date <= due
            assert p["amount_paid"] > 0
        elif status == "LATE":
            assert payment_date is not None
            assert payment_date > due
            assert p["amount_paid"] > 0
        elif status == "FAILED":
            assert p["amount_paid"] == 0


# 16: at least one ACTIVE loan
def test_at_least_one_active_loan(dataset_dicts):
    _, loans, _ = dataset_dicts
    assert any(l["loan_status"] == "ACTIVE" for l in loans)


# 17: at least one PAID payment
def test_at_least_one_paid_payment(dataset_dicts):
    _, _, payments = dataset_dicts
    assert any(p["payment_status"] == "PAID" for p in payments)


# 18: at least one LATE or MISSED payment
def test_at_least_one_late_or_missed_payment(dataset_dicts):
    _, _, payments = dataset_dicts
    assert any(p["payment_status"] in ("LATE", "MISSED") for p in payments)


# 19: every DEFAULTED loan has at least one LATE or MISSED payment
def test_defaulted_loans_have_late_or_missed_evidence(dataset_dicts):
    _, loans, payments = dataset_dicts
    payments_by_loan: dict[str, list[dict]] = {}
    for p in payments:
        payments_by_loan.setdefault(p["loan_id"], []).append(p)

    defaulted = [l for l in loans if l["loan_status"] == "DEFAULTED"]
    assert len(defaulted) > 0
    for loan in defaulted:
        loan_payments = payments_by_loan.get(loan["loan_id"], [])
        assert any(p["payment_status"] in ("LATE", "MISSED") for p in loan_payments)


# 20: same seed produces identical output
def test_same_seed_produces_identical_output(tmp_path):
    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    for out in (out1, out2):
        main(["--num-customers", "30", "--seed", "5", "--output-dir", str(out), "--as-of-date", AS_OF_DATE_STR])
    for name in ("customers.json", "loans.json", "payments.json"):
        assert (out1 / name).read_bytes() == (out2 / name).read_bytes()


# 21: different seeds produce different output
def test_different_seeds_produce_different_output(tmp_path):
    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    main(["--num-customers", "30", "--seed", "5", "--output-dir", str(out1), "--as-of-date", AS_OF_DATE_STR])
    main(["--num-customers", "30", "--seed", "6", "--output-dir", str(out2), "--as-of-date", AS_OF_DATE_STR])
    assert (out1 / "customers.json").read_bytes() != (out2 / "customers.json").read_bytes()


def test_default_configuration_hits_target_volume(dataset_dicts):
    _, loans, payments = dataset_dicts
    assert 60 <= len(loans) <= 80
    assert 300 <= len(payments) <= 800


def test_closed_loans_reconcile_to_principal_within_one_cent(dataset_dicts):
    _, loans, payments = dataset_dicts
    payments_by_loan: dict[str, list[dict]] = {}
    for p in payments:
        payments_by_loan.setdefault(p["loan_id"], []).append(p)

    closed = [l for l in loans if l["loan_status"] == "CLOSED"]
    assert len(closed) > 0
    for loan in closed:
        paid_total = sum(
            p["amount_paid"] for p in payments_by_loan[loan["loan_id"]] if p["payment_status"] == "PAID"
        )
        assert abs(paid_total - loan["principal_amount"]) <= 0.01


def test_records_are_sorted_by_id_in_written_files(tmp_path):
    main(["--num-customers", "20", "--seed", "2", "--output-dir", str(tmp_path), "--as-of-date", AS_OF_DATE_STR])
    customers = json.loads((tmp_path / "customers.json").read_text())
    loans = json.loads((tmp_path / "loans.json").read_text())
    payments = json.loads((tmp_path / "payments.json").read_text())
    assert [c["customer_id"] for c in customers] == sorted(c["customer_id"] for c in customers)
    assert [l["loan_id"] for l in loans] == sorted(l["loan_id"] for l in loans)
    assert [p["payment_id"] for p in payments] == sorted(p["payment_id"] for p in payments)


def test_invalid_num_customers_errors_cleanly():
    with pytest.raises(SystemExit):
        main(["--num-customers", "0"])


def test_invalid_as_of_date_errors_cleanly():
    with pytest.raises(SystemExit):
        main(["--as-of-date", "not-a-date"])
