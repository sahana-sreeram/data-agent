"""Tests for the incorrect_join scenario's deterministic loan generator."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from src.legacy.simulate_incorrect_join import (
    generate_no_payment_loans,
    load_records,
    main,
    write_loans,
)

AS_OF_DATE = date(2026, 7, 20)

LOANS = [
    {"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"},
    {"loan_id": "L000002", "customer_id": "C000002", "principal_amount": 2000.0, "loan_status": "ACTIVE"},
]
CUSTOMERS = [
    {"customer_id": "C000001", "state": "CA"},
    {"customer_id": "C000002", "state": "NY"},
    {"customer_id": "C000003", "state": "TX"},
    {"customer_id": "C000004", "state": "FL"},
    {"customer_id": "C000005", "state": "WA"},
    {"customer_id": "C000006", "state": "OR"},
]


def _rng():
    import random

    return random.Random(1)


def test_generates_the_requested_count():
    new_loans = generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=3)
    assert len(new_loans) == 3


def test_new_loans_are_active_with_positive_principal_and_unique_ids():
    new_loans = generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=3)
    loan_ids = [loan["loan_id"] for loan in new_loans]
    assert len(set(loan_ids)) == len(loan_ids)
    for loan in new_loans:
        assert loan["loan_status"] == "ACTIVE"
        assert loan["principal_amount"] > 0
        assert loan["scheduled_payment_amount"] == round(loan["principal_amount"] / loan["term_months"], 2)


def test_new_loan_ids_do_not_collide_with_existing_loans():
    new_loans = generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=3)
    existing_ids = {loan["loan_id"] for loan in LOANS}
    new_ids = {loan["loan_id"] for loan in new_loans}
    assert existing_ids.isdisjoint(new_ids)


def test_new_loans_use_existing_customer_ids():
    new_loans = generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=3)
    known_customer_ids = {c["customer_id"] for c in CUSTOMERS}
    for loan in new_loans:
        assert loan["customer_id"] in known_customer_ids


def test_originated_at_is_before_as_of_date_so_first_payment_is_not_yet_due():
    new_loans = generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=3)
    for loan in new_loans:
        assert date.fromisoformat(loan["originated_at"]) < AS_OF_DATE


def test_raises_if_not_enough_eligible_customers():
    with pytest.raises(ValueError):
        generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=100)


def test_same_seed_produces_identical_output():
    first = generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=3)
    second = generate_no_payment_loans(LOANS, CUSTOMERS, _rng(), AS_OF_DATE, count=3)
    assert first == second


def test_different_seed_produces_different_principal_amounts():
    import random

    first = generate_no_payment_loans(LOANS, CUSTOMERS, random.Random(1), AS_OF_DATE, count=3)
    second = generate_no_payment_loans(LOANS, CUSTOMERS, random.Random(2), AS_OF_DATE, count=3)
    assert [loan["principal_amount"] for loan in first] != [loan["principal_amount"] for loan in second]


def test_load_records_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_records(tmp_path / "does_not_exist.json", "loans")


def test_cli_writes_combined_loans_and_leaves_raw_data_untouched(tmp_path):
    loans_path = tmp_path / "loans.json"
    customers_path = tmp_path / "customers.json"
    loans_path.write_text(json.dumps(LOANS))
    customers_path.write_text(json.dumps(CUSTOMERS))
    before = loans_path.read_bytes()

    output_path = tmp_path / "scenario" / "loans.json"
    main(
        [
            "--loans-file", str(loans_path),
            "--customers-file", str(customers_path),
            "--output-file", str(output_path),
            "--as-of-date", "2026-07-20",
            "--count", "3",
            "--seed", "7",
        ]
    )

    assert loans_path.read_bytes() == before  # source loans.json (the "raw" stand-in) untouched
    written = json.loads(output_path.read_text())
    assert len(written) == len(LOANS) + 3
    assert written == sorted(written, key=lambda loan: loan["loan_id"])


def test_cli_same_inputs_produce_byte_identical_output(tmp_path):
    loans_path = tmp_path / "loans.json"
    customers_path = tmp_path / "customers.json"
    loans_path.write_text(json.dumps(LOANS))
    customers_path.write_text(json.dumps(CUSTOMERS))

    out1, out2 = tmp_path / "run1.json", tmp_path / "run2.json"
    for out in (out1, out2):
        main(
            [
                "--loans-file", str(loans_path),
                "--customers-file", str(customers_path),
                "--output-file", str(out),
                "--as-of-date", "2026-07-20",
                "--count", "3",
            ]
        )
    assert out1.read_bytes() == out2.read_bytes()


def test_cli_rejects_non_positive_count(tmp_path):
    with pytest.raises(SystemExit):
        main(["--count", "0"])


def test_cli_rejects_invalid_as_of_date(tmp_path):
    with pytest.raises(SystemExit):
        main(["--as-of-date", "not-a-date"])
