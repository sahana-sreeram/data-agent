"""Tests for the deterministic, full-lifecycle synthetic data generator."""

from __future__ import annotations

import json
from datetime import date

import pytest

from src.generate_data import CREDIT_SCORE_RANGE_BY_BAND, TABLE_ID_FIELDS, generate_dataset, main
from src.schemas import (
    ApplicationStatus,
    Channel,
    CreditScoreBand,
    Decision,
    DelinquencyBucket,
    DiscountType,
    EmailEventType,
    IncomeBand,
    LoanStatus,
    PaymentEventStatus,
    PaymentEventType,
    PaymentMethod,
    RejectionReason,
    RiskSegment,
)

AS_OF_DATE = date(2026, 7, 20)
AS_OF_DATE_STR = "2026-07-20"
DEFAULT_NUM_CUSTOMERS = 100
DEFAULT_SEED = 42

ALL_TABLES = tuple(TABLE_ID_FIELDS.keys())


@pytest.fixture(scope="module")
def dataset() -> dict[str, list[dict]]:
    raw = generate_dataset(DEFAULT_NUM_CUSTOMERS, DEFAULT_SEED, AS_OF_DATE)
    return {table_name: [r.to_dict() for r in records] for table_name, records in raw.items()}


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


# --- Files, uniqueness, sorting ---


def test_generated_files_are_created_and_contain_json_arrays(tmp_path):
    # A small scale (20 customers) -- some rare tables (delinquencies, defaults) can
    # legitimately be empty here. Non-emptiness of every table is checked separately
    # against the default (100-customer) fixture below.
    main(["--num-customers", "20", "--seed", "1", "--output-dir", str(tmp_path), "--as-of-date", AS_OF_DATE_STR])
    for table_name in ALL_TABLES:
        path = tmp_path / f"{table_name}.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert isinstance(data, list)
    for table_name in ("customers", "campaigns", "email_events", "prequal_offers", "applications", "loans"):
        assert len(json.loads((tmp_path / f"{table_name}.json").read_text())) > 0


def test_all_id_fields_are_unique_per_table(dataset):
    for table_name, id_field in TABLE_ID_FIELDS.items():
        ids = [r[id_field] for r in dataset[table_name]]
        assert len(ids) == len(set(ids)), f"{table_name}.{id_field} has duplicates"


def test_records_are_sorted_by_id_in_written_files(tmp_path):
    main(["--num-customers", "20", "--seed", "2", "--output-dir", str(tmp_path), "--as-of-date", AS_OF_DATE_STR])
    for table_name, id_field in TABLE_ID_FIELDS.items():
        records = json.loads((tmp_path / f"{table_name}.json").read_text())
        ids = [r[id_field] for r in records]
        assert ids == sorted(ids), f"{table_name} not sorted by {id_field}"


# --- Referential integrity across the full lifecycle chain ---


def test_referential_integrity_across_all_tables(dataset):
    customer_ids = {c["customer_id"] for c in dataset["customers"]}
    campaign_ids = {c["campaign_id"] for c in dataset["campaigns"]}
    offer_ids = {o["offer_id"] for o in dataset["prequal_offers"]}
    application_ids = {a["application_id"] for a in dataset["applications"]}
    loan_ids = {l["loan_id"] for l in dataset["loans"]}
    schedule_ids = {s["schedule_id"] for s in dataset["payment_schedule"]}

    for cr in dataset["coupon_rules"]:
        assert cr["campaign_id"] in campaign_ids

    for e in dataset["email_events"]:
        assert e["campaign_id"] in campaign_ids
        assert e["customer_id"] in customer_ids

    for o in dataset["prequal_offers"]:
        assert o["customer_id"] in customer_ids
        if o["campaign_id"] is not None:
            assert o["campaign_id"] in campaign_ids

    for a in dataset["applications"]:
        assert a["customer_id"] in customer_ids
        if a["offer_id"] is not None:
            assert a["offer_id"] in offer_ids

    for d in dataset["underwriting_decisions"]:
        assert d["application_id"] in application_ids

    for l in dataset["loans"]:
        assert l["application_id"] in application_ids
        assert l["customer_id"] in customer_ids

    for s in dataset["payment_schedule"]:
        assert s["loan_id"] in loan_ids

    for e in dataset["payment_events"]:
        assert e["loan_id"] in loan_ids
        if e["schedule_id"] is not None:
            assert e["schedule_id"] in schedule_ids

    for dq in dataset["delinquency_events"]:
        assert dq["loan_id"] in loan_ids

    for df in dataset["defaults"]:
        assert df["loan_id"] in loan_ids


def test_every_underwriting_decision_traces_back_to_a_decisioned_application(dataset):
    applications_by_id = {a["application_id"]: a for a in dataset["applications"]}
    for d in dataset["underwriting_decisions"]:
        assert applications_by_id[d["application_id"]]["application_status"] == ApplicationStatus.DECISIONED.value


def test_every_loan_traces_back_to_an_approved_decision(dataset):
    decisions_by_application = {d["application_id"]: d for d in dataset["underwriting_decisions"]}
    for l in dataset["loans"]:
        decision = decisions_by_application[l["application_id"]]
        assert decision["decision"] == Decision.APPROVED.value


# --- Enum validity ---


def test_customer_enum_values_are_valid(dataset):
    for c in dataset["customers"]:
        assert c["income_band"] in {b.value for b in IncomeBand}
        assert c["credit_score_band"] in {b.value for b in CreditScoreBand}
        assert c["risk_segment"] in {b.value for b in RiskSegment}
        assert len(c["state"]) == 2 and c["state"].isalpha()


def test_customer_credit_score_matches_its_band(dataset):
    for c in dataset["customers"]:
        band = CreditScoreBand(c["credit_score_band"])
        low, high = CREDIT_SCORE_RANGE_BY_BAND[band]
        assert low <= c["credit_score"] <= high


def test_campaign_enum_values_are_valid(dataset):
    for c in dataset["campaigns"]:
        assert c["channel"] in {ch.value for ch in Channel}
        if c["target_risk_segment"] is not None:
            assert c["target_risk_segment"] in {r.value for r in RiskSegment}


def test_coupon_rule_enum_values_are_valid(dataset):
    for cr in dataset["coupon_rules"]:
        assert cr["discount_type"] in {t.value for t in DiscountType}
        assert cr["discount_value"] > 0


def test_email_event_enum_values_are_valid(dataset):
    for e in dataset["email_events"]:
        assert e["event_type"] in {t.value for t in EmailEventType}


def test_application_enum_values_are_valid(dataset):
    for a in dataset["applications"]:
        assert a["application_status"] in {s.value for s in ApplicationStatus}


def test_decision_enum_values_are_valid(dataset):
    for d in dataset["underwriting_decisions"]:
        assert d["decision"] in {dec.value for dec in Decision}
        if d["rejection_reason"] is not None:
            assert d["rejection_reason"] in {r.value for r in RejectionReason}
            assert d["decision"] == Decision.REJECTED.value
        if d["decision"] == Decision.APPROVED.value:
            assert d["approved_amount"] > 0
            assert 0 < d["approved_apr"] < 1


def test_loan_enum_values_are_valid(dataset):
    for l in dataset["loans"]:
        assert l["loan_status"] in {s.value for s in LoanStatus}
        assert l["term_months"] in (12, 24, 36, 48, 60)


def test_payment_event_enum_values_are_valid(dataset):
    for e in dataset["payment_events"]:
        assert e["event_type"] in {t.value for t in PaymentEventType}
        assert e["payment_status"] in {s.value for s in PaymentEventStatus}
        assert e["payment_method"] in {m.value for m in PaymentMethod}
        if e["event_type"] == PaymentEventType.REVERSAL.value:
            assert e["payment_status"] == PaymentEventStatus.REVERSED.value


def test_delinquency_enum_values_are_valid(dataset):
    for dq in dataset["delinquency_events"]:
        assert dq["bucket"] in {b.value for b in DelinquencyBucket}
        assert dq["days_past_due"] > 0


# --- Positive amounts / ranges ---


def test_loan_amounts_are_positive_and_in_range(dataset):
    for l in dataset["loans"]:
        assert l["principal_amount"] > 0
        assert 0 < l["interest_rate"] < 1
        assert l["scheduled_payment_amount"] > 0


def test_payment_schedule_amounts_are_positive(dataset):
    for s in dataset["payment_schedule"]:
        assert s["scheduled_amount"] > 0


def test_offer_amounts_are_positive(dataset):
    for o in dataset["prequal_offers"]:
        assert o["offer_amount"] > 0
        assert 0 < o["offer_apr"] < 1


def test_application_requested_amounts_are_positive(dataset):
    for a in dataset["applications"]:
        assert a["requested_amount"] > 0


def test_default_balances_are_nonnegative(dataset):
    for df in dataset["defaults"]:
        assert df["balance_at_default"] >= 0
        assert df["recovery_amount"] >= 0


# --- Dates: ISO validity + causal ordering ---


def test_all_date_fields_are_valid_iso_strings(dataset):
    for c in dataset["customers"]:
        assert _is_iso_date(c["created_at"])
    for c in dataset["campaigns"]:
        assert _is_iso_date(c["start_date"])
        assert _is_iso_date(c["end_date"])
    for e in dataset["email_events"]:
        assert _is_iso_date(e["event_timestamp"])
    for o in dataset["prequal_offers"]:
        assert _is_iso_date(o["created_at"])
        assert _is_iso_date(o["expires_at"])
    for a in dataset["applications"]:
        assert _is_iso_date(a["submitted_at"])
    for d in dataset["underwriting_decisions"]:
        assert _is_iso_date(d["decided_at"])
    for l in dataset["loans"]:
        assert _is_iso_date(l["originated_at"])
    for s in dataset["payment_schedule"]:
        assert _is_iso_date(s["due_date"])
    for e in dataset["payment_events"]:
        if e["payment_date"] is not None:
            assert _is_iso_date(e["payment_date"])
    for dq in dataset["delinquency_events"]:
        assert _is_iso_date(dq["as_of_date"])
    for df in dataset["defaults"]:
        assert _is_iso_date(df["default_date"])
        if df["recovery_date"] is not None:
            assert _is_iso_date(df["recovery_date"])


def test_nothing_is_dated_after_as_of_date(dataset):
    for l in dataset["loans"]:
        assert l["originated_at"] <= AS_OF_DATE_STR
    for d in dataset["underwriting_decisions"]:
        assert d["decided_at"] <= AS_OF_DATE_STR
    for a in dataset["applications"]:
        assert a["submitted_at"] <= AS_OF_DATE_STR
    for e in dataset["payment_events"]:
        if e["payment_date"] is not None:
            assert e["payment_date"] <= AS_OF_DATE_STR


def test_lifecycle_dates_are_causally_ordered(dataset):
    offers_by_id = {o["offer_id"]: o for o in dataset["prequal_offers"]}
    applications_by_id = {a["application_id"]: a for a in dataset["applications"]}
    decisions_by_application = {d["application_id"]: d for d in dataset["underwriting_decisions"]}

    for a in dataset["applications"]:
        if a["offer_id"] is not None:
            assert a["submitted_at"] >= offers_by_id[a["offer_id"]]["created_at"]

    for d in dataset["underwriting_decisions"]:
        assert d["decided_at"] >= applications_by_id[d["application_id"]]["submitted_at"]

    for l in dataset["loans"]:
        decision = decisions_by_application[l["application_id"]]
        assert l["originated_at"] >= decision["decided_at"]


def test_payment_schedule_installments_increase_after_origination(dataset):
    loans_by_id = {l["loan_id"]: l for l in dataset["loans"]}
    schedule_by_loan: dict[str, list[dict]] = {}
    for s in dataset["payment_schedule"]:
        schedule_by_loan.setdefault(s["loan_id"], []).append(s)

    for loan_id, entries in schedule_by_loan.items():
        entries_sorted = sorted(entries, key=lambda e: e["installment_number"])
        due_dates = [e["due_date"] for e in entries_sorted]
        assert due_dates == sorted(due_dates)
        assert due_dates[0] > loans_by_id[loan_id]["originated_at"]


def test_payment_event_dates_follow_status_rules(dataset):
    schedule_by_id = {s["schedule_id"]: s for s in dataset["payment_schedule"]}
    for e in dataset["payment_events"]:
        if e["event_type"] == PaymentEventType.REVERSAL.value:
            assert e["amount"] < 0
            assert e["payment_date"] is not None
            continue

        due_date = schedule_by_id[e["schedule_id"]]["due_date"]
        status = e["payment_status"]
        if status == PaymentEventStatus.PAID.value:
            assert e["payment_date"] is not None and e["payment_date"] <= due_date
            assert e["amount"] > 0
        elif status == PaymentEventStatus.LATE.value:
            assert e["payment_date"] is not None and e["payment_date"] > due_date
            assert e["amount"] > 0
        elif status == PaymentEventStatus.MISSED.value:
            assert e["payment_date"] is None
            assert e["amount"] == 0
        elif status == PaymentEventStatus.FAILED.value:
            assert e["amount"] == 0


# --- Funnel shrinkage (each stage is a subset of the one before it) ---


def test_funnel_counts_shrink_moving_downstream(dataset):
    sent = sum(1 for e in dataset["email_events"] if e["event_type"] == EmailEventType.SENT.value)
    opened = sum(1 for e in dataset["email_events"] if e["event_type"] == EmailEventType.OPENED.value)
    clicked = sum(1 for e in dataset["email_events"] if e["event_type"] == EmailEventType.CLICKED.value)
    assert sent >= opened >= clicked > 0
    assert len(dataset["underwriting_decisions"]) <= len(dataset["applications"])
    assert len(dataset["loans"]) <= len(dataset["underwriting_decisions"])


def test_default_configuration_hits_target_volume(dataset):
    # Regression guard: the presence checks below only require ">= 1" per category, which
    # would still pass even if a funnel-probability regression collapsed a 100-customer run
    # from dozens of loans down to just one per status. These bounds are wide (generous
    # headroom around the actual current counts -- 28 loans, 154 payment_events at this
    # fixed seed/scale) specifically so routine probability tuning doesn't break this test,
    # while a genuine funnel collapse still would.
    assert 15 <= len(dataset["loans"]) <= 50
    assert 80 <= len(dataset["payment_events"]) <= 300
    assert 20 <= len(dataset["applications"]) <= 90


# --- Presence checks (this scenario -- fixed seed/scale -- is known to produce these) ---


def test_at_least_one_loan_in_each_status(dataset):
    statuses = {l["loan_status"] for l in dataset["loans"]}
    assert LoanStatus.ACTIVE.value in statuses
    assert LoanStatus.CLOSED.value in statuses
    assert LoanStatus.DEFAULTED.value in statuses


def test_at_least_one_payment_event_in_each_outcome(dataset):
    # FAILED is intentionally excluded here -- its probability is only 1-5% by risk
    # segment, so whether it appears at all at this fixed seed/scale is incidental;
    # its shape (amount == 0) is verified generically in
    # test_payment_event_dates_follow_status_rules whenever it does occur.
    statuses = {e["payment_status"] for e in dataset["payment_events"]}
    for status in (
        PaymentEventStatus.PAID,
        PaymentEventStatus.LATE,
        PaymentEventStatus.MISSED,
        PaymentEventStatus.REVERSED,
    ):
        assert status.value in statuses


def test_at_least_one_delinquency_event_and_default(dataset):
    assert len(dataset["delinquency_events"]) > 0
    assert len(dataset["defaults"]) > 0


def test_at_least_one_organic_offer_with_no_campaign(dataset):
    assert any(o["campaign_id"] is None for o in dataset["prequal_offers"])


def test_defaulted_loans_have_late_or_missed_evidence(dataset):
    events_by_loan: dict[str, list[dict]] = {}
    for e in dataset["payment_events"]:
        events_by_loan.setdefault(e["loan_id"], []).append(e)

    defaulted = [l for l in dataset["loans"] if l["loan_status"] == LoanStatus.DEFAULTED.value]
    assert len(defaulted) > 0
    for loan in defaulted:
        loan_events = events_by_loan.get(loan["loan_id"], [])
        assert any(e["payment_status"] in ("LATE", "MISSED") for e in loan_events)


def test_closed_loans_reconcile_to_principal_within_one_cent(dataset):
    events_by_loan: dict[str, list[dict]] = {}
    for e in dataset["payment_events"]:
        events_by_loan.setdefault(e["loan_id"], []).append(e)

    closed = [l for l in dataset["loans"] if l["loan_status"] == LoanStatus.CLOSED.value]
    assert len(closed) > 0
    for loan in closed:
        paid_total = sum(
            e["amount"] for e in events_by_loan[loan["loan_id"]] if e["payment_status"] == PaymentEventStatus.PAID.value
        )
        assert abs(paid_total - loan["principal_amount"]) <= 0.01


# --- Determinism ---


def test_same_seed_produces_identical_output(tmp_path):
    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    for out in (out1, out2):
        main(["--num-customers", "30", "--seed", "5", "--output-dir", str(out), "--as-of-date", AS_OF_DATE_STR])
    for table_name in ALL_TABLES:
        assert (out1 / f"{table_name}.json").read_bytes() == (out2 / f"{table_name}.json").read_bytes()


def test_different_seeds_produce_different_output(tmp_path):
    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    main(["--num-customers", "30", "--seed", "5", "--output-dir", str(out1), "--as-of-date", AS_OF_DATE_STR])
    main(["--num-customers", "30", "--seed", "6", "--output-dir", str(out2), "--as-of-date", AS_OF_DATE_STR])
    assert (out1 / "customers.json").read_bytes() != (out2 / "customers.json").read_bytes()


def test_invalid_num_customers_errors_cleanly():
    with pytest.raises(SystemExit):
        main(["--num-customers", "0"])


def test_invalid_as_of_date_errors_cleanly():
    with pytest.raises(SystemExit):
        main(["--as-of-date", "not-a-date"])
