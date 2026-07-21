"""Tests for src/validate_lifecycle_raw.py -- schema/enum/referential-integrity
checks over the 12-table lifecycle raw dataset.

Uses small, hand-crafted, fully-connected fixtures (no mocking of the check
logic itself), plus one PASS test against the real generated
data/lifecycle/raw/ dataset.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from src.validate_lifecycle_raw import (
    TABLE_FILENAMES,
    load_business_rules,
    load_lifecycle_tables,
    main,
    validate_lifecycle_raw,
)

BUSINESS_RULES_FILE = Path("context/business_rules.json")
VALIDATION_RULES_FILE = Path("context/validations/lifecycle_raw.json")

VALID_TABLES: dict[str, list[dict]] = {
    "customers": [
        {
            "customer_id": "C1",
            "created_at": "2025-01-01",
            "state": "CA",
            "income_band": "40000_60000",
            "credit_score_band": "680_719",
            "credit_score": 700,
            "risk_segment": "LOW",
        }
    ],
    "campaigns": [
        {
            "campaign_id": "CMP1",
            "name": "Test Campaign",
            "channel": "EMAIL",
            "start_date": "2025-01-01",
            "end_date": "2025-02-01",
            "target_risk_segment": "LOW",
        }
    ],
    "coupon_rules": [
        {
            "coupon_rule_id": "CPN1",
            "coupon_code": "WELCOME10",
            "campaign_id": "CMP1",
            "discount_type": "RATE_DISCOUNT",
            "discount_value": 0.02,
            "valid_from": "2025-01-01",
            "valid_to": "2025-02-01",
        }
    ],
    "email_events": [
        {
            "event_id": "EM1",
            "campaign_id": "CMP1",
            "customer_id": "C1",
            "event_type": "CLICKED",
            "event_timestamp": "2025-01-05",
        }
    ],
    "prequal_offers": [
        {
            "offer_id": "OFF1",
            "customer_id": "C1",
            "campaign_id": "CMP1",
            "coupon_code": "WELCOME10",
            "offer_amount": 5000.0,
            "offer_apr": 0.08,
            "created_at": "2025-01-06",
            "expires_at": "2025-02-05",
        }
    ],
    "applications": [
        {
            "application_id": "APP1",
            "customer_id": "C1",
            "offer_id": "OFF1",
            "requested_amount": 5000.0,
            "submitted_at": "2025-01-07",
            "application_status": "DECISIONED",
        }
    ],
    "underwriting_decisions": [
        {
            "decision_id": "DEC1",
            "application_id": "APP1",
            "decision": "APPROVED",
            "rejection_reason": None,
            "approved_amount": 4800.0,
            "approved_apr": 0.08,
            "model_version": "uw-model-v2",
            "decided_at": "2025-01-08",
        }
    ],
    "loans": [
        {
            "loan_id": "L1",
            "application_id": "APP1",
            "customer_id": "C1",
            "principal_amount": 4800.0,
            "interest_rate": 0.08,
            "term_months": 12,
            "originated_at": "2025-01-10",
            "loan_status": "ACTIVE",
            "scheduled_payment_amount": 400.0,
        }
    ],
    "payment_schedule": [
        {
            "schedule_id": "SCH1",
            "loan_id": "L1",
            "installment_number": 1,
            "due_date": "2025-02-10",
            "scheduled_amount": 400.0,
        }
    ],
    "payment_events": [
        {
            "event_id": "PEV1",
            "schedule_id": "SCH1",
            "loan_id": "L1",
            "event_type": "PAYMENT",
            "payment_date": "2025-02-09",
            "amount": 400.0,
            "payment_status": "PAID",
            "payment_method": "ACH",
        }
    ],
    "delinquency_events": [],
    "defaults": [],
}


def _tables_from(records_by_table: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    return {name: pd.DataFrame(records) for name, records in records_by_table.items()}


@pytest.fixture(scope="module")
def business_rules() -> dict:
    return load_business_rules(BUSINESS_RULES_FILE)


@pytest.fixture(scope="module")
def validation_rules() -> dict:
    with VALIDATION_RULES_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def test_fully_connected_fixture_passes_every_check(business_rules, validation_rules):
    tables = _tables_from(VALID_TABLES)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    assert result["overall_status"] == "PASS"
    assert result["failed_check_count"] == 0
    assert result["total_check_count"] == 37


def test_missing_required_column_fails_schema_check(business_rules, validation_rules):
    broken = copy.deepcopy(VALID_TABLES)
    del broken["campaigns"][0]["channel"]
    tables = _tables_from(broken)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    assert result["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["campaigns_required_columns_present"]["status"] == "FAIL"
    assert "channel" in checks_by_id["campaigns_required_columns_present"]["details"]


def test_invalid_enum_value_fails_enum_check(business_rules, validation_rules):
    broken = copy.deepcopy(VALID_TABLES)
    broken["underwriting_decisions"][0]["decision"] = "MAYBE"
    tables = _tables_from(broken)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    assert result["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["underwriting_decisions_decision_enum_valid"]["status"] == "FAIL"
    assert "MAYBE" in checks_by_id["underwriting_decisions_decision_enum_valid"]["details"]


def test_null_rejection_reason_on_approved_decision_does_not_fail_enum_check(business_rules, validation_rules):
    # VALID_TABLES already has rejection_reason=None on an APPROVED decision -- confirm
    # that null is skipped rather than treated as an invalid enum value.
    tables = _tables_from(VALID_TABLES)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["underwriting_decisions_rejection_reason_enum_valid"]["status"] == "PASS"


def test_dangling_foreign_key_fails_referential_integrity_check(business_rules, validation_rules):
    broken = copy.deepcopy(VALID_TABLES)
    broken["loans"][0]["application_id"] = "APP_DOES_NOT_EXIST"
    tables = _tables_from(broken)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    assert result["overall_status"] == "FAIL"
    checks_by_id = {c["id"]: c for c in result["checks"]}
    check = checks_by_id["loans_application_id_references_applications"]
    assert check["status"] == "FAIL"
    assert "APP_DOES_NOT_EXIST" in check["details"]
    # Everything else is untouched by this one broken row.
    assert checks_by_id["loans_customer_id_references_customers"]["status"] == "PASS"


def test_null_nullable_foreign_key_is_not_an_orphan(business_rules, validation_rules):
    organic = copy.deepcopy(VALID_TABLES)
    organic["prequal_offers"][0]["campaign_id"] = None
    tables = _tables_from(organic)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["prequal_offers_campaign_id_references_campaigns"]["status"] == "PASS"


def test_empty_rare_tables_pass_schema_and_referential_checks(business_rules, validation_rules):
    # delinquency_events/defaults are legitimately empty in VALID_TABLES -- confirm this
    # doesn't spuriously fail (mirrors validate_portfolio.py's empty-df-passes convention).
    tables = _tables_from(VALID_TABLES)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    checks_by_id = {c["id"]: c for c in result["checks"]}
    assert checks_by_id["delinquency_events_required_columns_present"]["status"] == "PASS"
    assert checks_by_id["defaults_loan_id_references_loans"]["status"] == "PASS"


def test_real_generated_lifecycle_dataset_passes(business_rules, validation_rules):
    raw_dir = Path("data/lifecycle/raw")
    if not all((raw_dir / filename).exists() for filename in TABLE_FILENAMES.values()):
        pytest.skip("data/lifecycle/raw/ not generated in this environment")
    tables = load_lifecycle_tables(raw_dir)
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    assert result["overall_status"] == "PASS"
    assert result["failed_check_count"] == 0


def test_cli_against_real_dataset_exits_cleanly(tmp_path):
    raw_dir = Path("data/lifecycle/raw")
    if not all((raw_dir / filename).exists() for filename in TABLE_FILENAMES.values()):
        pytest.skip("data/lifecycle/raw/ not generated in this environment")
    main(["--raw-dir", str(raw_dir), "--output-dir", str(tmp_path)])
    written = json.loads((tmp_path / "lifecycle_raw_validation_results.json").read_text())
    assert written["overall_status"] == "PASS"


def test_cli_raises_system_exit_on_validation_failure(tmp_path):
    broken = copy.deepcopy(VALID_TABLES)
    broken["loans"][0]["loan_status"] = "NOT_A_REAL_STATUS"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    for table_name, filename in TABLE_FILENAMES.items():
        (raw_dir / filename).write_text(json.dumps(broken[table_name]))

    with pytest.raises(SystemExit):
        main(["--raw-dir", str(raw_dir), "--output-dir", str(tmp_path / "out")])
