"""Tests for src/events_to_lifecycle_tables.py -- the seam that lets the existing 5 Spark ETL
pipelines run completely unmodified regardless of whether data came from direct generation or
from demo.services. Uses local-dir output (fast, deterministic, no S3 needed); the real S3 path and
a full run through the actual unmodified ETL pipelines was verified manually against live
MinIO (see the project plan's Phase 4 verification notes) rather than in this suite, to avoid
every CI run mutating real curated state.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from demo.services.application_service.contract import SCHEMA_VERSION as APPLICATION_SCHEMA_VERSION
from demo.services.application_service.main import SPECS as APPLICATION_SPECS
from demo.services.common.runner import produce_events, write_events
from demo.services.loan_service.contract import SCHEMA_VERSION as LOAN_SCHEMA_VERSION
from demo.services.loan_service.main import SPECS as LOAN_SPECS
from demo.services.marketing_service.contract import SCHEMA_VERSION as MARKETING_SCHEMA_VERSION
from demo.services.marketing_service.main import SPECS as MARKETING_SPECS
from demo.services.payment_service.contract import SCHEMA_VERSION as PAYMENT_SCHEMA_VERSION
from demo.services.payment_service.main import _build_specs as payment_specs
from demo.services.risk_service.contract import SCHEMA_VERSION as RISK_SCHEMA_VERSION
from demo.services.risk_service.main import SPECS as RISK_SPECS
from demo.services.underwriting_service.contract import SCHEMA_VERSION as UNDERWRITING_SCHEMA_VERSION
from demo.services.underwriting_service.main import SPECS as UNDERWRITING_SPECS
from src.events_to_lifecycle_tables import EVENT_TYPE_TO_TABLE, build_lifecycle_tables_from_events
from src.validate_lifecycle_raw import TABLE_REQUIRED_COLUMNS, validate_lifecycle_raw

NUM_CUSTOMERS = 300  # large enough that every table (including risk_service's) has rows
SEED = 42
AS_OF_DATE = "2026-07-20"


@pytest.fixture(scope="module")
def events_dir(tmp_path_factory) -> Path:
    local_dir = tmp_path_factory.mktemp("events")
    for service_name, schema_version, specs in [
        ("marketing_service", MARKETING_SCHEMA_VERSION, MARKETING_SPECS),
        ("application_service", APPLICATION_SCHEMA_VERSION, APPLICATION_SPECS),
        ("underwriting_service", UNDERWRITING_SCHEMA_VERSION, UNDERWRITING_SPECS),
        ("loan_service", LOAN_SCHEMA_VERSION, LOAN_SPECS),
        ("risk_service", RISK_SCHEMA_VERSION, RISK_SPECS),
        ("payment_service", PAYMENT_SCHEMA_VERSION, payment_specs("v1", NUM_CUSTOMERS, SEED, AS_OF_DATE)),
    ]:
        events_by_type = produce_events(service_name, schema_version, specs, NUM_CUSTOMERS, SEED, AS_OF_DATE)
        write_events(events_by_type, service_name, "local", local_dir=local_dir)
    return local_dir


def test_adapter_reconstructs_every_table(events_dir):
    tables = build_lifecycle_tables_from_events(local_dir=events_dir)
    assert set(tables) == set(EVENT_TYPE_TO_TABLE.values())


def test_adapter_output_has_every_required_column(events_dir):
    tables = build_lifecycle_tables_from_events(local_dir=events_dir)
    for table_name, required_columns in TABLE_REQUIRED_COLUMNS.items():
        assert required_columns.issubset(tables[table_name].columns), table_name


def test_adapter_output_never_contains_envelope_columns(events_dir):
    tables = build_lifecycle_tables_from_events(local_dir=events_dir)
    for table_name, df in tables.items():
        assert not any(c.startswith("_") for c in df.columns), table_name


def test_adapter_output_passes_the_real_raw_validator(events_dir):
    import json

    tables = build_lifecycle_tables_from_events(local_dir=events_dir)
    business_rules = json.loads(Path("context/business_rules.json").read_text())
    validation_rules = json.loads(Path("context/validations/lifecycle_raw.json").read_text())

    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    assert result["overall_status"] == "PASS", [c for c in result["checks"] if c["status"] == "FAIL"]


def test_adapter_referential_integrity_loans_trace_to_real_applications(events_dir):
    tables = build_lifecycle_tables_from_events(local_dir=events_dir)
    application_ids = set(tables["applications"]["application_id"])
    assert set(tables["loans"]["application_id"]).issubset(application_ids)


def test_missing_event_types_degrade_gracefully(tmp_path):
    """No events at all yet (e.g. before any service has run) -- returns an empty dict,
    never raises."""
    tables = build_lifecycle_tables_from_events(local_dir=tmp_path)
    assert tables == {}
