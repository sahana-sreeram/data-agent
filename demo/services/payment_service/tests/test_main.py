"""The v1 (PAID) vs v2 (SETTLED) contract-version behavior is the upstream contract-change
incident the whole framework's diagnosis story hinges on (see the project plan's Phase 6) --
this file tests it directly rather than relying on the end-to-end scenario alone."""

from __future__ import annotations

from demo.services.common.runner import produce_events
from demo.services.payment_service.contract import SCHEMA_VERSION
from demo.services.payment_service.main import _build_specs


def _statuses(events_by_type, event_type):
    return {event.payload["payment_status"] for event in events_by_type.get(event_type, [])}


def test_v1_contract_never_emits_settled():
    specs = _build_specs("v1", 100, 42, "2026-07-20")
    events_by_type = produce_events("payment_service", SCHEMA_VERSION, specs, 100, 42, "2026-07-20")
    assert "SETTLED" not in _statuses(events_by_type, "PaymentReceived")
    assert "PAID" in _statuses(events_by_type, "PaymentReceived")


def test_v2_contract_relabels_paid_to_settled():
    specs = _build_specs("v2", 100, 42, "2026-07-20")
    events_by_type = produce_events("payment_service", SCHEMA_VERSION, specs, 100, 42, "2026-07-20")
    assert "PAID" not in _statuses(events_by_type, "PaymentReceived")
    assert "SETTLED" in _statuses(events_by_type, "PaymentReceived")


def test_v1_and_v2_produce_the_same_number_of_events():
    """The contract change relabels a field value -- it must never change which
    installments succeeded/failed, only what the successful ones are called."""
    specs_v1 = _build_specs("v1", 100, 42, "2026-07-20")
    specs_v2 = _build_specs("v2", 100, 42, "2026-07-20")
    events_v1 = produce_events("payment_service", SCHEMA_VERSION, specs_v1, 100, 42, "2026-07-20")
    events_v2 = produce_events("payment_service", SCHEMA_VERSION, specs_v2, 100, 42, "2026-07-20")
    assert len(events_v1["PaymentReceived"]) == len(events_v2["PaymentReceived"])
    assert len(events_v1["PaymentScheduled"]) == len(events_v2["PaymentScheduled"])


def test_missed_payment_events_get_a_real_emitted_at_from_the_schedule():
    specs = _build_specs("v1", 100, 42, "2026-07-20")
    events_by_type = produce_events("payment_service", SCHEMA_VERSION, specs, 100, 42, "2026-07-20")
    failed_events = events_by_type.get("PaymentFailed", [])
    assert failed_events, "expected at least one PaymentFailed (MISSED/FAILED) event at this scale"
    for event in failed_events:
        assert event.emitted_at  # never None/empty, even though payment_date itself is null
        assert "emitted_at" not in event.payload  # never leaked into the stored payload
