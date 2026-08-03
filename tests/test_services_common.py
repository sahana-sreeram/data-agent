"""Tests for demo/services/common/{envelope,runner,seeding}.py -- the shared building blocks
every upstream-service producer is built from. No S3/model needed; all local/deterministic."""

from __future__ import annotations

from demo.services.common.envelope import Event, deterministic_event_id, events_to_dataframe
from demo.services.common.runner import TableEventSpec, produce_events
from demo.services.common.seeding import generate_shared_dataset


def test_deterministic_event_id_is_stable_across_calls():
    id1 = deterministic_event_id("loan_service", "LoanFunded", "L000001")
    id2 = deterministic_event_id("loan_service", "LoanFunded", "L000001")
    assert id1 == id2
    assert id1.startswith("evt_")


def test_deterministic_event_id_differs_by_natural_key():
    id1 = deterministic_event_id("loan_service", "LoanFunded", "L000001")
    id2 = deterministic_event_id("loan_service", "LoanFunded", "L000002")
    assert id1 != id2


def test_envelope_columns_never_collide_with_payload_fields():
    """payment_events' own event_id/event_type must survive untouched -- see
    demo/services/common/envelope.py's Event.to_flat_dict docstring for why this matters."""
    event = Event(
        event_id="evt_abc",
        event_type="PaymentReceived",
        schema_version="v1",
        service="payment_service",
        emitted_at="2026-07-20",
        payload={"event_id": "PE00001", "event_type": "PAYMENT", "payment_status": "PAID"},
    )
    flat = event.to_flat_dict()
    assert flat["_event_id"] == "evt_abc"
    assert flat["_event_type"] == "PaymentReceived"
    assert flat["event_id"] == "PE00001"  # the payload's own field, untouched
    assert flat["event_type"] == "PAYMENT"  # the payload's own field, untouched


def test_events_to_dataframe_empty_list():
    df = events_to_dataframe([])
    assert df.empty


def test_generate_shared_dataset_is_deterministic():
    first = generate_shared_dataset(20, 42, "2026-07-20")
    second = generate_shared_dataset(20, 42, "2026-07-20")
    assert [c.customer_id for c in first["customers"]] == [c.customer_id for c in second["customers"]]


def test_produce_events_groups_by_event_type_and_preserves_payload_shape():
    specs = [TableEventSpec("loans", "loan_id", lambda p: p["originated_at"], lambda p: "LoanFunded")]
    events_by_type = produce_events("loan_service", "v1", specs, 20, 42, "2026-07-20")

    assert set(events_by_type) == {"LoanFunded"}
    events = events_by_type["LoanFunded"]
    assert len(events) > 0
    for event in events:
        assert event.service == "loan_service"
        assert event.schema_version == "v1"
        assert "loan_id" in event.payload
        assert "emitted_at" not in event.payload  # envelope field never leaks into the payload
