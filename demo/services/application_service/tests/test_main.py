from __future__ import annotations

from demo.services.application_service.contract import SCHEMA_VERSION
from demo.services.application_service.main import SPECS
from demo.services.common.runner import produce_events


def test_application_service_produces_application_submitted_events():
    events_by_type = produce_events("application_service", SCHEMA_VERSION, SPECS, 50, 42, "2026-07-20")
    assert set(events_by_type) == {"ApplicationSubmitted"}
    for event in events_by_type["ApplicationSubmitted"]:
        assert event.payload["application_status"]
