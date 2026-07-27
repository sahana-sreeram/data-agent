from __future__ import annotations

from services.common.runner import produce_events
from services.underwriting_service.contract import SCHEMA_VERSION
from services.underwriting_service.main import SPECS


def test_underwriting_service_produces_decision_events_with_model_version():
    events_by_type = produce_events("underwriting_service", SCHEMA_VERSION, SPECS, 50, 42, "2026-07-20")
    assert set(events_by_type) == {"UnderwritingDecisionMade"}
    for event in events_by_type["UnderwritingDecisionMade"]:
        assert event.payload["decision"] in ("APPROVED", "REJECTED", "MANUAL_REVIEW")
        assert event.payload["model_version"]
