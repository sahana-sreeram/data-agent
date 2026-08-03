from __future__ import annotations

from demo.services.common.runner import produce_events
from demo.services.marketing_service.contract import SCHEMA_VERSION
from demo.services.marketing_service.main import SPECS


def test_marketing_service_produces_expected_event_types():
    events_by_type = produce_events("marketing_service", SCHEMA_VERSION, SPECS, 50, 42, "2026-07-20")
    assert "CampaignCreated" in events_by_type
    assert "CustomerProfileObserved" in events_by_type
    assert len(events_by_type["CustomerProfileObserved"]) == 50


def test_marketing_service_is_deterministic():
    first = produce_events("marketing_service", SCHEMA_VERSION, SPECS, 20, 42, "2026-07-20")
    second = produce_events("marketing_service", SCHEMA_VERSION, SPECS, 20, 42, "2026-07-20")
    assert [e.event_id for e in first["CampaignCreated"]] == [e.event_id for e in second["CampaignCreated"]]
