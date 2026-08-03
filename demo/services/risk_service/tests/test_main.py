from __future__ import annotations

from demo.services.common.runner import produce_events
from demo.services.risk_service.contract import SCHEMA_VERSION
from demo.services.risk_service.main import SPECS


def test_risk_service_produces_delinquency_and_default_events_at_sufficient_scale():
    # A handful of customers rarely produces any delinquencies/defaults -- use enough
    # customers that the underlying generator's delinquency/default rates are exercised.
    events_by_type = produce_events("risk_service", SCHEMA_VERSION, SPECS, 300, 42, "2026-07-20")
    assert "LoanBecameDelinquent" in events_by_type
    assert "LoanDefaulted" in events_by_type
    for event in events_by_type["LoanDefaulted"]:
        assert event.payload["balance_at_default"] > 0
