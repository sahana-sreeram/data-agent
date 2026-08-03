from __future__ import annotations

from demo.services.common.runner import produce_events
from demo.services.loan_service.contract import SCHEMA_VERSION
from demo.services.loan_service.main import SPECS


def test_loan_service_produces_loan_funded_events():
    events_by_type = produce_events("loan_service", SCHEMA_VERSION, SPECS, 50, 42, "2026-07-20")
    assert set(events_by_type) == {"LoanFunded"}
    for event in events_by_type["LoanFunded"]:
        assert event.payload["principal_amount"] > 0
