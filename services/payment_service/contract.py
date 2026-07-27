"""payment_service's data contract: scheduled installments and what actually happened against
each one.

Event types: PaymentScheduled, PaymentReceived, PaymentFailed.

Two contract versions of PaymentReceived's payload, on purpose -- this is the upstream
contract-change incident (see the project plan's Phase 6):

    v1 (default): a successfully collected installment has payment_status="PAID".
    v2:           the same installment has payment_status="SETTLED" instead -- a real
                   upstream rename, not a bug in this service. Downstream ETL that only
                   recognizes "PAID" (src/etl_spark_loan_portfolio.py,
                   src/etl_spark_payment_performance.py) will silently stop counting these
                   as successful once payment_service runs at v2, which is exactly the
                   diagnosable failure this scenario exists to produce.

PaymentFailed covers MISSED/FAILED installments; REVERSED and LATE installments still count
as PaymentReceived (money did arrive, possibly late or since corrected) in both versions.
"""

from __future__ import annotations

SCHEMA_VERSION = "v1"

FAILED_STATUSES = {"MISSED", "FAILED"}


def event_type_for_payment_status(payment_status: str) -> str:
    return "PaymentFailed" if payment_status in FAILED_STATUSES else "PaymentReceived"


def apply_contract_version(record: dict, contract_version: str) -> dict:
    """v1: pass through unchanged. v2: relabel a successfully collected PAID installment to
    SETTLED -- the exact upstream rename this scenario models. Every other status
    (LATE/MISSED/FAILED/REVERSED) is untouched in both versions."""
    if contract_version == "v2" and record["payment_status"] == "PAID":
        record = {**record, "payment_status": "SETTLED"}
    return record
