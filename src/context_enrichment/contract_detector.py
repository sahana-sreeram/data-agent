"""Detects which version of an upstream service's data contract is actually present in raw
data, so context can be invalidated/regenerated when it changes -- see
src.context_retriever.ContextRetriever.ensure_fresh.

payment_service is the only service with more than one contract version today (see
services/payment_service/contract.py): v1 labels a successfully collected installment
payment_status="PAID"; v2 relabels the same installment "SETTLED". Detection reads the raw
data directly rather than trusting any config, since the whole point is to notice when the
data no longer matches what context was generated against.
"""

from __future__ import annotations

from src.storage import S3Storage

PAYMENT_SERVICE_V2_STATUS = "SETTLED"


def detect_payment_service_contract_version(storage: S3Storage) -> str:
    """"v2" if any raw payment_events row carries the v2-only "SETTLED" status, else "v1".
    Defensive: returns "v1" (the default/original contract) if the raw table isn't available
    yet -- a missing table here should never crash context enrichment."""
    if not storage.exists("raw/payment_events.parquet"):
        return "v1"
    payment_events = storage.read_parquet("raw/payment_events.parquet")
    if "payment_status" not in payment_events.columns:
        return "v1"
    return "v2" if (payment_events["payment_status"] == PAYMENT_SERVICE_V2_STATUS).any() else "v1"
