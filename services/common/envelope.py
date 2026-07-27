"""The event envelope every service wraps its domain events in, and the helper that flattens
a batch of them into one Parquet-writable DataFrame (envelope columns first, then the
payload's own fields -- so Spark/pandas readers see a normal flat schema, not a nested one)."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    schema_version: str
    service: str
    emitted_at: str  # ISO date/datetime string, derived from the underlying record, never wall-clock
    payload: dict[str, Any] = field(default_factory=dict)

    def to_flat_dict(self) -> dict:
        """Envelope columns are underscore-prefixed so they can never collide with a payload
        field of the same name (e.g. payment_events already has its own event_id/event_type;
        without the prefix, flat.update(payload) would silently overwrite the envelope's
        deterministic event_id and domain event_type with the payload's raw ones)."""
        flat = {
            "_event_id": self.event_id,
            "_event_type": self.event_type,
            "_schema_version": self.schema_version,
            "_service": self.service,
            "_emitted_at": self.emitted_at,
        }
        flat.update(self.payload)
        return flat


def deterministic_event_id(service: str, event_type: str, natural_key: str) -> str:
    """A stable, seed-derived event_id -- deterministic so regenerating the same seed
    reproduces byte-identical event_ids, unlike uuid4()."""
    digest = hashlib.sha256(f"{service}:{event_type}:{natural_key}".encode("utf-8")).hexdigest()
    return f"evt_{digest[:24]}"


def events_to_dataframe(events: list[Event]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame()
    return pd.DataFrame([event.to_flat_dict() for event in events])
