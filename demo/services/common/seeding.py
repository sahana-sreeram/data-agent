"""Shared deterministic generation for all 6 services.

Referential integrity across independently-run services (a payment references a real loan_id,
a loan references a real application_id, etc.) only holds if every service derives its slice
of data from the exact same (num_customers, seed, as_of_date) generation -- src/generate_data.py's
generate_dataset() is already fully deterministic given those three inputs, so no service needs
to talk to another at generation time: running each service's main.py with the same
--seed/--num-customers/--as-of-date reproduces byte-identical shared entities independently.

This module adds a process-local cache only (useful when multiple services' data is needed in
one process, e.g. the events_to_lifecycle_tables adapter or tests) -- it is not a substitute
for passing the same seed/count/date to every service.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache

from src import generate_data


@lru_cache(maxsize=8)
def generate_shared_dataset(num_customers: int, seed: int, as_of_date: str) -> dict[str, list]:
    """Returns the same dict shape as generate_data.generate_dataset(): table_name -> list of
    dataclass records. Cached per (num_customers, seed, as_of_date) within one process."""
    return generate_data.generate_dataset(num_customers, seed, date.fromisoformat(as_of_date))


# Every ID-like column (primary or foreign key) per table, in the exact dict keys
# record.to_dict() produces -- used by generate_namespaced_batch to make IDs unique
# across independently-generated batches (see src/generate_upstream_events.py). coupon_code
# is deliberately excluded: it's a real, intentionally-reusable code, not a unique entity id
# (see src/generate_data.py's own note on this).
ID_COLUMNS_BY_TABLE: dict[str, list[str]] = {
    "customers": ["customer_id"],
    "campaigns": ["campaign_id"],
    "coupon_rules": ["coupon_rule_id", "campaign_id"],
    "email_events": ["event_id", "campaign_id", "customer_id"],
    "prequal_offers": ["offer_id", "customer_id", "campaign_id"],
    "applications": ["application_id", "customer_id", "offer_id"],
    "underwriting_decisions": ["decision_id", "application_id"],
    "loans": ["loan_id", "application_id", "customer_id"],
    "payment_schedule": ["schedule_id", "loan_id"],
    "payment_events": ["event_id", "schedule_id", "loan_id"],
    "delinquency_events": ["delinquency_id", "loan_id"],
    "defaults": ["default_id", "loan_id"],
}


def _namespace_value(value, batch_prefix: str):
    return f"{batch_prefix}-{value}" if value is not None else value


def generate_namespaced_batch(num_customers: int, seed: int, as_of_date: str, batch_prefix: str) -> dict[str, list[dict]]:
    """Generate one batch (as plain dicts, not dataclass records) with every ID/FK column
    prefixed by `batch_prefix` -- so concatenating many batches (each generated with a
    different seed) never collides IDs, without touching src/generate_data.py's own
    per-call "restart numbering at 1" ID scheme. Used by src/generate_upstream_events.py for
    scale generation; ordinary single-batch service runs never need this."""
    dataset = generate_data.generate_dataset(num_customers, seed, date.fromisoformat(as_of_date))
    namespaced: dict[str, list[dict]] = {}
    for table_name, records in dataset.items():
        id_columns = ID_COLUMNS_BY_TABLE.get(table_name, [])
        batch_records = []
        for record in records:
            row = record.to_dict()
            for column in id_columns:
                if column in row:
                    row[column] = _namespace_value(row[column], batch_prefix)
            batch_records.append(row)
        namespaced[table_name] = batch_records
    return namespaced
