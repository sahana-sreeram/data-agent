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
