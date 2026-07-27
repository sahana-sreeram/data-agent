"""Shared building blocks for the 6 upstream-service producers: a common event envelope,
deterministic seeding (shared across services so referential integrity holds -- see
seeding.py's docstring for why), and a producer runner that turns a table of records into a
partitioned Parquet event batch. Each service's own main.py is a thin declaration of which
tables it owns and how a record maps to an event type; everything else lives here.
"""
