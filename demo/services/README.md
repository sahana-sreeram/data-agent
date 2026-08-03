# Upstream services

6 lightweight, deterministic, containerized event producers representing independently-owned
production systems in the lending lifecycle. Each is a batch CLI (not a long-running web app --
see the note below on why), not Kafka: file-based event batches written as partitioned Parquet
are enough to prove the point (deterministic, reproducible, easy to inspect) without the
operational overhead of a message broker this demo doesn't need.

```
demo/services/
├── common/                 shared envelope, deterministic seeding, producer runner
├── marketing_service/      customers, campaigns, coupon rules, email events, prequal offers
├── application_service/    loan applications
├── underwriting_service/   approve/reject/manual-review decisions
├── loan_service/           funded loans
├── payment_service/        scheduled installments + payment outcomes (has 2 contract versions)
└── risk_service/           delinquency + default events
```

## Referential integrity across independently-run services

Every service derives its slice of data from `src/generate_data.py`'s already-deterministic
`generate_dataset(num_customers, seed, as_of_date)` (see `demo/services/common/seeding.py`) --
running every service with the *same* `--seed`/`--num-customers`/`--as-of-date` reproduces
byte-identical shared entities (the same customer_ids, application_ids, loan_ids) independently,
with no service calling another at generation time. This is a deliberate simplification: a real
lending company's services would talk to each other over time; this demo's point is the
data/context layer downstream, not distributed-systems realism (see the project plan's Phase 4
notes for the full reasoning).

## Where the events go

```
s3://<bucket>/events/<service>/<event_type>/event_month=YYYY-MM/part-0000.parquet
```

`src/events_to_lifecycle_tables.py` projects these event batches back into the exact
`raw/*.parquet` table shapes the 5 existing Spark ETL pipelines already read -- verified live
against real MinIO: writing event-sourced `raw/*.parquet` and rerunning
`python3 -m src.run_lifecycle_etl_pipelines` produces `overall_status: SUCCESS` with **zero**
changes to any `etl_spark_*.py` file.

## Running all 6 locally (one-shot, small scale)

```
for svc in marketing application underwriting loan risk; do
  python3 -m demo.services.${svc}_service.main --output s3 --seed 42 --num-customers 1000
done
python3 -m demo.services.payment_service.main --output s3 --seed 42 --num-customers 1000 --contract-version v1
python3 -m src.events_to_lifecycle_tables --from s3
python3 -m src.run_lifecycle_etl_pipelines
```

## Scale generation

For anything bigger than a few thousand customers, use `src.generate_upstream_events`
instead of calling each service directly -- it runs all 6 services together across
bounded-memory batches (see its module docstring for why generation is chunked at all:
`generate_data.generate_dataset()` scales roughly quadratically with customer count, so many
small chunks beat one huge call):

```
python3 -m src.generate_upstream_events --profile small --output local   # 1,000 customers
python3 -m src.generate_upstream_events --profile demo --output s3       # 20,000 customers
python3 -m src.generate_upstream_events --profile large --output s3      # 100,000 customers
python3 -m src.events_to_lifecycle_tables --from s3
python3 -m src.run_lifecycle_etl_pipelines
```

Measured on this machine (10 CPU cores, single process): `demo` (20,000 customers, 5 batches
of 4,000) produced **8.65M events / 396MB** across **2,636 partitioned Parquet files** in
**~101 seconds** (53s generation + 47s write). `large` (100,000 customers, 25 batches) is the
same work 5x over -- expect roughly 8-9 minutes; it's sized to make a laptop's single-node
pandas genuinely impractical, not to run routinely.
