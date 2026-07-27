# Upstream services

6 lightweight, deterministic, containerized event producers representing independently-owned
production systems in the lending lifecycle. Each is a batch CLI (not a long-running web app --
see the note below on why), not Kafka: file-based event batches written as partitioned Parquet
are enough to prove the point (deterministic, reproducible, easy to inspect) without the
operational overhead of a message broker this demo doesn't need.

```
services/
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
`generate_dataset(num_customers, seed, as_of_date)` (see `services/common/seeding.py`) --
running every service with the *same* `--seed`/`--num-customers`/`--as-of-date` reproduces
byte-identical shared entities (the same customer_ids, application_ids, loan_ids) independently,
with no service calling another at generation time. This is a deliberate simplification: a real
lending company's services would talk to each other over time; this demo's point is the
data/context layer downstream, not distributed-systems realism (see the project plan's Phase 4
notes for the full reasoning).

## Where the events go

```
s3://<bucket>/events/<service>/<event_type>/event_date=YYYY-MM-DD/part-0000.parquet
```

`src/events_to_lifecycle_tables.py` projects these event batches back into the exact
`raw/*.parquet` table shapes the 5 existing Spark ETL pipelines already read -- verified live
against real MinIO: writing event-sourced `raw/*.parquet` and rerunning
`python3 -m src.run_lifecycle_etl_pipelines` produces `overall_status: SUCCESS` with **zero**
changes to any `etl_spark_*.py` file.

## Running all 6 locally

```
for svc in marketing application underwriting loan risk; do
  python3 -m services.${svc}_service.main --output s3 --seed 42 --num-customers 1000
done
python3 -m services.payment_service.main --output s3 --seed 42 --num-customers 1000 --contract-version v1
python3 -m src.events_to_lifecycle_tables --from s3
python3 -m src.run_lifecycle_etl_pipelines
```
