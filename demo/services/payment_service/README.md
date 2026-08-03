# payment_service

Owns scheduled installments and what actually happened against each one.

## Contract

| Event type | Source table | Natural key |
|---|---|---|
| `PaymentScheduled` | `payment_schedule` | `schedule_id` |
| `PaymentReceived` | `payment_events` (status PAID/SETTLED/LATE/REVERSED) | `event_id` |
| `PaymentFailed` | `payment_events` (status MISSED/FAILED) | `event_id` |

### Two contract versions, on purpose

This is the framework's upstream contract-change incident (see the top-level project plan,
Phase 6):

- **v1 (default)**: a successfully collected installment has `payment_status: "PAID"`.
- **v2**: the exact same installment has `payment_status: "SETTLED"` instead -- a real
  upstream rename, not a bug in this service.

Downstream ETL that only recognizes `"PAID"` (`src/etl_spark_loan_portfolio.py`,
`src/etl_spark_payment_performance.py`) will silently stop counting `SETTLED` installments as
successful once this service runs at v2 -- the Spark job doesn't crash, but outstanding
principal and collection-rate metrics become wrong and validation fails. That's the intended,
diagnosable failure this scenario exists to produce; see `src/eval_scenarios.py`'s
`UpstreamContractScenario`.

## Run

```
python3 -m demo.services.payment_service.main --output local --num-customers 1000                        # v1: PAID
python3 -m demo.services.payment_service.main --output local --num-customers 1000 --contract-version v2  # v2: SETTLED
```

## Docker

```
docker build -f demo/services/payment_service/Dockerfile -t payment-service .
docker run --rm --env-file .env payment-service --output s3 --contract-version v2
```

Events land at `events/payment_service/<event_type>/event_month=YYYY-MM/part-0000.parquet`.
