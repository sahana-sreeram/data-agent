# risk_service

Owns loans becoming delinquent and, ultimately, defaulting.

## Contract (v1)

| Event type | Source table | Natural key |
|---|---|---|
| `LoanBecameDelinquent` | `delinquency_events` | `delinquency_id` |
| `LoanDefaulted` | `defaults` | `default_id` |

## Run

```
python3 -m services.risk_service.main --output local --num-customers 1000
python3 -m services.risk_service.main --output s3 --seed 42 --num-customers 1000
```

## Docker

```
docker build -f services/risk_service/Dockerfile -t risk-service .
docker run --rm --env-file .env risk-service --output s3 --num-customers 1000
```

Events land at `events/risk_service/<event_type>/event_month=YYYY-MM/part-0000.parquet`. At
small `--num-customers`, few (or zero) loans reach delinquency/default -- use at least a few
hundred customers to reliably see both event types.
