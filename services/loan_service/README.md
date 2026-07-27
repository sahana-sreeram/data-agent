# loan_service

Owns a loan reaching funding: principal, interest rate, term, and scheduled payment amount.

## Contract (v1)

| Event type | Source table | Natural key |
|---|---|---|
| `LoanFunded` | `loans` | `loan_id` |

## Run

```
python3 -m services.loan_service.main --output local --num-customers 1000
python3 -m services.loan_service.main --output s3 --seed 42 --num-customers 1000
```

## Docker

```
docker build -f services/loan_service/Dockerfile -t loan-service .
docker run --rm --env-file .env loan-service --output s3 --num-customers 1000
```

Events land at `events/loan_service/LoanFunded/event_date=YYYY-MM-DD/part-0000.parquet`.
