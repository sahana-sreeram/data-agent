# loan_service

Owns a loan reaching funding: principal, interest rate, term, and scheduled payment amount.

## Contract (v1)

| Event type | Source table | Natural key |
|---|---|---|
| `LoanFunded` | `loans` | `loan_id` |

## Run

```
python3 -m demo.services.loan_service.main --output local --num-customers 1000
python3 -m demo.services.loan_service.main --output s3 --seed 42 --num-customers 1000
```

## Docker

```
docker build -f demo/services/loan_service/Dockerfile -t loan-service .
docker run --rm --env-file .env loan-service --output s3 --num-customers 1000
```

Events land at `events/loan_service/LoanFunded/event_month=YYYY-MM/part-0000.parquet`.
