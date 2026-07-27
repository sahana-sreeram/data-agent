# underwriting_service

Owns the approve/reject/manual-review decision for one application, including which model
version made it.

## Contract (v1)

| Event type | Source table | Natural key |
|---|---|---|
| `UnderwritingDecisionMade` | `underwriting_decisions` | `decision_id` |

## Run

```
python3 -m services.underwriting_service.main --output local --num-customers 1000
python3 -m services.underwriting_service.main --output s3 --seed 42 --num-customers 1000
```

## Docker

```
docker build -f services/underwriting_service/Dockerfile -t underwriting-service .
docker run --rm --env-file .env underwriting-service --output s3 --num-customers 1000
```

Events land at `events/underwriting_service/UnderwritingDecisionMade/event_date=YYYY-MM-DD/part-0000.parquet`.
