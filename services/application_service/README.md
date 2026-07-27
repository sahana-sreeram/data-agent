# application_service

Owns loan applications submitted against a prequalification offer (or organically, with no
offer).

## Contract (v1)

| Event type | Source table | Natural key |
|---|---|---|
| `ApplicationSubmitted` | `applications` | `application_id` |

## Run

```
python3 -m services.application_service.main --output local --num-customers 1000
python3 -m services.application_service.main --output s3 --seed 42 --num-customers 1000
```

## Docker

```
docker build -f services/application_service/Dockerfile -t application-service .
docker run --rm --env-file .env application-service --output s3 --num-customers 1000
```

Events land at `events/application_service/ApplicationSubmitted/event_month=YYYY-MM/part-0000.parquet`.
