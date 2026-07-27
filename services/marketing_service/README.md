# marketing_service

Owns customer acquisition: customer profiles, campaigns, coupon rules, email engagement, and
prequalification offers.

## Contract (v1)

| Event type | Source table | Natural key |
|---|---|---|
| `CustomerProfileObserved` | `customers` | `customer_id` |
| `CampaignCreated` | `campaigns` | `campaign_id` |
| `CouponRuleDefined` | `coupon_rules` | `coupon_rule_id` |
| `EmailSent` / `EmailOpened` / `EmailClicked` | `email_events` | `event_id` |
| `PrequalificationCreated` | `prequal_offers` | `offer_id` |

## Run

```
python3 -m services.marketing_service.main --output local --num-customers 1000
python3 -m services.marketing_service.main --output s3 --seed 42 --num-customers 1000
```

## Docker

```
docker build -f services/marketing_service/Dockerfile -t marketing-service .
docker run --rm --env-file .env marketing-service --output s3 --num-customers 1000
```

Events land at `events/marketing_service/<event_type>/event_date=YYYY-MM-DD/part-0000.parquet`.
Referential integrity with every other service comes from all 6 services sharing the same
`--seed`/`--num-customers`/`--as-of-date` (see `services/common/seeding.py`) -- this service
never calls another service directly.
