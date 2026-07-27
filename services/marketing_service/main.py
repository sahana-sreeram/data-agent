"""marketing_service producer.

    python3 -m services.marketing_service.main --output local
    python3 -m services.marketing_service.main --output s3 --seed 42 --num-customers 1000
"""

from __future__ import annotations

from pathlib import Path

from services.common.runner import TableEventSpec, base_arg_parser, print_report, produce_events, write_events
from services.marketing_service.contract import EMAIL_EVENT_TYPE_MAP, SCHEMA_VERSION

SPECS = [
    TableEventSpec("customers", "customer_id", lambda p: p["created_at"], lambda p: "CustomerProfileObserved"),
    TableEventSpec("campaigns", "campaign_id", lambda p: p["start_date"], lambda p: "CampaignCreated"),
    TableEventSpec("coupon_rules", "coupon_rule_id", lambda p: p["valid_from"], lambda p: "CouponRuleDefined"),
    TableEventSpec("email_events", "event_id", lambda p: p["event_timestamp"], lambda p: EMAIL_EVENT_TYPE_MAP[p["event_type"]]),
    TableEventSpec("prequal_offers", "offer_id", lambda p: p["created_at"], lambda p: "PrequalificationCreated"),
]


def main(argv: list[str] | None = None) -> None:
    args = base_arg_parser("marketing_service: customers, campaigns, coupon rules, email engagement, prequal offers.").parse_args(argv)
    events_by_type = produce_events("marketing_service", SCHEMA_VERSION, SPECS, args.num_customers, args.seed, args.as_of_date)
    report = write_events(events_by_type, "marketing_service", args.output, local_dir=Path(args.output_dir))
    print_report("marketing_service", report)


if __name__ == "__main__":
    main()
