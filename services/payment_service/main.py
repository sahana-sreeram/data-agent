"""payment_service producer.

    python3 -m services.payment_service.main --output local                        # v1: PAID
    python3 -m services.payment_service.main --output local --contract-version v2  # v2: SETTLED
"""

from __future__ import annotations

from pathlib import Path

from services.common.runner import TableEventSpec, base_arg_parser, print_report, produce_events, write_events
from services.common.seeding import generate_shared_dataset
from services.payment_service.contract import SCHEMA_VERSION, apply_contract_version, event_type_for_payment_status


def _build_specs(
    contract_version: str, num_customers: int, seed: int, as_of_date: str, dataset: dict[str, list] | None = None
) -> list[TableEventSpec]:
    # payment_events' payment_date is null for MISSED installments (money never arrived) --
    # fall back to the schedule's due_date so every event still gets a real emitted_at.
    # `dataset`, if given, is used as-is (e.g. an already-namespaced batch from
    # src/generate_upstream_events.py) instead of regenerating via generate_shared_dataset.
    if dataset is None:
        dataset = generate_shared_dataset(num_customers, seed, as_of_date)
    schedule_records = dataset["payment_schedule"]
    due_date_by_schedule_id = {
        (r.schedule_id if hasattr(r, "schedule_id") else r["schedule_id"]): (r.due_date if hasattr(r, "due_date") else r["due_date"])
        for r in schedule_records
    }

    def transform_payment_event(record: dict) -> dict:
        return apply_contract_version(record, contract_version)

    def emitted_at_for_payment_event(payload: dict) -> str:
        # MISSED installments have payment_date=None (money never arrived) -- fall back to
        # the schedule's due_date so every event still gets a real emitted_at, without
        # storing a synthetic field in the payload itself (payload must stay identical to
        # the original payment_events row shape for events_to_lifecycle_tables.py).
        return payload["payment_date"] or due_date_by_schedule_id[payload["schedule_id"]]

    return [
        TableEventSpec("payment_schedule", "schedule_id", lambda p: p["due_date"], lambda p: "PaymentScheduled"),
        TableEventSpec(
            "payment_events",
            "event_id",
            emitted_at_for_payment_event,
            lambda p: event_type_for_payment_status(p["payment_status"]),
            transform_fn=transform_payment_event,
        ),
    ]


def main(argv: list[str] | None = None) -> None:
    parser = base_arg_parser("payment_service: scheduled installments and payment outcomes.")
    parser.add_argument("--contract-version", type=str, choices=["v1", "v2"], default="v1")
    args = parser.parse_args(argv)

    specs = _build_specs(args.contract_version, args.num_customers, args.seed, args.as_of_date)
    events_by_type = produce_events("payment_service", SCHEMA_VERSION, specs, args.num_customers, args.seed, args.as_of_date)
    report = write_events(events_by_type, "payment_service", args.output, local_dir=Path(args.output_dir))
    print_report("payment_service", report)
    print(f"  contract_version: {args.contract_version}")


if __name__ == "__main__":
    main()
