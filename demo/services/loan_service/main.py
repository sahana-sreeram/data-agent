"""loan_service producer.

    python3 -m services.loan_service.main --output local
"""

from __future__ import annotations

from pathlib import Path

from demo.services.common.runner import TableEventSpec, base_arg_parser, print_report, produce_events, write_events
from demo.services.loan_service.contract import SCHEMA_VERSION

SPECS = [
    TableEventSpec("loans", "loan_id", lambda p: p["originated_at"], lambda p: "LoanFunded"),
]


def main(argv: list[str] | None = None) -> None:
    args = base_arg_parser("loan_service: funded loans.").parse_args(argv)
    events_by_type = produce_events("loan_service", SCHEMA_VERSION, SPECS, args.num_customers, args.seed, args.as_of_date)
    report = write_events(events_by_type, "loan_service", args.output, local_dir=Path(args.output_dir))
    print_report("loan_service", report)


if __name__ == "__main__":
    main()
