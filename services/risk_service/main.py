"""risk_service producer.

    python3 -m services.risk_service.main --output local
"""

from __future__ import annotations

from pathlib import Path

from services.common.runner import TableEventSpec, base_arg_parser, print_report, produce_events, write_events
from services.risk_service.contract import SCHEMA_VERSION

SPECS = [
    TableEventSpec("delinquency_events", "delinquency_id", lambda p: p["as_of_date"], lambda p: "LoanBecameDelinquent"),
    TableEventSpec("defaults", "default_id", lambda p: p["default_date"], lambda p: "LoanDefaulted"),
]


def main(argv: list[str] | None = None) -> None:
    args = base_arg_parser("risk_service: delinquency and default events.").parse_args(argv)
    events_by_type = produce_events("risk_service", SCHEMA_VERSION, SPECS, args.num_customers, args.seed, args.as_of_date)
    report = write_events(events_by_type, "risk_service", args.output, local_dir=Path(args.output_dir))
    print_report("risk_service", report)


if __name__ == "__main__":
    main()
