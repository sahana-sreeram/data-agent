"""application_service producer.

    python3 -m services.application_service.main --output local
"""

from __future__ import annotations

from pathlib import Path

from services.application_service.contract import SCHEMA_VERSION
from services.common.runner import TableEventSpec, base_arg_parser, print_report, produce_events, write_events

SPECS = [
    TableEventSpec("applications", "application_id", lambda p: p["submitted_at"], lambda p: "ApplicationSubmitted"),
]


def main(argv: list[str] | None = None) -> None:
    args = base_arg_parser("application_service: loan applications.").parse_args(argv)
    events_by_type = produce_events("application_service", SCHEMA_VERSION, SPECS, args.num_customers, args.seed, args.as_of_date)
    report = write_events(events_by_type, "application_service", args.output, local_dir=Path(args.output_dir))
    print_report("application_service", report)


if __name__ == "__main__":
    main()
