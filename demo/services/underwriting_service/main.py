"""underwriting_service producer.

    python3 -m services.underwriting_service.main --output local
"""

from __future__ import annotations

from pathlib import Path

from demo.services.common.runner import TableEventSpec, base_arg_parser, print_report, produce_events, write_events
from demo.services.underwriting_service.contract import SCHEMA_VERSION

SPECS = [
    TableEventSpec("underwriting_decisions", "decision_id", lambda p: p["decided_at"], lambda p: "UnderwritingDecisionMade"),
]


def main(argv: list[str] | None = None) -> None:
    args = base_arg_parser("underwriting_service: approve/reject decisions.").parse_args(argv)
    events_by_type = produce_events("underwriting_service", SCHEMA_VERSION, SPECS, args.num_customers, args.seed, args.as_of_date)
    report = write_events(events_by_type, "underwriting_service", args.output, local_dir=Path(args.output_dir))
    print_report("underwriting_service", report)


if __name__ == "__main__":
    main()
