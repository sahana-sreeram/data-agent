"""Deterministic orchestration of the transform -> validate pipeline.

Runs src.transform then src.validate_portfolio in-process (no subprocess),
and records what happened -- per-stage status plus an overall pass/fail --
to data/processed/pipeline_run.json. This is the entrypoint a future repair
loop will rerun after fixing an upstream break, to prove the fix actually
restored a passing pipeline.

This module intentionally does not regenerate raw data, repair anything, or
call any agent -- see README.md for scope.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.transform import compute_portfolio_summary, load_business_rules, load_loans, load_payments, write_summary
from src.validate_portfolio import load_summary, load_validation_rules, validate_portfolio, write_validation_results

DEFAULT_LOANS_FILE = "data/raw/loans.json"
DEFAULT_PAYMENTS_FILE = "data/raw/payments.json"
DEFAULT_OUTPUT_DIR = "data/processed"
DEFAULT_AS_OF_DATE = "2026-07-20"
DEFAULT_BUSINESS_RULES_FILE = "context/business_rules.json"
DEFAULT_VALIDATION_RULES_FILE = "context/validation_rules.json"


def run_pipeline(
    loans_file: str,
    payments_file: str,
    output_dir: str,
    as_of_date: str,
    business_rules_file: str,
    validation_rules_file: str,
    *,
    validation_business_rules_file: str | None = None,
) -> dict:
    """Run transform then validation in-process, capturing per-stage outcomes.

    etl_status/validation_status report what happened in each stage;
    overall_status is SUCCESS only if the ETL ran cleanly AND every
    validation check passed. etl_status=SUCCESS means only that the ETL
    executed without raising an error -- it does not by itself mean the
    output is correct; that's exactly what validation_status checks.

    business_rules_file is what the ETL uses. validation_business_rules_file,
    if given, is what the validator independently recomputes against instead
    -- this exists to model a source/business-rule contract change that the
    ETL's last run predates (the ETL itself is not re-run under the new
    rules; only re-validated against them). Defaults to business_rules_file
    when not given, matching prior behavior exactly.
    """
    started_at = datetime.now(timezone.utc).isoformat()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    summary_path = output_path / "portfolio_summary.json"
    validation_path = output_path / "validation_results.json"

    etl_status = "FAILURE"
    etl_error = None
    validation_status = "ERROR"
    validation_error = None
    validation_results = None

    try:
        loans_df = load_loans(Path(loans_file))
        payments_df = load_payments(Path(payments_file))
        business_rules = load_business_rules(Path(business_rules_file))
        summary = compute_portfolio_summary(loans_df, payments_df, as_of_date, business_rules)
        write_summary(summary_path, summary)
        etl_status = "SUCCESS"
    except Exception as exc:  # noqa: BLE001 -- pipeline-status boundary: report, don't crash the orchestrator
        etl_error = str(exc)

    if etl_status == "SUCCESS":
        try:
            validation_rules = load_validation_rules(Path(validation_rules_file))
            summary_for_validation = load_summary(summary_path)
            validation_business_rules = (
                load_business_rules(Path(validation_business_rules_file))
                if validation_business_rules_file
                else business_rules
            )
            validation_results = validate_portfolio(
                loans_df, payments_df, summary_for_validation, validation_business_rules, validation_rules
            )
            write_validation_results(validation_path, validation_results)
            validation_status = validation_results["overall_status"]
        except Exception as exc:  # noqa: BLE001
            validation_error = str(exc)

    overall_status = "SUCCESS" if etl_status == "SUCCESS" and validation_status == "PASS" else "FAILURE"
    completed_at = datetime.now(timezone.utc).isoformat()

    return {
        "as_of_date": as_of_date,
        "run_started_at": started_at,
        "run_completed_at": completed_at,
        "etl_status": etl_status,
        "etl_error": etl_error,
        "validation_status": validation_status,
        "validation_error": validation_error,
        "overall_status": overall_status,
        "artifacts": {
            "portfolio_summary": str(summary_path) if etl_status == "SUCCESS" else None,
            "validation_results": str(validation_path) if validation_results is not None else None,
        },
    }


def write_pipeline_run(path: Path, run_record: dict) -> None:
    """Write the pipeline run record as a single JSON object with a trailing newline."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(run_record, f, indent=2)
        f.write("\n")


def print_pipeline_run(run_record: dict) -> None:
    """Print a human-readable rendering of the pipeline run record."""
    print("Pipeline run")
    for key in ("as_of_date", "etl_status", "validation_status", "overall_status"):
        print(f"  {key:<18} {run_record[key]}")
    if run_record["etl_error"]:
        print(f"  etl_error: {run_record['etl_error']}")
    if run_record["validation_error"]:
        print(f"  validation_error: {run_record['validation_error']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate CLI arguments."""
    parser = argparse.ArgumentParser(description="Run the transform -> validate pipeline end to end.")
    parser.add_argument("--loans-file", type=str, default=DEFAULT_LOANS_FILE)
    parser.add_argument("--payments-file", type=str, default=DEFAULT_PAYMENTS_FILE)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    parser.add_argument("--business-rules-file", type=str, default=DEFAULT_BUSINESS_RULES_FILE)
    parser.add_argument("--validation-rules-file", type=str, default=DEFAULT_VALIDATION_RULES_FILE)
    parser.add_argument(
        "--validation-business-rules-file",
        type=str,
        default=None,
        help="If set, the validator checks against this business-rules file instead of --business-rules-file "
        "(models a rule change the ETL's last run predates). Defaults to --business-rules-file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_record = run_pipeline(
        args.loans_file,
        args.payments_file,
        args.output_dir,
        args.as_of_date,
        args.business_rules_file,
        args.validation_rules_file,
        validation_business_rules_file=args.validation_business_rules_file,
    )

    write_pipeline_run(Path(args.output_dir) / "pipeline_run.json", run_record)
    print_pipeline_run(run_record)

    if run_record["overall_status"] != "SUCCESS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
