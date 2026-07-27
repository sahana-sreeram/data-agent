"""Deterministic orchestration of the incorrect-join ETL -> validate pipeline.

Mirrors src/run_pipeline.py exactly in structure and output shape, but calls
the deliberately buggy transform.compute_portfolio_summary_with_payment_join
and validates with validate_portfolio.validate_portfolio_with_join_profile
instead of the one-row-per-payment pair. Kept as a separate module, rather
than branching inside run_pipeline.py, so the original (well-tested)
pipeline is at zero risk from this addition.

This module intentionally does not regenerate raw data, repair anything, or
call any agent -- see README.md for scope.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.legacy.transform import (
    compute_portfolio_summary_with_payment_join,
    load_business_rules,
    load_loans,
    load_payments,
    write_summary,
)
from src.legacy.validate_portfolio import (
    load_summary,
    load_validation_rules,
    validate_portfolio_with_join_profile,
    write_validation_results,
)

DEFAULT_LOANS_FILE = "data/scenarios/incorrect_join/loans.json"
DEFAULT_PAYMENTS_FILE = "data/raw/payments.json"
DEFAULT_OUTPUT_DIR = "data/scenarios/incorrect_join"
DEFAULT_AS_OF_DATE = "2026-07-20"
DEFAULT_BUSINESS_RULES_FILE = "context/business_rules.json"
DEFAULT_VALIDATION_RULES_FILE = "data/scenarios/incorrect_join/validation_rules.json"


def run_incorrect_join_pipeline(
    loans_file: str,
    payments_file: str,
    output_dir: str,
    as_of_date: str,
    business_rules_file: str,
    validation_rules_file: str,
) -> dict:
    """Run the join-based ETL then validation in-process, capturing per-stage outcomes.

    Same return shape and semantics as run_pipeline.run_pipeline: etl_status
    reflects only whether the ETL executed without error, never whether its
    output is correct -- that's what validation_status is for.
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
        summary = compute_portfolio_summary_with_payment_join(loans_df, payments_df, as_of_date, business_rules)
        write_summary(summary_path, summary)
        etl_status = "SUCCESS"
    except Exception as exc:  # noqa: BLE001 -- pipeline-status boundary: report, don't crash the orchestrator
        etl_error = str(exc)

    if etl_status == "SUCCESS":
        try:
            validation_rules = load_validation_rules(Path(validation_rules_file))
            summary_for_validation = load_summary(summary_path)
            validation_results = validate_portfolio_with_join_profile(
                loans_df, payments_df, summary_for_validation, business_rules, validation_rules
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
    with path.open("w", encoding="utf-8") as f:
        json.dump(run_record, f, indent=2)
        f.write("\n")


def print_pipeline_run(run_record: dict) -> None:
    print("Incorrect-join pipeline run")
    for key in ("as_of_date", "etl_status", "validation_status", "overall_status"):
        print(f"  {key:<18} {run_record[key]}")
    if run_record["etl_error"]:
        print(f"  etl_error: {run_record['etl_error']}")
    if run_record["validation_error"]:
        print(f"  validation_error: {run_record['validation_error']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the incorrect-join ETL -> validate pipeline end to end.")
    parser.add_argument("--loans-file", type=str, default=DEFAULT_LOANS_FILE)
    parser.add_argument("--payments-file", type=str, default=DEFAULT_PAYMENTS_FILE)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    parser.add_argument("--business-rules-file", type=str, default=DEFAULT_BUSINESS_RULES_FILE)
    parser.add_argument("--validation-rules-file", type=str, default=DEFAULT_VALIDATION_RULES_FILE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_record = run_incorrect_join_pipeline(
        args.loans_file,
        args.payments_file,
        args.output_dir,
        args.as_of_date,
        args.business_rules_file,
        args.validation_rules_file,
    )

    write_pipeline_run(Path(args.output_dir) / "pipeline_run.json", run_record)
    print_pipeline_run(run_record)

    if run_record["overall_status"] != "SUCCESS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
