"""CLI entrypoint for the diagnosis-agent milestone.

Loads validation_results.json. If validation passed, writes a NO_INCIDENT
diagnosis without ever constructing a model client (no live-model usage for
a clean pipeline). If it failed, builds the read-only investigation tools,
runs the reasoning agent, validates its structured output, and writes
data/processed/diagnosis.json.

This module intentionally does not repair anything, modify code, or rerun
the pipeline -- see README.md for scope.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Callable

import pandas as pd

from src.legacy.diagnosis_agent import DiagnosisAgentError, run_diagnosis
from src.legacy.diagnosis_models import (
    DiagnosisValidationError,
    build_no_incident_diagnosis,
    diagnosis_to_dict,
)
from src.legacy.diagnostic_tools import DEFAULT_ETL_FUNCTION_NAME, ETL_FUNCTIONS_BY_NAME, DiagnosticTools
from src.model_client import (
    DEFAULT_MODEL,
    DEFAULT_TEMPERATURE,
    DiagnosisModelClient,
    ModelClientError,
    OpenAIDiagnosisModelClient,
)
from src.legacy.transform import load_business_rules, load_loans, load_payment_events, load_payments
from src.legacy.validate_portfolio import load_summary, load_validation_rules

DEFAULT_LOANS_FILE = "data/raw/loans.json"
DEFAULT_PAYMENTS_FILE = "data/raw/payments.json"
DEFAULT_SUMMARY_FILE = "data/processed/portfolio_summary.json"
DEFAULT_VALIDATION_RESULTS_FILE = "data/processed/validation_results.json"
DEFAULT_BUSINESS_RULES_FILE = "context/business_rules.json"
DEFAULT_VALIDATION_RULES_FILE = "context/validation_rules.json"
DEFAULT_LINEAGE_FILE = "context/lineage.json"
DEFAULT_DATA_DICTIONARY_FILE = "context/data_dictionary.json"
DEFAULT_PIPELINE_RUN_FILE = "data/processed/pipeline_run.json"
DEFAULT_OUTPUT_DIR = "data/processed"
DIAGNOSIS_MODEL_ENV_VAR = "DIAGNOSIS_MODEL"

# Fixed allowlist of real repository files the model may cite as evidence
# sources or a recommended_fix.target_file. Never derived from model input --
# grows as new scenarios are added, but always a hardcoded set of real paths.
KNOWN_FILE_PATHS = {
    "src/transform.py",
    "src/validate_portfolio.py",
    "src/run_pipeline.py",
    "src/run_payment_events_pipeline.py",
    "src/simulate_upstream_change.py",
    "src/simulate_payment_events_migration.py",
    "context/business_rules.json",
    "context/validation_rules.json",
    "context/lineage.json",
    "context/data_dictionary.json",
    "data/raw/loans.json",
    "data/raw/payments.json",
    "data/scenarios/settled_bug/payments.json",
    "data/scenarios/settled_rule_adopted/business_rules.json",
    "data/scenarios/settled_rule_adopted/pipeline_config.json",
    "data/scenarios/payment_events_cardinality/business_rules.json",
    "data/scenarios/payment_events_cardinality/validation_rules.json",
    "data/scenarios/payment_events_cardinality/payment_events.json",
    "data/scenarios/incorrect_join/loans.json",
    "data/scenarios/incorrect_join/validation_rules.json",
}


class DiagnoseIncidentError(Exception):
    """Application-level failure: missing artifacts, invalid config, model failure, or malformed model output."""


def load_json_object(path: Path, label: str) -> dict:
    if not path.exists():
        raise DiagnoseIncidentError(f"{label} file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_optional_json_object(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_starting_context(validation_results: dict) -> dict:
    """The agent's only starting signal: overall status and failed checks, verbatim. No root cause, no fix."""
    failed_checks = [c for c in validation_results.get("checks", []) if c.get("status") == "FAIL"]
    return {
        "overall_status": validation_results.get("overall_status"),
        "failed_checks": [
            {
                "id": c["id"],
                "description": c.get("description"),
                "expected": c.get("expected"),
                "actual": c.get("actual"),
                "difference": c.get("difference"),
                "details": c.get("details"),
            }
            for c in failed_checks
        ],
    }


def known_metric_names_from(data_dictionary: dict) -> set[str]:
    return set(data_dictionary.get("portfolio_summary", {}).get("fields", {}))


def build_diagnostic_tools(args: argparse.Namespace) -> DiagnosticTools:
    loans_df = load_loans(Path(args.loans_file))
    summary = load_summary(Path(args.summary_file))
    business_rules = load_business_rules(Path(args.business_rules_file))
    validation_results = load_json_object(Path(args.validation_results_file), "validation results")
    validation_rules = load_validation_rules(Path(args.validation_rules_file))
    lineage = load_json_object(Path(args.lineage_file), "lineage")
    data_dictionary = load_json_object(Path(args.data_dictionary_file), "data dictionary")
    pipeline_run = load_optional_json_object(Path(args.pipeline_run_file))

    payment_events_file = getattr(args, "payment_events_file", None)
    if payment_events_file:
        payments_df = pd.DataFrame()
        payment_events_df = load_payment_events(Path(payment_events_file))
    else:
        payments_df = load_payments(Path(args.payments_file))
        payment_events_df = None

    return DiagnosticTools(
        loans_df=loans_df,
        payments_df=payments_df,
        portfolio_summary=summary,
        business_rules=business_rules,
        validation_results=validation_results,
        validation_rules=validation_rules,
        lineage=lineage,
        data_dictionary=data_dictionary,
        pipeline_run=pipeline_run,
        payment_events_df=payment_events_df,
        etl_function_name=getattr(args, "etl_function_name", DEFAULT_ETL_FUNCTION_NAME),
    )


def run_diagnose_incident(
    args: argparse.Namespace, model_client_factory: Callable[[], DiagnosisModelClient]
) -> dict:
    """Core logic. Takes a model_client_factory so tests can inject a fake
    client without ever constructing OpenAIDiagnosisModelClient.
    """
    validation_results = load_json_object(Path(args.validation_results_file), "validation results")

    if validation_results.get("overall_status") == "PASS":
        result = build_no_incident_diagnosis()
    else:
        try:
            tools = build_diagnostic_tools(args)
            starting_context = build_starting_context(validation_results)
            known_metrics = known_metric_names_from(tools.data_dictionary)
            model_client = model_client_factory()
            result = run_diagnosis(
                starting_context,
                tools,
                model_client,
                known_metric_names=known_metrics,
                known_file_paths=KNOWN_FILE_PATHS,
                validation_overall_status=validation_results.get("overall_status"),
            )
        except (FileNotFoundError, ValueError, DiagnosisAgentError, DiagnosisValidationError, ModelClientError) as exc:
            raise DiagnoseIncidentError(str(exc)) from exc

    output_dict = diagnosis_to_dict(result)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "diagnosis.json").open("w", encoding="utf-8") as f:
        json.dump(output_dict, f, indent=2)
        f.write("\n")

    return output_dict


def print_diagnosis(result: dict) -> None:
    print("Diagnosis")
    print(f"  diagnosis_status:    {result['diagnosis_status']}")
    print(f"  incident_summary:    {result['incident_summary']}")
    if result.get("affected_metrics"):
        print(f"  affected_metrics:    {', '.join(result['affected_metrics'])}")
    if result["diagnosis_status"] == "DIAGNOSED":
        print(f"  root_cause_category: {result['root_cause_category']}")
        print(f"  root_cause:          {result['root_cause']}")
        print(f"  confidence:          {result['confidence']}")
        if result.get("recommended_fix"):
            fix = result["recommended_fix"]
            print(f"  recommended_fix:     [{fix['scope']}] {fix['target_file']}: {fix['change_summary']}")
    if result["diagnosis_status"] == "INSUFFICIENT_EVIDENCE":
        print(f"  additional_evidence_needed: {result['additional_evidence_needed']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Diagnose why deterministic validation failed, using a read-only investigation agent."
    )
    parser.add_argument("--loans-file", type=str, default=DEFAULT_LOANS_FILE)
    parser.add_argument("--payments-file", type=str, default=DEFAULT_PAYMENTS_FILE)
    parser.add_argument(
        "--payment-events-file",
        type=str,
        default=None,
        help="If set, diagnose an incident whose source is a payment-EVENTS stream instead of one-row-per-payment "
        "data -- --payments-file is ignored when this is set.",
    )
    parser.add_argument(
        "--etl-function-name",
        type=str,
        default=DEFAULT_ETL_FUNCTION_NAME,
        choices=sorted(ETL_FUNCTIONS_BY_NAME),
        help="Which ETL function get_relevant_etl_source introspects for this incident.",
    )
    parser.add_argument("--summary-file", type=str, default=DEFAULT_SUMMARY_FILE)
    parser.add_argument("--validation-results-file", type=str, default=DEFAULT_VALIDATION_RESULTS_FILE)
    parser.add_argument("--business-rules-file", type=str, default=DEFAULT_BUSINESS_RULES_FILE)
    parser.add_argument("--validation-rules-file", type=str, default=DEFAULT_VALIDATION_RULES_FILE)
    parser.add_argument("--lineage-file", type=str, default=DEFAULT_LINEAGE_FILE)
    parser.add_argument("--data-dictionary-file", type=str, default=DEFAULT_DATA_DICTIONARY_FILE)
    parser.add_argument("--pipeline-run-file", type=str, default=DEFAULT_PIPELINE_RUN_FILE)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--model", type=str, default=None, help=f"Defaults to ${DIAGNOSIS_MODEL_ENV_VAR}, then {DEFAULT_MODEL!r}.")
    parser.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    model_name = args.model or os.environ.get(DIAGNOSIS_MODEL_ENV_VAR)

    def model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=model_name, temperature=args.temperature) if model_name else OpenAIDiagnosisModelClient(temperature=args.temperature)

    try:
        result = run_diagnose_incident(args, model_client_factory)
    except DiagnoseIncidentError as exc:
        print(f"Diagnosis failed: {exc}")
        raise SystemExit(1)

    print_diagnosis(result)
    # Exit successfully even though an incident may have been found -- only
    # application failures (above) warrant a nonzero exit code.


if __name__ == "__main__":
    main()
