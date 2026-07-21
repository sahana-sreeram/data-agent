"""Diagnose why the loan_portfolio lifecycle pipeline's validation failed. Parallel to
src/diagnose_incident.py (left completely unmodified) for the S3-backed lifecycle model.

Unlike the original model, no persisted artifact carries the loan_portfolio validation's
full per-check expected/actual detail (src/run_lifecycle_etl_pipelines.py only stores a
status string in curated/pipeline_run.json) -- so this recomputes it fresh via the
existing, unmodified src.validate_loan_portfolio.validate_loan_portfolio(), a pure
read-only pandas call, rather than inventing a new persisted artifact.
"""

from __future__ import annotations

import argparse
import os

from src.diagnose_incident import build_starting_context
from src.diagnosis_agent import DiagnosisAgentError
from src.diagnosis_models import DiagnosisValidationError, build_no_incident_diagnosis, diagnosis_to_dict
from src.lifecycle_diagnosis_agent import run_lifecycle_diagnosis
from src.lifecycle_diagnostic_tools import LifecycleDiagnosticTools
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.storage import S3Storage
from src.validate_loan_portfolio import validate_loan_portfolio

DIAGNOSIS_MODEL_ENV_VAR = "DIAGNOSIS_MODEL"
DIAGNOSIS_OUTPUT_KEY = "curated/loan_portfolio_diagnosis.json"

# Fixed allowlist of real repository/context files the model may cite as evidence
# sources or a recommended_fix.target_file -- never derived from model input.
KNOWN_FILE_PATHS = {
    "src/etl_spark_loan_portfolio.py",
    "src/validate_loan_portfolio.py",
    "context/metrics/loan_portfolio.json",
    "context/validations/loan_portfolio.json",
    "context/business_rules.json",
}


class DiagnoseLoanPortfolioError(Exception):
    """Application-level failure: missing artifacts, model failure, or malformed model output."""


def known_metric_names_from(metrics: dict) -> set[str]:
    return set(metrics.get("metrics", {}))


def build_lifecycle_diagnostic_tools(
    storage: S3Storage, business_rules: dict, metrics: dict, validation_results: dict
) -> LifecycleDiagnosticTools:
    return LifecycleDiagnosticTools(
        loans=storage.read_parquet("raw/loans.parquet"),
        payment_events=storage.read_parquet("raw/payment_events.parquet"),
        validation_results=validation_results,
        business_rules=business_rules,
        metrics=metrics,
    )


def run_diagnose_loan_portfolio(storage: S3Storage, model_client_factory) -> dict:
    """Core logic. Takes a model_client_factory so tests can inject a fake client."""
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json("context/validations/loan_portfolio.json")
    validation_results = validate_loan_portfolio(storage, business_rules, validation_rules)

    if validation_results["overall_status"] == "PASS":
        result = build_no_incident_diagnosis()
    else:
        try:
            metrics = storage.read_json("context/metrics/loan_portfolio.json")
            tools = build_lifecycle_diagnostic_tools(storage, business_rules, metrics, validation_results)
            starting_context = build_starting_context(validation_results)
            known_metrics = known_metric_names_from(metrics)
            model_client = model_client_factory()
            result = run_lifecycle_diagnosis(
                starting_context,
                tools,
                model_client,
                known_metric_names=known_metrics,
                known_file_paths=KNOWN_FILE_PATHS,
                validation_overall_status=validation_results["overall_status"],
            )
        except (DiagnosisAgentError, DiagnosisValidationError, ModelClientError) as exc:
            raise DiagnoseLoanPortfolioError(str(exc)) from exc

    output_dict = diagnosis_to_dict(result)
    storage.write_json(DIAGNOSIS_OUTPUT_KEY, output_dict)
    return output_dict


def print_diagnosis(result: dict) -> None:
    print("Diagnosis (loan_portfolio)")
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
    parser = argparse.ArgumentParser(description="Diagnose the loan_portfolio lifecycle pipeline.")
    parser.add_argument("--model", type=str, default=None, help=f"Defaults to ${DIAGNOSIS_MODEL_ENV_VAR}.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    model_name = args.model or os.environ.get(DIAGNOSIS_MODEL_ENV_VAR)

    def model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=model_name) if model_name else OpenAIDiagnosisModelClient()

    try:
        storage = S3Storage()
        result = run_diagnose_loan_portfolio(storage, model_client_factory)
    except DiagnoseLoanPortfolioError as exc:
        print(f"Diagnosis failed: {exc}")
        raise SystemExit(1)

    print_diagnosis(result)


if __name__ == "__main__":
    main()
