"""Diagnose why one of the 5 lifecycle pipelines' validation failed. Parallel to
src/diagnose_incident.py (left completely unmodified) for the S3-backed lifecycle model,
generalized (via src/lifecycle_pipeline_registry.py) to work for any of the 5 pipelines
instead of being hardcoded to loan_portfolio.

Unlike the original model, no persisted artifact carries a pipeline's validation's full
per-check expected/actual detail (src/run_lifecycle_etl_pipelines.py only stores a status
string in curated/pipeline_run.json) -- so this recomputes it fresh via the registry's
run_validate closure (always the real, unmodified validate_*.py function), a pure
read-only pandas call, rather than inventing a new persisted artifact.

This function is pure (returns data, does not write to S3) -- src/lifecycle_run_self_healing.py
is responsible for persisting diagnosis artifacts as part of a full self-healing run; this
module's own main() persists a "latest" convenience copy only when run standalone.
"""

from __future__ import annotations

import argparse
import os

from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.legacy.diagnose_incident import build_starting_context
from src.legacy.diagnosis_agent import DiagnosisAgentError
from src.legacy.diagnosis_models import DiagnosisValidationError, build_no_incident_diagnosis, diagnosis_to_dict
from src.lifecycle_diagnosis_agent import run_lifecycle_diagnosis
from src.lifecycle_diagnostic_tools import build_diagnostic_tools_for_pipeline
from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.storage import S3Storage
from src.validate_lifecycle_raw import TABLE_FILENAMES, validate_lifecycle_raw

DIAGNOSIS_MODEL_ENV_VAR = "DIAGNOSIS_MODEL"


class DiagnosePipelineError(Exception):
    """Application-level failure: missing artifacts, model failure, or malformed model output."""


def known_metric_names_from(metrics: dict) -> set[str]:
    return set(metrics.get("metrics", {}))


def known_file_paths_for(pipeline_name: str) -> set[str]:
    """Fixed allowlist of real repository/context files the model may cite as evidence
    sources or a recommended_fix.target_file, derived from the pipeline's registry entry."""
    spec = PIPELINE_REGISTRY[pipeline_name]
    return {
        spec.etl_source_file,
        f"src/validate_{pipeline_name}.py",
        spec.metrics_key,
        spec.validation_rules_key,
        "context/business_rules.json",
    }


def _raw_validation_status(storage: S3Storage, business_rules: dict) -> dict | None:
    """The 12-table raw validator (schema/enum/referential-integrity, independent of any one
    curated pipeline) catches a class of failure curated reconciliation checks structurally
    cannot: a genuine upstream contract change where the ETL and its own validator apply the
    identical (now-wrong) filter to the identical data and simply agree with each other --
    reconciliation only ever compares them to EACH OTHER, never to an external source of
    truth like an approved enum vocabulary. Returns None if the raw tables aren't fully
    available (defensive -- a missing-table error here should never block diagnosing a
    pipeline whose real problem is unrelated)."""
    try:
        tables = {name: storage.read_parquet(f"raw/{name}.parquet") for name in TABLE_FILENAMES}
        validation_rules = storage.read_json("context/validations/lifecycle_raw.json")
    except Exception:  # noqa: BLE001 -- any read failure here just means "can't check this", not "diagnosis should crash"
        return None
    return validate_lifecycle_raw(tables, business_rules, validation_rules)


def run_diagnose_pipeline(pipeline_name: str, storage: S3Storage, model_client_factory) -> dict:
    """Core logic. Takes a model_client_factory so tests can inject a fake client. Returns
    the diagnosis dict; does not write anything to S3 (see module docstring)."""
    spec = PIPELINE_REGISTRY[pipeline_name]
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json(spec.validation_rules_key)
    validation_results = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

    if validation_results["overall_status"] == "PASS":
        # Curated validation passing doesn't guarantee the raw data itself is healthy -- see
        # _raw_validation_status's docstring. Fall back to it before concluding there's
        # genuinely no incident; every existing ETL-logic/business-rule-mismatch scenario
        # already fails curated validation directly and never reaches this branch.
        raw_validation = _raw_validation_status(storage, business_rules)
        if raw_validation is None or raw_validation["overall_status"] == "PASS":
            return diagnosis_to_dict(build_no_incident_diagnosis())
        validation_results = raw_validation

    try:
        context_retriever = ContextRetriever(store=FileContextStore())
        tools = build_diagnostic_tools_for_pipeline(
            pipeline_name, storage, validation_results, business_rules, context_retriever=context_retriever
        )
        starting_context = build_starting_context(validation_results)
        known_metrics = known_metric_names_from(tools.metrics)
        model_client = model_client_factory()
        result = run_lifecycle_diagnosis(
            starting_context,
            tools,
            model_client,
            known_metric_names=known_metrics,
            known_file_paths=known_file_paths_for(pipeline_name),
            validation_overall_status=validation_results["overall_status"],
        )
    except (DiagnosisAgentError, DiagnosisValidationError, ModelClientError) as exc:
        raise DiagnosePipelineError(str(exc)) from exc

    return diagnosis_to_dict(result)


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
    parser = argparse.ArgumentParser(description="Diagnose one of the 5 lifecycle pipelines.")
    parser.add_argument("pipeline_name", choices=sorted(PIPELINE_REGISTRY))
    parser.add_argument("--model", type=str, default=None, help=f"Defaults to ${DIAGNOSIS_MODEL_ENV_VAR}.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    model_name = args.model or os.environ.get(DIAGNOSIS_MODEL_ENV_VAR)

    def model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=model_name) if model_name else OpenAIDiagnosisModelClient()

    try:
        storage = S3Storage()
        result = run_diagnose_pipeline(args.pipeline_name, storage, model_client_factory)
    except DiagnosePipelineError as exc:
        print(f"Diagnosis failed: {exc}")
        raise SystemExit(1)

    storage.write_json(f"curated/{args.pipeline_name}_diagnosis.json", result)
    print_diagnosis(result)


if __name__ == "__main__":
    main()
