"""Answer a business question from trusted, validated portfolio data --
closing the loop from the project's original vision: answer -> detect a bad
answer -> diagnose -> repair -> reverify -> return the corrected answer.

If the target scenario's validation currently PASSES, this answers directly.
If it currently FAILS, this automatically runs diagnosis and the full
self-healing workflow (reusing everything from those milestones unchanged)
before answering. If the incident can't be safely auto-repaired (blocked for
human review, or repaired but not verified), this refuses to fabricate a
confident number -- it reports the data is unreliable and explains why.

This module intentionally does not investigate incidents or propose fixes
itself -- it only orchestrates the existing diagnose/repair/verify pipeline
and the read-only business Q&A agent. See README.md for scope.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Callable

from src.legacy.answer_models import AnswerValidationError, build_unreliable_data_answer, business_answer_to_dict
from src.legacy.apply_repair import DEFAULT_CONFIDENCE_THRESHOLD, DEFAULT_REPAIR_TARGETS_FILE, ApplyRepairError, write_json_file
from src.legacy.business_agent import BusinessAgentError, run_business_qa
from src.legacy.business_tools import BusinessTools
from src.legacy.diagnose_incident import DiagnoseIncidentError, run_diagnose_incident
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.legacy.run_self_healing import run_self_healing
from src.legacy.verify_repair import VerifyRepairError

DEFAULT_MANIFEST_FILE = "data/processed/repair_manifest.json"
DIAGNOSIS_MODEL_ENV_VAR = "DIAGNOSIS_MODEL"
REPAIR_MODEL_ENV_VAR = "REPAIR_MODEL"
ANSWER_MODEL_ENV_VAR = "ANSWER_MODEL"


class AskError(Exception):
    """Application-level failure: missing manifest or base artifacts (not a data-reliability issue)."""


def _load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise AskError(f"{label} file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _authoritative_business_rules_file(manifest: dict) -> str:
    """The currently-approved business rules file for this scenario -- what the agent should cite, not necessarily what a stale ETL used."""
    rerun_cfg = manifest["rerun"]
    return rerun_cfg.get("validation_business_rules_file") or rerun_cfg["business_rules_file"]


def _diagnose_args_from_manifest(manifest: dict, *, output_dir: str) -> argparse.Namespace:
    """Build the argparse.Namespace run_diagnose_incident expects, from a repair manifest."""
    rerun_cfg = manifest["rerun"]
    return argparse.Namespace(
        loans_file=rerun_cfg["loans_file"],
        payments_file=rerun_cfg.get("payments_file"),
        payment_events_file=rerun_cfg.get("payment_events_file"),
        etl_function_name=manifest["etl_function_name"],
        summary_file=manifest["portfolio_summary_file"],
        validation_results_file=manifest["validation_results_file"],
        business_rules_file=_authoritative_business_rules_file(manifest),
        validation_rules_file=rerun_cfg["validation_rules_file"],
        lineage_file=manifest["lineage_file"],
        data_dictionary_file=manifest["data_dictionary_file"],
        pipeline_run_file=manifest["pipeline_run_file"],
        output_dir=output_dir,
    )


def _finalize(answer, self_healing_summary: dict, output_dir: Path) -> dict:
    result = {"answer": business_answer_to_dict(answer), "self_healing": self_healing_summary}
    write_json_file(output_dir / "answer.json", result)
    return result


def answer_question(
    question: str,
    manifest: dict,
    *,
    diagnosis_model_client_factory: Callable[[], DiagnosisModelClient],
    repair_model_client_factory: Callable[[], DiagnosisModelClient],
    answer_model_client_factory: Callable[[], DiagnosisModelClient],
    repair_targets_file: str = DEFAULT_REPAIR_TARGETS_FILE,
    confidence_threshold: str = DEFAULT_CONFIDENCE_THRESHOLD,
) -> dict:
    """Answer a business question, auto-healing the pipeline first if needed. Returns the written result dict."""
    scenario_dir = Path(manifest["diagnosis_file"]).parent
    validation_results = _load_json(Path(manifest["validation_results_file"]), "validation results")

    self_healing_summary = None

    if validation_results.get("overall_status") != "PASS":
        self_healing_summary = {"attempted": True}
        try:
            diag_args = _diagnose_args_from_manifest(manifest, output_dir=str(scenario_dir))
            diagnosis = run_diagnose_incident(diag_args, diagnosis_model_client_factory)
            self_healing_summary["diagnosis_status"] = diagnosis.get("diagnosis_status")
            self_healing_summary["root_cause_category"] = diagnosis.get("root_cause_category")

            outcome = run_self_healing(
                manifest,
                repair_model_client_factory,
                repair_targets_file=repair_targets_file,
                confidence_threshold=confidence_threshold,
                output_dir=scenario_dir,
            )
            self_healing_summary["repair_status"] = outcome["repair_result"]["repair_status"]
            self_healing_summary["verification_status"] = outcome["repair_verification"]["verification_status"]

            if outcome["repair_verification"]["verification_status"] != "VERIFIED":
                reason = (
                    f"Validation failed ({diagnosis.get('root_cause_category', 'unknown cause')}): "
                    f"{diagnosis.get('incident_summary', '')} Automated repair "
                    f"{'was blocked' if outcome['repair_result']['repair_status'] != 'APPLIED' else 'did not verify'} "
                    "-- human review is required before this number can be trusted."
                )
                answer = build_unreliable_data_answer(question, reason)
                return _finalize(answer, self_healing_summary, scenario_dir)

            validation_results = _load_json(Path(manifest["validation_results_file"]), "validation results")
        except (DiagnoseIncidentError, ApplyRepairError, VerifyRepairError) as exc:
            answer = build_unreliable_data_answer(question, f"Could not validate or repair the underlying data: {exc}")
            return _finalize(answer, self_healing_summary, scenario_dir)

    # Data is trustworthy now (either always was, or was just healed) -- answer the question.
    portfolio_summary = _load_json(Path(manifest["portfolio_summary_file"]), "portfolio summary")
    business_rules = _load_json(Path(_authoritative_business_rules_file(manifest)), "business rules")
    data_dictionary = _load_json(Path(manifest["data_dictionary_file"]), "data dictionary")
    known_metrics = set(data_dictionary.get("portfolio_summary", {}).get("fields", {}))

    try:
        tools = BusinessTools(portfolio_summary=portfolio_summary, business_rules=business_rules, data_dictionary=data_dictionary)
        model_client = answer_model_client_factory()
        answer = run_business_qa(question, tools, model_client, known_metric_names=known_metrics)
    except (BusinessAgentError, AnswerValidationError, ModelClientError) as exc:
        answer = build_unreliable_data_answer(question, f"Could not produce a grounded answer: {exc}")

    return _finalize(answer, self_healing_summary, scenario_dir)


def print_result(result: dict) -> None:
    self_healing = result.get("self_healing")
    if self_healing:
        print("Self-healing")
        for key, value in self_healing.items():
            print(f"  {key}: {value}")
        print()

    answer = result["answer"]
    print("Answer")
    print(f"  status: {answer['answer_status']}")
    print(f"  {answer['answer_summary']}")
    if answer["cited_metrics"]:
        print("  cited metrics:")
        for metric in answer["cited_metrics"]:
            print(f"    {metric['metric_name']} = {metric['value']}")
    if answer["caveats"]:
        print("  caveats:")
        for caveat in answer["caveats"]:
            print(f"    - {caveat}")


def parse_args(argv: list = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Answer a business question from trusted, validated portfolio data, self-healing the pipeline first if needed."
    )
    parser.add_argument("question", type=str, help="e.g. 'What is today's total outstanding loan balance?'")
    parser.add_argument("--scenario-manifest-file", type=str, default=DEFAULT_MANIFEST_FILE)
    parser.add_argument("--repair-targets-file", type=str, default=DEFAULT_REPAIR_TARGETS_FILE)
    parser.add_argument("--confidence-threshold", type=str, default=DEFAULT_CONFIDENCE_THRESHOLD, choices=["LOW", "MEDIUM", "HIGH"])
    parser.add_argument("--diagnosis-model", type=str, default=None)
    parser.add_argument("--repair-model", type=str, default=None)
    parser.add_argument("--answer-model", type=str, default=None)
    return parser.parse_args(argv)


def main(argv: list = None) -> None:
    args = parse_args(argv)
    manifest = _load_json(Path(args.scenario_manifest_file), "scenario manifest")

    diagnosis_model = args.diagnosis_model or os.environ.get(DIAGNOSIS_MODEL_ENV_VAR)
    repair_model = args.repair_model or os.environ.get(REPAIR_MODEL_ENV_VAR)
    answer_model = args.answer_model or os.environ.get(ANSWER_MODEL_ENV_VAR)

    def _client(model_name: str | None) -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=model_name) if model_name else OpenAIDiagnosisModelClient()

    try:
        result = answer_question(
            args.question,
            manifest,
            diagnosis_model_client_factory=lambda: _client(diagnosis_model),
            repair_model_client_factory=lambda: _client(repair_model),
            answer_model_client_factory=lambda: _client(answer_model),
            repair_targets_file=args.repair_targets_file,
            confidence_threshold=args.confidence_threshold,
        )
    except AskError as exc:
        print(f"Could not process the question: {exc}")
        raise SystemExit(1)

    print_result(result)
    # UNRELIABLE_DATA/INSUFFICIENT_DATA are honest, successful outcomes, not
    # crashes -- only the AskError branch above warrants a nonzero exit.


if __name__ == "__main__":
    main()
