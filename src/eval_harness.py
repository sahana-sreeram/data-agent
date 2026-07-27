"""Repeatable evals for the lifecycle self-healing pipeline: inject a known failure, run the
real diagnose -> repair -> verify -> promote flow, and score it against six dimensions --
diagnosis success, tool-call efficiency, repair success, refusal accuracy, latency, and
failure by bug class -- instead of relying on ad hoc, one-off live-fire testing.

Two distinct failure shapes are covered: BugScenario (a surgical find/replace bug injected
into a real etl_spark_*.py source file -- see run_bug_scenario) and UpstreamContractScenario
(a genuine upstream contract change -- payment_service emitting SETTLED instead of PAID --
where no ETL file is ever touched; only the raw data payment_service produces changes -- see
run_upstream_contract_scenario). The latter is the reason run_diagnose_pipeline
(src/lifecycle_diagnose_pipeline.py) falls back to the 12-table raw validator when curated
reconciliation passes: reconciliation alone cannot catch a case where the ETL and its own
validator apply the identical (now-wrong) filter to the identical data and simply agree with
each other.

Purely additive: this module only CALLS the existing generalized lifecycle pipeline
(src/lifecycle_diagnose_pipeline.py, src/lifecycle_apply_repair.py,
src/lifecycle_verify_repair.py, src/repair_models.py) -- it does not modify any agent loop,
tool, or model client, and never touches src/apply_repair.py, src/diagnosis_agent.py,
src/repair_agent.py, or any of the original 3-scenario files.

Each scenario run is a real, repeatable benchmark, not a permanent fix: regardless of whether
the repair verifies and promotes, the harness always restores whatever it changed (a
BugScenario's target ETL file, or an UpstreamContractScenario's affected raw tables) to its
pre-scenario bytes and reruns that pipeline's clean ETL afterward, so running the suite never
leaves a lasting change behind and the system is left fully healthy between scenarios.
"""

from __future__ import annotations

import importlib
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.eval_scenarios import BUG_SCENARIOS, REFUSAL_CASES, UPSTREAM_CONTRACT_SCENARIOS, BugScenario, UpstreamContractScenario
from src.lifecycle_apply_repair import run_apply_lifecycle_repair
from src.lifecycle_diagnose_pipeline import run_diagnose_pipeline
from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.lifecycle_run_self_healing import _persist_run_artifacts
from src.lifecycle_verify_repair import run_verify_lifecycle_repair
from src.model_client import DiagnosisModelClient, OpenAIDiagnosisModelClient, OpenAIResponsesModelClient
from src.legacy.repair_models import evaluate_repair_eligibility
from src.spark_session import get_spark_session
from src.storage import S3Storage

DIAGNOSIS_MODEL_ENV_VAR = "DIAGNOSIS_MODEL"
REPAIR_MODEL_ENV_VAR = "REPAIR_MODEL"
PIPELINE_RUN_KEY = "curated/pipeline_run.json"
EVAL_SPARK_APP_NAME = "lifecycle-eval-harness"


class EvalScenarioError(Exception):
    """Raised when a BugScenario's `find` string doesn't match its real target file exactly
    once -- the scenario has drifted out of sync with the real ETL source it targets."""


@dataclass
class ClientStats:
    turns_used: int = 0
    tool_calls_used: int = 0
    total_latency_seconds: float = 0.0


class InstrumentedModelClient:
    """Wraps any DiagnosisModelClient to record turns, total tool calls, and latency across
    a single agent-loop run, with zero changes to model_client.py or any agent loop -- the
    wrapped client is still called exactly the way it always is; this just observes."""

    def __init__(self, wrapped: DiagnosisModelClient) -> None:
        self._wrapped = wrapped
        self.stats = ClientStats()

    def send(self, messages: list[dict], tools: list[dict]):
        start = time.monotonic()
        try:
            response = self._wrapped.send(messages, tools)
        finally:
            self.stats.total_latency_seconds += time.monotonic() - start
        self.stats.turns_used += 1
        self.stats.tool_calls_used += len(response.tool_calls)
        return response


def _ensure_spark_session(spark):
    """run_verify_lifecycle_repair's targeted-test rerun invokes pytest.main() in-process
    (src.lifecycle_verify_repair._run_pytest_against_patched_code); if the pipeline's test
    file pulls in the session-scoped `spark_session` fixture from tests/conftest.py, that
    fixture's teardown stops the shared local SparkContext -- and since local[*] means one
    SparkContext per JVM, that kills every Spark operation still to come in THIS process too,
    not just the pytest sub-run's own. get_spark_session()'s SparkSession.builder.getOrCreate()
    transparently starts a fresh context when the active one has been stopped, so this is a
    cheap no-op when spark is still alive and a real recovery when it isn't."""
    try:
        stopped = spark.sparkContext._jsc.sc().isStopped()
    except Exception:  # noqa: BLE001 -- any failure inspecting a possibly-dead context means "assume dead, get a fresh one"
        stopped = True
    if not stopped:
        return spark
    spark = get_spark_session(EVAL_SPARK_APP_NAME)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _reload_etl_module(target_file: str):
    module_name = target_file.replace("/", ".").removesuffix(".py")
    if module_name in sys.modules:
        return importlib.reload(sys.modules[module_name])
    return importlib.import_module(module_name)


def _restore_pipeline_run_entry(storage: S3Storage, pipeline_name: str, validation_status: str) -> None:
    pipeline_run = storage.read_json(PIPELINE_RUN_KEY) if storage.exists(PIPELINE_RUN_KEY) else {"pipelines": {}}
    pipeline_run.setdefault("pipelines", {})[pipeline_name] = {
        "etl_status": "SUCCESS",
        "etl_error": None,
        "validation_status": validation_status,
        "validation_error": None,
    }
    pipeline_run["overall_status"] = (
        "SUCCESS"
        if all(
            r.get("etl_status") == "SUCCESS" and r.get("validation_status") == "PASS"
            for r in pipeline_run["pipelines"].values()
        )
        else "FAILURE"
    )
    storage.write_json(PIPELINE_RUN_KEY, pipeline_run)


def run_bug_scenario(
    scenario: BugScenario,
    spark,
    storage: S3Storage,
    diagnosis_model_client_factory,
    repair_model_client_factory,
) -> dict:
    """Inject scenario's bug into its real target file, run that pipeline's ETL to produce
    genuinely broken curated output, run the full diagnose -> repair -> verify flow against
    it, then ALWAYS restore the target file and rerun a clean ETL -- regardless of outcome.
    Returns a scenario-result dict; raises on an application-level failure (a model/API
    error, a missing artifact) after the file has already been safely restored."""
    spec = PIPELINE_REGISTRY[scenario.pipeline_name]
    spark = _ensure_spark_session(spark)
    business_rules = storage.read_json("context/business_rules.json")
    target_path = Path(scenario.target_file)
    original_source = target_path.read_text()

    occurrences = original_source.count(scenario.find)
    if occurrences != 1:
        raise EvalScenarioError(
            f"scenario {scenario.name!r}: expected exactly 1 occurrence of its `find` string in "
            f"{scenario.target_file}, found {occurrences} -- the scenario has drifted out of sync "
            f"with the real ETL source"
        )

    try:
        target_path.write_text(original_source.replace(scenario.find, scenario.replace, 1))
        buggy_module = _reload_etl_module(scenario.target_file)
        broken_outputs = spec.run_etl(buggy_module, spark, business_rules, DEFAULT_AS_OF_DATE)
        for key, df in broken_outputs.items():
            storage.write_parquet(key, df)

        diagnosis_client = InstrumentedModelClient(diagnosis_model_client_factory())
        diagnosis_start = time.monotonic()
        diagnosis = run_diagnose_pipeline(scenario.pipeline_name, storage, lambda: diagnosis_client)
        diagnosis_latency = time.monotonic() - diagnosis_start

        validation_rules = storage.read_json(spec.validation_rules_key)
        validation_before = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

        repair_client = InstrumentedModelClient(repair_model_client_factory())
        repair_start = time.monotonic()
        repair_plan, repair_result = run_apply_lifecycle_repair(
            scenario.pipeline_name, storage, diagnosis, validation_before, lambda: repair_client
        )
        repair_latency = time.monotonic() - repair_start

        run_id = uuid.uuid4().hex[:12]
        verify_start = time.monotonic()
        repair_verification = run_verify_lifecycle_repair(
            scenario.pipeline_name,
            spark,
            storage,
            business_rules,
            validation_rules,
            validation_before,
            repair_result,
            run_id=run_id,
        )
        verify_latency = time.monotonic() - verify_start

        _persist_run_artifacts(
            storage,
            scenario.pipeline_name,
            run_id,
            {
                "diagnosis": diagnosis,
                "repair_plan": repair_plan,
                "repair_result": repair_result,
                "repair_verification": repair_verification,
            },
        )
    finally:
        target_path.write_text(original_source)
        clean_module = _reload_etl_module(scenario.target_file)
        spark = _ensure_spark_session(spark)
        clean_outputs = spec.run_etl(clean_module, spark, business_rules, DEFAULT_AS_OF_DATE)
        for key, df in clean_outputs.items():
            storage.write_parquet(key, df)
        clean_validation_rules = storage.read_json(spec.validation_rules_key)
        clean_validation = spec.run_validate(storage, business_rules, clean_validation_rules, DEFAULT_AS_OF_DATE)
        _restore_pipeline_run_entry(storage, scenario.pipeline_name, clean_validation["overall_status"])

    return {
        "scenario_name": scenario.name,
        "pipeline_name": scenario.pipeline_name,
        "bug_class": scenario.bug_class,
        "error": None,
        "diagnosis": {
            "status": diagnosis["diagnosis_status"],
            "root_cause_category": diagnosis.get("root_cause_category"),
            "confidence": diagnosis.get("confidence"),
            "matches_expected": diagnosis.get("root_cause_category") == scenario.expected_root_cause_category,
            "turns_used": diagnosis_client.stats.turns_used,
            "tool_calls_used": diagnosis_client.stats.tool_calls_used,
            "latency_seconds": diagnosis_latency,
        },
        "repair": {
            "repair_status": repair_result["repair_status"],
            "repair_decision": repair_plan["repair_decision"],
            "turns_used": repair_client.stats.turns_used,
            "latency_seconds": repair_latency,
        },
        "verify": {
            "verification_status": repair_verification["verification_status"],
            "promoted": repair_verification["verification_status"] == "VERIFIED",
            "latency_seconds": verify_latency,
        },
        "end_to_end_latency_seconds": diagnosis_latency + repair_latency + verify_latency,
    }


def run_upstream_contract_scenario(
    scenario: UpstreamContractScenario,
    spark,
    storage: S3Storage,
    diagnosis_model_client_factory,
    repair_model_client_factory,
) -> dict:
    """Regenerates raw/payment_schedule.parquet + raw/payment_events.parquet for real, via the
    real payment_service running at scenario.contract_version and the real
    events_to_lifecycle_tables adapter, then runs the same diagnose -> repair -> verify flow
    as run_bug_scenario -- but the injected difference is in the DATA, never in any
    etl_spark_*.py file. Per policy, evaluate_repair_eligibility refuses SOURCE_CONTRACT_CHANGE
    regardless of confidence (src/legacy/repair_models.py), so repair_result is always expected
    to come back BLOCKED here -- this scenario proves the diagnosis traces the failure back to
    the upstream service, not that it gets auto-repaired. ALWAYS restores the original raw
    tables and reruns clean ETL afterward, regardless of outcome."""
    from services.common.envelope import events_to_dataframe
    from services.common.runner import produce_events
    from services.payment_service.main import _build_specs
    from src.events_to_lifecycle_tables import EVENT_TYPE_TO_TABLE, _strip_envelope

    spec = PIPELINE_REGISTRY[scenario.pipeline_name]
    spark = _ensure_spark_session(spark)
    business_rules = storage.read_json("context/business_rules.json")

    affected_raw_tables = ["payment_schedule", "payment_events"]
    backup_prefix = f"_backup/eval_upstream_contract/{uuid.uuid4().hex[:8]}/"
    for table in affected_raw_tables:
        storage.copy_or_promote(f"raw/{table}.parquet", f"{backup_prefix}raw/{table}.parquet")

    try:
        # Built entirely in memory -- never touches the real events/ S3 prefix, so this
        # scenario can't pollute real upstream-service event history.
        payment_specs = _build_specs(scenario.contract_version, scenario.num_customers, scenario.seed, DEFAULT_AS_OF_DATE)
        events_by_type = produce_events(
            "payment_service", "v1", payment_specs, scenario.num_customers, scenario.seed, DEFAULT_AS_OF_DATE
        )
        by_table: dict[str, list[pd.DataFrame]] = {}
        for event_type, events in events_by_type.items():
            table_name = EVENT_TYPE_TO_TABLE[event_type]
            by_table.setdefault(table_name, []).append(_strip_envelope(events_to_dataframe(events)))
        reconstructed = {
            name: pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0] for name, parts in by_table.items()
        }
        for table in affected_raw_tables:
            storage.write_parquet(f"raw/{table}.parquet", reconstructed[table])

        module = _reload_etl_module(spec.etl_source_file)
        broken_outputs = spec.run_etl(module, spark, business_rules, DEFAULT_AS_OF_DATE)
        for key, df in broken_outputs.items():
            storage.write_parquet(key, df)

        diagnosis_client = InstrumentedModelClient(diagnosis_model_client_factory())
        diagnosis_start = time.monotonic()
        diagnosis = run_diagnose_pipeline(scenario.pipeline_name, storage, lambda: diagnosis_client)
        diagnosis_latency = time.monotonic() - diagnosis_start

        validation_rules = storage.read_json(spec.validation_rules_key)
        validation_before = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

        repair_client = InstrumentedModelClient(repair_model_client_factory())
        repair_start = time.monotonic()
        repair_plan, repair_result = run_apply_lifecycle_repair(
            scenario.pipeline_name, storage, diagnosis, validation_before, lambda: repair_client
        )
        repair_latency = time.monotonic() - repair_start

        run_id = uuid.uuid4().hex[:12]
        verify_start = time.monotonic()
        repair_verification = run_verify_lifecycle_repair(
            scenario.pipeline_name, spark, storage, business_rules, validation_rules, validation_before, repair_result, run_id=run_id
        )
        verify_latency = time.monotonic() - verify_start

        _persist_run_artifacts(
            storage,
            scenario.pipeline_name,
            run_id,
            {"diagnosis": diagnosis, "repair_plan": repair_plan, "repair_result": repair_result, "repair_verification": repair_verification},
        )
    finally:
        for table in affected_raw_tables:
            storage.copy_or_promote(f"{backup_prefix}raw/{table}.parquet", f"raw/{table}.parquet")
            storage.delete(f"{backup_prefix}raw/{table}.parquet")
        clean_module = _reload_etl_module(spec.etl_source_file)
        spark = _ensure_spark_session(spark)
        clean_outputs = spec.run_etl(clean_module, spark, business_rules, DEFAULT_AS_OF_DATE)
        for key, df in clean_outputs.items():
            storage.write_parquet(key, df)
        clean_validation_rules = storage.read_json(spec.validation_rules_key)
        clean_validation = spec.run_validate(storage, business_rules, clean_validation_rules, DEFAULT_AS_OF_DATE)
        _restore_pipeline_run_entry(storage, scenario.pipeline_name, clean_validation["overall_status"])

    return {
        "scenario_name": scenario.name,
        "pipeline_name": scenario.pipeline_name,
        "bug_class": "SOURCE_CONTRACT_CHANGE",
        "error": None,
        "diagnosis": {
            "status": diagnosis["diagnosis_status"],
            "root_cause_category": diagnosis.get("root_cause_category"),
            "confidence": diagnosis.get("confidence"),
            "matches_expected": diagnosis.get("root_cause_category") == scenario.expected_root_cause_category,
            "turns_used": diagnosis_client.stats.turns_used,
            "tool_calls_used": diagnosis_client.stats.tool_calls_used,
            "latency_seconds": diagnosis_latency,
        },
        "repair": {
            "repair_status": repair_result["repair_status"],
            "repair_decision": repair_plan["repair_decision"],
            "turns_used": repair_client.stats.turns_used,
            "latency_seconds": repair_latency,
        },
        "verify": {
            "verification_status": repair_verification["verification_status"],
            "promoted": repair_verification["verification_status"] == "VERIFIED",
            "latency_seconds": verify_latency,
        },
        "end_to_end_latency_seconds": diagnosis_latency + repair_latency + verify_latency,
    }


def _precision_recall(expected: set, actual: set) -> dict:
    true_positives = len(expected & actual)
    precision = true_positives / len(actual) if actual else (1.0 if not expected else 0.0)
    recall = true_positives / len(expected) if expected else 1.0
    return {
        "precision": precision,
        "recall": recall,
        "f1": (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0,
        "expected": sorted(expected),
        "actual": sorted(actual),
        "missing": sorted(expected - actual),
        "extra": sorted(actual - expected),
    }


# Deterministic ground truth for src.context_enrichment.code_enricher's structural
# extraction, measured directly against each pipeline's real ETL source (see the project
# plan's Phase 9/Ongoing notes) -- precision/recall against known-correct joins and
# business-rule references, not an LLM judge's opinion of whether the extraction "looks
# right." A join is (right_side_table, tuple(on_columns), how); empty for pipelines whose
# real ETL genuinely has no src.join() call our regex targets (delinquency_default,
# payment_performance aggregate/filter rather than join).
CONTEXT_EXTRACTION_GROUND_TRUTH: dict[str, dict] = {
    "loan_portfolio": {"joins": {("net_paid_by_loan", ("loan_id",), "left")}, "business_rule_references": {"interest_accrual", "successful_payment_statuses"}},
    "delinquency_default": {"joins": set(), "business_rule_references": {"loss_rate_denominator"}},
    "payment_performance": {"joins": set(), "business_rule_references": {"prepayment_threshold_days", "successful_payment_statuses"}},
    "campaign_funnel": {
        "joins": {
            ("app_attribution", ("application_id",), "inner"),
            ("emails_by_campaign", ("campaign_key",), "left"),
            ("offers_by_campaign", ("campaign_key",), "left"),
            ("applications_by_campaign", ("campaign_key",), "left"),
            ("approved_by_campaign", ("campaign_key",), "left"),
            ("loans_by_campaign", ("campaign_key",), "left"),
        },
        "business_rule_references": set(),
    },
    "underwriting_performance": {"joins": {("app_customer", ("application_id",), "inner")}, "business_rule_references": set()},
}


def score_context_extraction() -> dict:
    """Precision/recall of the deterministic structural extractor
    (src.context_enrichment.code_enricher) against known-correct ground truth for every
    registered pipeline's real ETL source. No model call -- this measures the code_enricher
    itself, which is also what Codex enrichment's structural baseline relies on."""
    from src.context_enrichment.code_enricher import enrich_pipeline_structurally

    by_pipeline = {}
    for pipeline_name, ground_truth in CONTEXT_EXTRACTION_GROUND_TRUTH.items():
        metadata = enrich_pipeline_structurally(pipeline_name)
        actual_joins = {(j.right, tuple(j.on), j.how) for j in metadata.joins}
        by_pipeline[pipeline_name] = {
            "joins": _precision_recall(ground_truth["joins"], actual_joins),
            "business_rule_references": _precision_recall(ground_truth["business_rule_references"], set(metadata.business_rule_lookups)),
        }

    overall_f1 = _mean(
        [by_pipeline[p][kind]["f1"] for p in by_pipeline for kind in ("joins", "business_rule_references")]
    )
    return {"by_pipeline": by_pipeline, "overall_f1": overall_f1}


PR_ARTIFACT_REQUIRED_FIELDS = (
    "run_id", "pipeline_name", "target_file", "diff", "diagnosis_summary", "root_cause_category",
    "failed_checks_before", "tests_status", "metrics_after", "risk_classification", "human_review_required",
)


def score_pr_artifact_completeness(pr_artifact: dict | None) -> dict:
    """Whether a create_pr-mode result actually contains everything the project's Part 6
    deliverable list calls for: branch/diff, diagnosis summary, root cause, failed checks,
    tests run, metrics, risk classification, human-review flag. None (no artifact -- e.g. the
    repair was BLOCKED before verification) is reported distinctly from an incomplete one."""
    if pr_artifact is None:
        return {"complete": False, "present": False, "missing_fields": list(PR_ARTIFACT_REQUIRED_FIELDS)}
    missing = [field for field in PR_ARTIFACT_REQUIRED_FIELDS if pr_artifact.get(field) is None]
    return {"complete": not missing, "present": True, "missing_fields": missing}


def run_refusal_accuracy_suite() -> dict:
    """Deterministic (no model, no Spark) accuracy check against the real, production
    repair-eligibility gate (src.repair_models.evaluate_repair_eligibility) -- the actual
    decision point that refuses to let an incident reach the repair model."""
    cases = []
    correct = 0
    for name, diagnosis, allowed_targets, expected_decision in REFUSAL_CASES:
        decision = evaluate_repair_eligibility(diagnosis, allowed_target_files=allowed_targets)
        is_correct = decision.decision == expected_decision
        correct += int(is_correct)
        cases.append(
            {
                "name": name,
                "expected_decision": expected_decision.value,
                "actual_decision": decision.decision.value,
                "correct": is_correct,
                "reasons": decision.reasons,
            }
        )
    return {"cases": cases, "accuracy": correct / len(cases) if cases else 1.0}


def _rate(flags: list[bool]) -> float:
    return sum(flags) / len(flags) if flags else 0.0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _scenario_flags(results: list[dict]) -> dict:
    diagnosis_ok, repair_ok, end_to_end_ok, tool_calls = [], [], [], []
    latencies = {"diagnosis": [], "repair": [], "verify": [], "end_to_end": []}
    for r in results:
        if r.get("error"):
            diagnosis_ok.append(False)
            repair_ok.append(False)
            end_to_end_ok.append(False)
            continue
        diagnosis, repair, verify = r["diagnosis"], r["repair"], r["verify"]
        diagnosis_ok.append(diagnosis["status"] == "DIAGNOSED" and diagnosis["matches_expected"])
        repair_ok.append(repair["repair_status"] == "APPLIED")
        end_to_end_ok.append(verify["promoted"])
        tool_calls.append(diagnosis["tool_calls_used"])
        latencies["diagnosis"].append(diagnosis["latency_seconds"])
        latencies["repair"].append(repair["latency_seconds"])
        latencies["verify"].append(verify["latency_seconds"])
        latencies["end_to_end"].append(r["end_to_end_latency_seconds"])
    return {
        "scenario_count": len(results),
        "diagnosis_success_rate": _rate(diagnosis_ok),
        "repair_success_rate": _rate(repair_ok),
        "end_to_end_success_rate": _rate(end_to_end_ok),
        "avg_tool_calls_per_diagnosis": _mean(tool_calls),
        "avg_latency_seconds": {k: _mean(v) for k, v in latencies.items()},
    }


def _summarize(scenario_results: list[dict], refusal: dict) -> dict:
    summary = _scenario_flags(scenario_results)
    summary["refusal_accuracy"] = refusal["accuracy"]
    summary["by_bug_class"] = {
        bug_class: _scenario_flags([r for r in scenario_results if r["bug_class"] == bug_class])
        for bug_class in sorted({r["bug_class"] for r in scenario_results})
    }
    return summary


def run_evals(
    storage: S3Storage,
    spark,
    *,
    scenarios: list[BugScenario] | None = None,
    upstream_contract_scenarios: list[UpstreamContractScenario] | None = None,
    diagnosis_model: str | None = None,
    repair_model: str | None = None,
) -> dict:
    scenarios = BUG_SCENARIOS if scenarios is None else scenarios
    upstream_contract_scenarios = UPSTREAM_CONTRACT_SCENARIOS if upstream_contract_scenarios is None else upstream_contract_scenarios

    def diagnosis_model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=diagnosis_model) if diagnosis_model else OpenAIDiagnosisModelClient()

    def repair_model_client_factory() -> DiagnosisModelClient:
        return OpenAIResponsesModelClient(model=repair_model) if repair_model else OpenAIResponsesModelClient()

    scenario_results = []
    for scenario in scenarios:
        spark = _ensure_spark_session(spark)
        try:
            result = run_bug_scenario(scenario, spark, storage, diagnosis_model_client_factory, repair_model_client_factory)
        except Exception as exc:  # noqa: BLE001 -- a scenario-level application failure is a reportable outcome, not a harness crash
            result = {
                "scenario_name": scenario.name,
                "pipeline_name": scenario.pipeline_name,
                "bug_class": scenario.bug_class,
                "error": str(exc),
                "diagnosis": None,
                "repair": None,
                "verify": None,
                "end_to_end_latency_seconds": None,
            }
        scenario_results.append(result)

    for scenario in upstream_contract_scenarios:
        spark = _ensure_spark_session(spark)
        try:
            result = run_upstream_contract_scenario(scenario, spark, storage, diagnosis_model_client_factory, repair_model_client_factory)
        except Exception as exc:  # noqa: BLE001 -- a scenario-level application failure is a reportable outcome, not a harness crash
            result = {
                "scenario_name": scenario.name,
                "pipeline_name": scenario.pipeline_name,
                "bug_class": "SOURCE_CONTRACT_CHANGE",
                "error": str(exc),
                "diagnosis": None,
                "repair": None,
                "verify": None,
                "end_to_end_latency_seconds": None,
            }
        scenario_results.append(result)

    refusal = run_refusal_accuracy_suite()
    context_extraction = score_context_extraction()
    pr_artifact_scores = {
        r["scenario_name"]: score_pr_artifact_completeness((r.get("verify") or {}).get("pr_artifact"))
        for r in scenario_results
        if not r.get("error")
    }
    return {
        "scenarios": scenario_results,
        "refusal_accuracy": refusal,
        "context_extraction": context_extraction,
        "pr_artifact_scores": pr_artifact_scores,
        "summary": _summarize(scenario_results, refusal),
    }


def print_eval_report(report: dict) -> None:
    summary = report["summary"]
    print("Eval report")
    print(f"  scenarios run:            {summary['scenario_count']}")
    print(f"  diagnosis_success_rate:   {summary['diagnosis_success_rate']:.0%}")
    print(f"  repair_success_rate:      {summary['repair_success_rate']:.0%}")
    print(f"  end_to_end_success_rate:  {summary['end_to_end_success_rate']:.0%}")
    print(f"  refusal_accuracy:         {summary['refusal_accuracy']:.0%}")
    print(f"  avg_tool_calls_per_diag:  {summary['avg_tool_calls_per_diagnosis']:.1f}")
    lat = summary["avg_latency_seconds"]
    print(
        f"  avg_latency (s):          diagnosis={lat['diagnosis']:.1f} repair={lat['repair']:.1f} "
        f"verify={lat['verify']:.1f} end_to_end={lat['end_to_end']:.1f}"
    )
    print("  by bug class:")
    for bug_class, stats in summary["by_bug_class"].items():
        print(
            f"    {bug_class:<24} diagnosis={stats['diagnosis_success_rate']:.0%} "
            f"repair={stats['repair_success_rate']:.0%} end_to_end={stats['end_to_end_success_rate']:.0%} "
            f"(n={stats['scenario_count']})"
        )
    print("  per-scenario detail:")
    for r in report["scenarios"]:
        if r.get("error"):
            print(f"    {r['scenario_name']:<40} ERROR: {r['error']}")
        else:
            print(
                f"    {r['scenario_name']:<40} diagnosis={r['diagnosis']['status']}/{r['diagnosis']['root_cause_category']} "
                f"repair={r['repair']['repair_status']} verify={r['verify']['verification_status']}"
            )
    if "context_extraction" in report:
        print(f"  context_extraction overall_f1: {report['context_extraction']['overall_f1']:.2f}")
        for pipeline_name, scores in report["context_extraction"]["by_pipeline"].items():
            print(
                f"    {pipeline_name:<26} joins_f1={scores['joins']['f1']:.2f} "
                f"business_rule_refs_f1={scores['business_rule_references']['f1']:.2f}"
            )
    if report.get("pr_artifact_scores"):
        print("  pr_artifact_scores:")
        for scenario_name, score in report["pr_artifact_scores"].items():
            print(f"    {scenario_name:<40} present={score['present']} complete={score['complete']}")
    print("  refusal accuracy cases:")
    for c in report["refusal_accuracy"]["cases"]:
        mark = "OK" if c["correct"] else "WRONG"
        print(f"    [{mark}] {c['name']:<40} expected={c['expected_decision']} actual={c['actual_decision']}")


def parse_args(argv: list[str] | None = None):
    import argparse

    parser = argparse.ArgumentParser(description="Run the lifecycle self-healing eval harness against real S3/Spark.")
    parser.add_argument(
        "--scenario",
        action="append",
        dest="scenario_names",
        default=None,
        choices=[s.name for s in BUG_SCENARIOS] + [s.name for s in UPSTREAM_CONTRACT_SCENARIOS],
        help="Run only these scenarios (repeatable). Default: all, including the upstream-contract scenario.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    from src.spark_session import get_spark_session

    args = parse_args(argv)
    scenarios = BUG_SCENARIOS if not args.scenario_names else [s for s in BUG_SCENARIOS if s.name in args.scenario_names]
    upstream_contract_scenarios = (
        UPSTREAM_CONTRACT_SCENARIOS if not args.scenario_names else [s for s in UPSTREAM_CONTRACT_SCENARIOS if s.name in args.scenario_names]
    )

    storage = S3Storage()
    spark = get_spark_session("lifecycle-eval-harness")
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = run_evals(
            storage,
            spark,
            scenarios=scenarios,
            upstream_contract_scenarios=upstream_contract_scenarios,
            diagnosis_model=os.environ.get(DIAGNOSIS_MODEL_ENV_VAR),
            repair_model=os.environ.get(REPAIR_MODEL_ENV_VAR),
        )
    finally:
        _ensure_spark_session(spark).stop()

    run_id = uuid.uuid4().hex[:12]
    storage.write_json(f"curated/eval_reports/{run_id}.json", report)
    storage.write_json("curated/eval_report_latest.json", report)
    print_eval_report(report)

    if any(r.get("error") for r in report["scenarios"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
