"""Deterministic, flag-driven walkthrough of the flagship enterprise incident: payment_service
ships a v2 contract (successful installments renamed PAID -> SETTLED), loan_portfolio's Spark
ETL still only recognizes PAID, and the resulting incident is investigated, diagnosed, and
(optionally) repaired end to end -- through nothing but the existing, unmodified system:
src.data_ops's incident-response orchestrator, the real GitWorktreeSandbox, the real raw-
contract and business-reconciliation validators, and src.eval_scenarios' already-proven
UpstreamContractScenario definition for the injection itself. This module only sequences and
narrates those calls for a demo audience; it adds no new diagnosis/repair/verification logic.

    python3 -m src.demo.enterprise_incident --healthy-only
    python3 -m src.demo.enterprise_incident --inject-contract-change
    python3 -m src.demo.enterprise_incident --inject-contract-change --run-repair
    python3 -m src.demo.enterprise_incident --reset

--scripted-model (the default) costs no API calls: diagnosis and repair planning are driven
by a ScriptedDiagnosisModelClient replaying the exact tool-call sequence and final
submission a real run of this scenario produces (see _scripted_diagnosis_client_factory/
_scripted_repair_client_factory below) -- the same diagnosis/repair agent loops run for real,
only the model responses are canned. --live-model makes real calls through the same
OpenAIDiagnosisModelClient/OpenAIResponsesModelClient this codebase already uses everywhere
else, and additionally narrates Stage 3 as a natural-language business question rather than a
direct pipeline check.

Known scripted-mode limitation: once the human-approved repair reaches VERIFIED_PENDING_PR,
src.data_ops additionally tries to narrate the corrected CANDIDATE answer via a natural-
language Q&A pass (a genuinely different tool-calling loop than diagnosis/repair). The
scripted diagnosis client can't serve that loop, so this one final narration step reports "-
could not narrate a candidate answer via Q&A" instead of a sentence -- the PR artifact itself
(diff, branch, risk classification, before/after checks) is unaffected; it's real either way.
--live-model does not have this limitation, since a real model can serve any tool loop it's
asked to.

Idempotent: --inject-contract-change is a no-op (with a message, not an error) if the
incident is already injected; --reset restores the real raw tables + pipeline_run status from
a fixed backup prefix and is itself a no-op if there's nothing to restore. Every stage writes
its result into a run manifest under --output-dir for later inspection -- nothing here is
required to reproduce the demo; it's the recording of one run.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from pathlib import Path

import pandas as pd

from src.data_ops import _default_model_client_factory, _pending_repair_key, _section, run_incident_response
from src.eval_harness import _ensure_spark_session, _reload_etl_module
from src.eval_scenarios import UPSTREAM_CONTRACT_SCENARIOS
from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall
from src.storage import S3Storage
from src.validate_lifecycle_raw import TABLE_FILENAMES, validate_lifecycle_raw

SCENARIO = next(s for s in UPSTREAM_CONTRACT_SCENARIOS if s.name == "payment_service_v2_settled_rename")
DEMO_PIPELINE = SCENARIO.pipeline_name  # "loan_portfolio"
AFFECTED_RAW_TABLES = ("payment_schedule", "payment_events")
BACKUP_PREFIX = "_backup/enterprise_demo/"
PIPELINE_RUN_KEY = "curated/pipeline_run.json"
HEALTHY_QUESTION = "What is our total outstanding principal?"
DRIFT_CHECK_ID = "total_outstanding_principal_status_vocabulary_drift"
DEFAULT_OUTPUT_DIR = "demo_output"


def _demo_banner(title: str) -> None:
    _section(title)


def _is_injected(storage: S3Storage) -> bool:
    return all(storage.exists(f"{BACKUP_PREFIX}{table}.parquet") for table in AFFECTED_RAW_TABLES)


def _write_pipeline_run_entry(storage: S3Storage, pipeline_name: str, etl_status: str, validation_status: str) -> None:
    pipeline_run = storage.read_json(PIPELINE_RUN_KEY) if storage.exists(PIPELINE_RUN_KEY) else {"pipelines": {}}
    pipeline_run.setdefault("pipelines", {})[pipeline_name] = {
        "etl_status": etl_status,
        "etl_error": None,
        "validation_status": validation_status,
        "validation_error": None,
    }
    pipeline_run["overall_status"] = (
        "SUCCESS"
        if all(r.get("etl_status") == "SUCCESS" and r.get("validation_status") == "PASS" for r in pipeline_run["pipelines"].values())
        else "FAILURE"
    )
    storage.write_json(PIPELINE_RUN_KEY, pipeline_run)


def _prune_stale_repair_branches() -> list[str]:
    """Courtesy cleanup: GitWorktreeSandbox's create_pr mode intentionally keeps a repair
    candidate's branch (repair/<run_id>) around as a real, inspectable commit -- see
    src.sandbox.backend.GitWorktreeSandbox.keep_branch. Each demo run gets a fresh uuid, so
    this is never required for correctness, only to avoid branch clutter across repeated
    demo runs. Best-effort: a branch that fails to delete (e.g. still checked out somewhere)
    is silently left alone."""
    try:
        result = subprocess.run(["git", "branch", "--list", "repair/*"], capture_output=True, text=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    branches = [line.strip().lstrip("* ").strip() for line in result.stdout.splitlines() if line.strip()]
    pruned = []
    for branch in branches:
        outcome = subprocess.run(["git", "branch", "-D", branch], capture_output=True, text=True)
        if outcome.returncode == 0:
            pruned.append(branch)
    return pruned


def _clear_stale_pending_repair(storage: S3Storage) -> bool:
    """A pending-repair record (see src.data_ops.auto_scan_and_repair/run_incident_response)
    references a candidate branch -- once --reset prunes that branch (or restores the
    pipeline to healthy), the record is a ghost: it would make a later scan report
    'already_pending' for a repair that no longer exists anywhere. Cleared unconditionally
    on every reset; a no-op if there was never one."""
    key = _pending_repair_key(DEMO_PIPELINE)
    if storage.exists(key):
        storage.delete(key)
        return True
    return False


DEFAULT_BUSINESS_RULES_FILE = "context/business_rules.json"


def _restore_pointer_file_default(storage: S3Storage) -> bool:
    """A real Accept (src.data_ops.accept_repair) performs a genuine git merge that can
    permanently repoint loan_portfolio's context/pipeline_rules/loan_portfolio.json --
    restored here (locally and in S3) the same way --reset already restores the raw tables
    it manages, so the flagship contract-change scenario stays replayable. Never rewrites
    git history -- the merge commit itself stays in the log; only the file's current content
    is reset, and only if it currently differs from the default. Inert with respect to any
    currently-healthy pipeline: production ETL/validation never reads this pointer at all
    (only accept_repair's own resolution step does), so resetting its content can't affect
    what's already running."""
    spec = PIPELINE_REGISTRY[DEMO_PIPELINE]
    pointer_file = getattr(spec, "pipeline_configuration_file", None)
    if not pointer_file:
        return False
    local_path = Path(pointer_file)
    if not local_path.exists():
        return False
    current = json.loads(local_path.read_text())
    if current.get("business_rules_file") == DEFAULT_BUSINESS_RULES_FILE:
        return False
    current["business_rules_file"] = DEFAULT_BUSINESS_RULES_FILE
    local_path.write_text(json.dumps(current, indent=2) + "\n")
    storage.write_json(pointer_file, current)
    return True


def reset(storage: S3Storage, spark) -> dict:
    _demo_banner("RESET")
    cleared_pending = _clear_stale_pending_repair(storage)
    restored_pointer = _restore_pointer_file_default(storage)
    if not _is_injected(storage):
        print("  nothing to reset -- demo environment is already clean.")
        pruned = _prune_stale_repair_branches()
        if pruned:
            print(f"  pruned {len(pruned)} leftover demo repair branch(es): {pruned}")
        if cleared_pending:
            print(f"  cleared a stale pending-repair record for {DEMO_PIPELINE}")
        if restored_pointer:
            print(f"  restored {DEMO_PIPELINE}'s config pointer to its default (a prior Accept had repointed it)")
        return {
            "reset_performed": False,
            "pruned_branches": pruned,
            "cleared_pending": cleared_pending,
            "restored_pointer": restored_pointer,
        }

    spark = _ensure_spark_session(spark)
    spec = PIPELINE_REGISTRY[DEMO_PIPELINE]
    for table in AFFECTED_RAW_TABLES:
        storage.copy_or_promote(f"{BACKUP_PREFIX}{table}.parquet", f"raw/{table}.parquet")
        storage.delete(f"{BACKUP_PREFIX}{table}.parquet")

    business_rules = storage.read_json("context/business_rules.json")
    module = _reload_etl_module(spec.etl_source_file)
    outputs = spec.run_etl(module, spark, business_rules, DEFAULT_AS_OF_DATE)
    for key, df in outputs.items():
        storage.write_parquet(key, df)
    validation_rules = storage.read_json(spec.validation_rules_key)
    validation = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)
    _write_pipeline_run_entry(storage, DEMO_PIPELINE, "SUCCESS", validation["overall_status"])

    pruned = _prune_stale_repair_branches()
    print(f"  raw/{{{','.join(AFFECTED_RAW_TABLES)}}} restored to pre-injection bytes.")
    print(f"  {DEMO_PIPELINE} reran clean: validation_status={validation['overall_status']}")
    if pruned:
        print(f"  pruned {len(pruned)} leftover demo repair branch(es): {pruned}")
    if cleared_pending:
        print(f"  cleared a stale pending-repair record for {DEMO_PIPELINE}")
    if restored_pointer:
        print(f"  restored {DEMO_PIPELINE}'s config pointer to its default (a prior Accept had repointed it)")
    return {
        "reset_performed": True,
        "validation_status": validation["overall_status"],
        "pruned_branches": pruned,
        "cleared_pending": cleared_pending,
        "restored_pointer": restored_pointer,
    }


def inject_contract_change(storage: S3Storage, spark) -> dict:
    _demo_banner("STAGE 4: DEPLOY UPSTREAM SERVICE CONTRACT CHANGE")
    if _is_injected(storage):
        print(f"  payment_service is already running at contract {SCENARIO.contract_version} in this demo environment (idempotent no-op).")
        pipeline_run = storage.read_json(PIPELINE_RUN_KEY) if storage.exists(PIPELINE_RUN_KEY) else {}
        return {"already_injected": True, "pipeline_run_entry": pipeline_run.get("pipelines", {}).get(DEMO_PIPELINE)}

    from services.common.envelope import events_to_dataframe
    from services.common.runner import produce_events
    from services.payment_service.main import _build_specs
    from src.events_to_lifecycle_tables import EVENT_TYPE_TO_TABLE, _strip_envelope

    spec = PIPELINE_REGISTRY[DEMO_PIPELINE]
    spark = _ensure_spark_session(spark)
    business_rules = storage.read_json("context/business_rules.json")

    print(f"  payment_service: contract v1 (PAID) -> {SCENARIO.contract_version} (SETTLED for the same successful installments)")
    for table in AFFECTED_RAW_TABLES:
        storage.copy_or_promote(f"raw/{table}.parquet", f"{BACKUP_PREFIX}{table}.parquet")

    payment_specs = _build_specs(SCENARIO.contract_version, SCENARIO.num_customers, SCENARIO.seed, DEFAULT_AS_OF_DATE)
    events_by_type = produce_events("payment_service", "v1", payment_specs, SCENARIO.num_customers, SCENARIO.seed, DEFAULT_AS_OF_DATE)
    by_table: dict[str, list[pd.DataFrame]] = {}
    for event_type, events in events_by_type.items():
        table_name = EVENT_TYPE_TO_TABLE[event_type]
        by_table.setdefault(table_name, []).append(_strip_envelope(events_to_dataframe(events)))
    reconstructed = {name: pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0] for name, parts in by_table.items()}
    for table in AFFECTED_RAW_TABLES:
        storage.write_parquet(f"raw/{table}.parquet", reconstructed[table])
    print("  event generation:    SUCCESS -- payment_service v2 emitted SETTLED for successful installments")
    print("  ingestion:           SUCCESS -- raw/payment_schedule.parquet, raw/payment_events.parquet updated")

    module = _reload_etl_module(spec.etl_source_file)
    outputs = spec.run_etl(module, spark, business_rules, DEFAULT_AS_OF_DATE)
    for key, df in outputs.items():
        storage.write_parquet(key, df)
    print("  spark pipeline:      SUCCESS -- loan_portfolio ETL completed with no error and wrote curated output")

    validation_rules = storage.read_json(spec.validation_rules_key)
    validation = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)
    _write_pipeline_run_entry(storage, DEMO_PIPELINE, "SUCCESS", validation["overall_status"])

    raw_tables = {name: storage.read_parquet(f"raw/{name}.parquet") for name in TABLE_FILENAMES}
    raw_validation_rules = storage.read_json("context/validations/lifecycle_raw.json")
    raw_validation = validate_lifecycle_raw(raw_tables, business_rules, raw_validation_rules)

    failed_curated = [c["id"] for c in validation["checks"] if c["status"] == "FAIL"]
    failed_raw = [c["id"] for c in raw_validation["checks"] if c["status"] == "FAIL"]
    print(f"  raw contract validation (validate_lifecycle_raw):  {raw_validation['overall_status']} -- failed: {failed_raw or 'none'}")
    print(f"  business reconciliation (validate_{DEMO_PIPELINE}): {validation['overall_status']} -- failed: {failed_curated or 'none'}")
    print(f"  {DEMO_PIPELINE} is now UNTRUSTED despite every job (event generation, ingestion, Spark) reporting success.")

    return {
        "already_injected": False,
        "contract_version": SCENARIO.contract_version,
        "curated_validation_status": validation["overall_status"],
        "curated_failed_checks": failed_curated,
        "raw_validation_status": raw_validation["overall_status"],
        "raw_failed_checks": failed_raw,
    }


# --- scripted (no-API-cost) diagnosis + repair -----------------------------------------------
#
# Exact tool-call sequence and final submission a real diagnosis/repair run of this scenario
# produces. ScriptedDiagnosisModelClient replays these verbatim regardless of what the real
# tool dispatch returns (see src/model_client.py) -- the real diagnosis/repair agent loops,
# real tool dispatch, and real grounding/validation all still run; only the model's responses
# are canned, so a demo run costs zero API calls and is fully repeatable.

SCRIPTED_DIAGNOSIS = {
    "diagnosis_status": "DIAGNOSED",
    "incident_summary": (
        "payment_service v2 renamed a successfully collected installment's payment_status from "
        "PAID to SETTLED; loan_portfolio's net-payment filter and the business rule it reads from "
        "never learned the new status, so net_paid silently collapses toward 0 and "
        "total_outstanding_principal is overstated."
    ),
    "affected_metrics": ["total_outstanding_principal"],
    "root_cause_category": "SOURCE_CONTRACT_CHANGE",
    "initiating_event": (
        "payment_service deployed contract v2, renaming a successfully collected installment's "
        "payment_status from PAID to SETTLED."
    ),
    "root_cause": (
        "src/etl_spark_loan_portfolio.py's compute_loan_portfolio filters payment_events on "
        "business_rules['successful_payment_statuses'] plus REVERSED, which still only contains "
        "'PAID' -- it never recognizes the new 'SETTLED' status the upstream contract now emits."
    ),
    "reasoning_summary": (
        "get_failed_checks showed total_outstanding_principal_status_vocabulary_drift failing; "
        "trace_failed_check_to_code pointed at compute_loan_portfolio's net_payment_statuses "
        "filter; get_context_conflicts confirmed the approved successful_payment_statuses "
        "definition (PAID only) no longer matches what payment_events actually contains after "
        "the v2 rename."
    ),
    "evidence": [
        {
            "source_type": "VALIDATION",
            "source_reference": "get_failed_checks",
            "finding": (
                "total_outstanding_principal_status_vocabulary_drift failed: the amount-agnostic "
                "recomputation disagrees with the curated value by far more than ordinary "
                "LATE-payment exclusion would explain."
            ),
            "expected": None,
            "actual": None,
        },
        {
            "source_type": "ETL_SOURCE",
            "source_reference": "trace_failed_check_to_code",
            "finding": (
                "compute_loan_portfolio only treats business_rules['successful_payment_statuses'] "
                "(['PAID']) plus 'REVERSED' as net-payment-affecting statuses."
            ),
            "expected": None,
            "actual": None,
        },
        {
            "source_type": "BUSINESS_RULE",
            "source_reference": "get_context_conflicts",
            "finding": (
                "the approved successful_payment_statuses definition no longer matches the "
                "payment_status values payment_events actually contains post-v2."
            ),
            "expected": "PAID",
            "actual": "SETTLED",
        },
    ],
    "recommended_fix": {
        "target_file": "context/pipeline_rules/loan_portfolio.json",
        "change_summary": (
            "Point loan_portfolio's business_rules_file at the already-approved "
            "settled-adopted ruleset instead of editing the shared, cross-pipeline "
            "business_rules.json or the ETL source directly."
        ),
        "scope": "MINIMAL",
    },
    "confidence": "HIGH",
    "uncertainties": [],
    "additional_evidence_needed": [],
}

SCRIPTED_REPAIR_SUBMISSION = {
    "repair_decision": "PROPOSE_REPAIR",
    "repair_type": "CONFIGURATION_CHANGE",
    "incident_id": DEMO_PIPELINE,
    "diagnosis_reference": SCRIPTED_DIAGNOSIS["incident_summary"],
    "root_cause_addressed": SCRIPTED_DIAGNOSIS["root_cause"],
    "target_file": "context/pipeline_rules/loan_portfolio.json",
    "target_symbol_or_setting": "business_rules_file",
    "current_behavior": "loan_portfolio reads context/business_rules.json, which still only approves PAID as a successful payment status.",
    "proposed_behavior": "loan_portfolio reads context/business_rules_settled_adopted.json instead, which additionally approves SETTLED.",
    "change_description": "Point loan_portfolio's business_rules_file at the already-approved settled-adopted ruleset.",
    "patch": {
        "format": "STRUCTURED_CONFIG_EDIT",
        "content": {"operations": [{"field": "business_rules_file", "value": "context/business_rules_settled_adopted.json"}]},
    },
    "files_expected_to_change": ["context/pipeline_rules/loan_portfolio.json"],
    "files_expected_not_to_change": ["context/business_rules.json", "src/etl_spark_loan_portfolio.py"],
    "verification_steps": ["rerun loan_portfolio ETL against the adopted ruleset", "rerun validate_loan_portfolio"],
    "rollback_description": "Revert the pointer back to context/business_rules.json.",
    "risk_level": "LOW",
    "assumptions": ["context/business_rules_settled_adopted.json correctly reflects the org's approved response to the payment_service v2 contract."],
    "evidence_references": ["get_failed_checks", "trace_failed_check_to_code"],
}


def _scripted_diagnosis_client_factory() -> ScriptedDiagnosisModelClient:
    from src.legacy.diagnosis_agent import SUBMIT_DIAGNOSIS_TOOL_NAME

    exploration = ModelResponse(
        tool_calls=[
            ToolCall(id="1", name="get_failed_checks", arguments={}),
            ToolCall(id="2", name="trace_failed_check_to_code", arguments={"check_id": DRIFT_CHECK_ID}),
            ToolCall(id="3", name="get_context_conflicts", arguments={"metric_name": "total_outstanding_principal"}),
            ToolCall(id="4", name="get_business_rules", arguments={}),
        ]
    )
    submission = ModelResponse(tool_calls=[ToolCall(id="5", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=SCRIPTED_DIAGNOSIS)])
    return ScriptedDiagnosisModelClient([exploration, submission])


def _scripted_repair_client_factory() -> ScriptedDiagnosisModelClient:
    from src.lifecycle_repair_agent import SUBMIT_REPAIR_PLAN_TOOL_NAME

    submission = ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_REPAIR_PLAN_TOOL_NAME, arguments=SCRIPTED_REPAIR_SUBMISSION)])
    return ScriptedDiagnosisModelClient([submission])


# --- stages ------------------------------------------------------------------------------------


def run_healthy_stage(storage: S3Storage, *, live_model: bool) -> dict:
    _demo_banner("STAGE 3: HEALTHY BUSINESS REQUEST")
    if live_model:
        result = run_incident_response(storage, _default_model_client_factory, question=HEALTHY_QUESTION, mode="create_pr")
    else:
        print(
            "  (scripted-model mode: natural-language Q&A needs a real reasoning model -- checking "
            "the data product's trust status directly instead. Pass --live-model for the full "
            "business-question narrative.)"
        )
        result = run_incident_response(storage, _default_model_client_factory, pipeline_name=DEMO_PIPELINE, mode="create_pr")
    return {"live_model": live_model, "result": result}


def run_investigation_and_repair_stage(storage: S3Storage, spark, *, live_model: bool) -> dict:
    if not _is_injected(storage):
        print("(auto-injecting the upstream contract change first -- pass --inject-contract-change on its own to see that stage in isolation.)")
        inject_contract_change(storage, spark)

    diagnosis_factory = _default_model_client_factory if live_model else _scripted_diagnosis_client_factory
    repair_factory = None if live_model else _scripted_repair_client_factory

    _demo_banner("STAGE 5-6: INCIDENT INVESTIGATION (repair not yet approved)")
    refused = run_incident_response(
        storage,
        diagnosis_factory,
        pipeline_name=DEMO_PIPELINE,
        mode="create_pr",
        repair_model_client_factory=repair_factory,
    )

    _demo_banner("STAGE 7-11: HUMAN-APPROVED GOVERNED REPAIR")
    approved = run_incident_response(
        storage,
        diagnosis_factory,
        pipeline_name=DEMO_PIPELINE,
        mode="create_pr",
        human_approved_categories=frozenset({"SOURCE_CONTRACT_CHANGE"}),
        repair_model_client_factory=repair_factory,
    )

    return {"live_model": live_model, "refused": refused, "approved": approved}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministic, flag-driven enterprise incident demo.")
    parser.add_argument("--reset", action="store_true", help="Restore real data + repo state to pre-demo, idempotent.")
    parser.add_argument("--healthy-only", action="store_true", help="Stage 3: show the healthy, trusted request.")
    parser.add_argument("--inject-contract-change", action="store_true", help="Stage 4: deploy payment_service v2.")
    parser.add_argument("--run-repair", action="store_true", help="Stages 5-11: investigate, then human-approved repair.")
    model_group = parser.add_mutually_exclusive_group()
    model_group.add_argument("--scripted-model", action="store_true", help="No API calls (default).")
    model_group.add_argument("--live-model", action="store_true", help="Real OpenAI calls.")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    from src.spark_session import get_spark_session

    args = parse_args(argv)
    live_model = args.live_model

    storage = S3Storage()
    spark = get_spark_session("enterprise-incident-demo")
    spark.sparkContext.setLogLevel("WARN")

    run_id = uuid.uuid4().hex[:12]
    manifest: dict = {"run_id": run_id, "live_model": live_model, "stages": []}
    try:
        if args.reset:
            manifest["stages"].append({"stage": "reset", "result": reset(storage, spark)})
        if args.healthy_only:
            manifest["stages"].append({"stage": "healthy", "result": run_healthy_stage(storage, live_model=live_model)})
        if args.inject_contract_change:
            manifest["stages"].append({"stage": "inject_contract_change", "result": inject_contract_change(storage, spark)})
        if args.run_repair:
            manifest["stages"].append(
                {"stage": "investigate_and_repair", "result": run_investigation_and_repair_stage(storage, spark, live_model=live_model)}
            )
    finally:
        _ensure_spark_session(spark).stop()

    if not manifest["stages"]:
        print("No stage flag given -- nothing to do. See --help.")
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    print(f"\nRun manifest written to {manifest_path}")

    # Also persisted to S3 -- a real, growing audit trail of every demo run (scripted and
    # live-model alike), not just the latest -- so src.eval_report can discover and bucket
    # real scripted-model/live-model results without needing a local file path.
    storage.write_json(f"curated/demo_runs/{run_id}.json", manifest)
    storage.write_json("curated/demo_run_latest.json", manifest)


if __name__ == "__main__":
    main()
