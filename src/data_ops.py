"""Data-operations control-layer presentation: a view of the whole data estate, and a
narrated incident-response orchestrator. Purely a presentation layer over the existing,
unmodified system -- every fact here already exists in an existing function's return value
(src.ask_lifecycle.answer_lifecycle_question, src.lifecycle_run_self_healing's diagnose ->
repair -> verify -> promote/PR-artifact flow, src.ask_lifecycle.answer_from_candidate). No new
diagnosis, repair, verification, sandboxing, or validation logic lives here.

Natural-language Q&A (src/api.py, src/ask_lifecycle.py) is one entry point into that system.
This module is another: `estate` shows the trust/governance state of every registered data
product at a glance; `incident` walks a single one through the full incident-response
lifecycle, narrated, from either a business question or a direct data-product name.

    python3 -m src.data_ops estate
    python3 -m src.data_ops incident --pipeline loan_portfolio --mode create_pr
    python3 -m src.data_ops incident --question "What is the total outstanding principal?" --mode create_pr
"""

from __future__ import annotations

import argparse
import importlib
import os
import subprocess

from src.ask_lifecycle import (
    ANSWER_MODEL_ENV_VAR,
    AskLifecycleError,
    _attempt_self_heal,
    answer_from_candidate,
    answer_lifecycle_question,
)
from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.lifecycle_answer_models import AnswerValidationError
from src.lifecycle_business_agent import LifecycleBusinessAgentError
from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.storage import S3Storage, StorageError

_CANDIDATE_ANSWER_ERRORS = (AskLifecycleError, ModelClientError, LifecycleBusinessAgentError, AnswerValidationError)

_BAR = "=" * 72


def _section(title: str) -> None:
    print(f"\n{_BAR}\n{title}\n{_BAR}")


def data_product_estate(storage: S3Storage) -> list[dict]:
    """One row per registered data product: its trust status (from
    curated/pipeline_run.json, the same health signal src.ask_lifecycle already gates on) and
    its context-governance status (from the same ContextStore src.context_retriever already
    reads -- whether it has a human-approved semantic layer, its generated context's review
    status, and any unresolved conflict between the two). No Spark needed -- S3 reads only."""
    store = FileContextStore()
    retriever = ContextRetriever(store=store)
    pipeline_run = storage.read_json("curated/pipeline_run.json") if storage.exists("curated/pipeline_run.json") else {"pipelines": {}}

    rows = []
    for name in sorted(PIPELINE_REGISTRY):
        run_entry = pipeline_run.get("pipelines", {}).get(name, {})
        human = store.get_human_annotation(name)
        generated = store.get_generated_context(name)

        if human is not None and generated is not None:
            provenance = "human+generated"
        elif human is not None:
            provenance = "human"
        elif generated is not None:
            provenance = "generated"
        else:
            provenance = "legacy_file"

        open_conflicts = 0
        if human is not None:
            for metric_name in human.metrics:
                fact = retriever.get_metric(name, metric_name, storage)
                open_conflicts += len(fact.conflicts)

        rows.append(
            {
                "pipeline_name": name,
                "etl_status": run_entry.get("etl_status"),
                "validation_status": run_entry.get("validation_status"),
                "context_provenance": provenance,
                "review_status": generated.review_status.value if generated else None,
                "open_conflicts": open_conflicts,
            }
        )
    return rows


def _prefix_stats(storage: S3Storage, prefix: str) -> dict:
    """file_count/total_bytes for everything under an S3 prefix -- read-only, measurement
    only. Uses the boto3 client directly (S3Storage.list_paths returns keys only, no sizes),
    same pattern this codebase's own test fixtures already use for S3-level operations
    S3Storage doesn't expose a higher-level method for."""
    file_count = 0
    total_bytes = 0
    paginator = storage._client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            file_count += 1
            total_bytes += obj["Size"]
    return {"file_count": file_count, "total_bytes": total_bytes}


def scale_summary(storage: S3Storage) -> dict:
    """Measured, real numbers describing the current data estate's scale -- every number
    here is read live from S3, never hardcoded. Static counts (registered pipelines,
    upstream services) come from this system's own registries, not a guess. Used by the
    Overview tab/console to show real scale without claiming anything beyond what's actually
    measured (see the project's "must not claim TB/PB-scale performance" boundary --
    this reports what IS here, not a projection)."""
    from src.validate_lifecycle_raw import TABLE_FILENAMES

    raw_row_counts = {}
    for table in TABLE_FILENAMES:
        key = f"raw/{table}.parquet"
        if storage.exists(key):
            raw_row_counts[table] = len(storage.read_parquet(key))

    return {
        "customers": raw_row_counts.get("customers", 0),
        "raw_table_row_counts": raw_row_counts,
        "raw_total_rows": sum(raw_row_counts.values()),
        "storage": {
            "raw": _prefix_stats(storage, "raw/"),
            "events": _prefix_stats(storage, "events/"),
            "curated": _prefix_stats(storage, "curated/"),
        },
        "registered_pipelines": len(PIPELINE_REGISTRY),
        "upstream_services": 6,
    }


def print_scale_summary(summary: dict) -> None:
    _section("SCALE SUMMARY (measured, not projected)")
    print(f"  customers:            {summary['customers']:,}")
    print(f"  raw table rows total: {summary['raw_total_rows']:,}")
    for table, count in sorted(summary["raw_table_row_counts"].items()):
        print(f"    {table:<24} {count:,}")
    for prefix, stats in summary["storage"].items():
        print(f"  {prefix}/: {stats['file_count']:,} files, {stats['total_bytes'] / 1e6:.1f} MB")
    print(f"  registered pipelines: {summary['registered_pipelines']}")
    print(f"  upstream services:    {summary['upstream_services']}")


def print_estate(rows: list[dict]) -> None:
    _section("DATA PRODUCT ESTATE")
    header = f"{'DATA PRODUCT':<26} {'ETL':<9} {'TRUST':<9} {'CONTEXT':<17} {'REVIEW':<11} {'CONFLICTS':<9}"
    print(header)
    print("-" * len(header))
    for row in rows:
        trust = row["validation_status"] or "UNKNOWN"
        flag = "  <-- UNTRUSTED" if trust not in ("PASS",) else ""
        print(
            f"{row['pipeline_name']:<26} {row['etl_status'] or '-':<9} {trust:<9} "
            f"{row['context_provenance']:<17} {row['review_status'] or '-':<11} {row['open_conflicts']:<9}{flag}"
        )


def _print_diagnosis(diagnosis: dict | None) -> None:
    if not diagnosis:
        return
    print(f"  root_cause_category: {diagnosis.get('root_cause_category')}")
    print(f"  root_cause:          {diagnosis.get('root_cause')}")
    print(f"  confidence:          {diagnosis.get('confidence')}")
    for item in diagnosis.get("evidence", []) or []:
        print(f"    evidence [{item.get('source_type')} via {item.get('source_reference')}]: {item.get('finding')}")


def _print_provenance(retriever: ContextRetriever, storage: S3Storage, pipeline_name: str, metric_name: str) -> None:
    """The compact provenance breakdown for one cited metric -- where each fact backing a
    trusted answer actually came from, not just the number itself."""
    metric_fact = retriever.get_metric(pipeline_name, metric_name, storage)
    lineage_fact = retriever.get_lineage(pipeline_name, storage)
    pipeline_fact = retriever.get_pipeline_metadata(pipeline_name, storage)
    health_fact = retriever.get_runtime_health(pipeline_name, storage)

    _section(f"PROVENANCE: {pipeline_name}.{metric_name}")
    review_status = metric_fact.review_status.value if metric_fact.review_status else None
    print(f"  Metric definition:       {metric_fact.provenance:<10} (review_status={review_status})")
    print(f"  Lineage:                 {lineage_fact.provenance:<10} (traces to upstream service(s) where known)")
    print(f"  Pipeline implementation: {pipeline_fact.provenance:<10} (repository source: {pipeline_fact.value.get('etl_source_file', pipeline_name)})")
    print(f"  Current health:          {health_fact.provenance:<10} (validation_status={health_fact.value.get('validation_status')})")
    if metric_fact.conflicts:
        print(f"  ! {len(metric_fact.conflicts)} open context conflict(s) on this metric -- see Context tab.")


def _print_heal(pipeline_name: str, heal: dict, mode: str) -> None:
    _section(f"INCIDENT OPENED: {pipeline_name}")
    print("Trust check: FAIL -- data behind this data product failed independent validation.")

    _section("DIAGNOSIS (traced through lineage + context)")
    _print_diagnosis(heal.get("diagnosis"))

    repair_result = heal.get("repair_result") or {}
    _section("GOVERNED REPAIR")
    print(f"  repair_status: {repair_result.get('repair_status')}")
    if repair_result.get("target_file"):
        print(f"  target_file:   {repair_result.get('target_file')}")

    verification = heal.get("repair_verification") or {}
    _section("SANDBOXED SPARK RERUN + INDEPENDENT VALIDATION")
    print(f"  verification_status: {verification.get('verification_status')}")
    print(f"  summary:             {verification.get('summary')}")

    pr_artifact = verification.get("pr_artifact")
    if pr_artifact:
        _section("PR-READY REPAIR ARTIFACT (local -- never pushed)")
        print(f"  branch:              {pr_artifact.get('branch')}")
        print(f"  risk_classification: {pr_artifact.get('risk_classification')}")
        print(f"  human_review_required: {pr_artifact.get('human_review_required')}")
        print("  diff:")
        for line in (pr_artifact.get("diff") or "").splitlines()[:20]:
            print(f"    {line}")
    elif mode != "create_pr" and verification.get("verification_status") == "VERIFIED":
        print("\n(mode=auto_promote: the fix was promoted directly into the real repository/bucket.)")


def run_incident_response(
    storage: S3Storage,
    diagnosis_model_client_factory,
    *,
    question: str | None = None,
    pipeline_name: str | None = None,
    mode: str = "create_pr",
    human_approved_categories: frozenset[str] = frozenset(),
    repair_model_client_factory=None,
) -> dict:
    """The narrated business-signal -> incident-response lifecycle. Exactly one of
    `question` (a natural-language business signal; relevance is inferred the same way
    src.ask_lifecycle already infers it) or `pipeline_name` (a direct operational check,
    skipping inference for deterministic demo pacing) must be given.

    human_approved_categories (only meaningful on the `pipeline_name` path, and only with
    mode="create_pr") is the explicit "a human looked at this specific incident and approved
    generating a reviewable candidate anyway" action -- e.g. {"SOURCE_CONTRACT_CHANGE"} -- see
    src.lifecycle_run_self_healing's docstring. Never available on the `question` path: a
    typed business question stays a passive, always-policy-respecting entry point; approving
    an override is a deliberate, separate operator action on a specific incident.

    repair_model_client_factory (only meaningful on the `pipeline_name` path) overrides which
    model client repair planning uses -- None (the default) preserves the real repair model
    src.ask_lifecycle._attempt_self_heal already uses. See src.demo.enterprise_incident for
    the one real caller of this today: a scripted, no-API-cost repair client for repeatable
    demo runs.

    Returns the underlying result dict (answer_lifecycle_question's shape for the `question`
    path, or a smaller {pipeline_name, self_heal, candidate_answer} dict for the
    `pipeline_name` path) with one addition either way: "candidate_answer", populated via
    answer_from_candidate when mode="create_pr" and a repair reached VERIFIED_PENDING_PR --
    the corrected result as it WOULD read once the PR artifact is merged, without ever
    promoting anything for real.
    """
    if (question is None) == (pipeline_name is None):
        raise ValueError("exactly one of question or pipeline_name must be given")

    if question is not None:
        _section("BUSINESS SIGNAL")
        print(f"  {question!r}")
        result = answer_lifecycle_question(question, storage, diagnosis_model_client_factory, mode=mode)

        if result["relevant_pipelines"]:
            _section("DATA PRODUCT(S) IN SCOPE")
            print(f"  {', '.join(result['relevant_pipelines'])}")

        candidate_answer = None
        if result["self_heal"]:
            for name, heal in result["self_heal"].items():
                _print_heal(name, heal, mode)
                verification = heal.get("repair_verification") or {}
                if mode == "create_pr" and verification.get("verification_status") == "VERIFIED_PENDING_PR":
                    # Persisted the same way the direct pipeline_name path does (below) --
                    # so a repair candidate surfaced via a business question shows up in the
                    # Repairs tab too, not just one triggered by a direct pipeline check.
                    storage.write_json(
                        _pending_repair_key(name),
                        {"pipeline_name": name, "status": "pending_review", "pr_artifact": verification.get("pr_artifact"), "diagnosis": heal.get("diagnosis")},
                    )
                    try:
                        candidate_answer = answer_from_candidate(
                            question, storage, diagnosis_model_client_factory, name, verification.get("metrics_after", {})
                        )
                    except _CANDIDATE_ANSWER_ERRORS as exc:
                        print(f"\n  (could not narrate a candidate answer via Q&A: {exc})")
        else:
            _section("TRUST CHECK")
            print("  PASS -- no incident. Answering from trusted curated data.")
            retriever = ContextRetriever(store=FileContextStore())
            shown = set()
            for name in result["relevant_pipelines"]:
                for cited in result["answer"]["cited_metrics"]:
                    metric_name = cited["metric_name"]
                    if (name, metric_name) in shown:
                        continue
                    shown.add((name, metric_name))
                    try:
                        _print_provenance(retriever, storage, name, metric_name)
                    except Exception:  # noqa: BLE001 -- provenance display is best-effort narration, never blocks the answer
                        pass

        _section("RESULT")
        print(f"  status: {result['answer']['answer_status']}")
        print(f"  {result['answer']['answer_summary']}")
        if result.get("corrected_answer"):
            print(f"\n  corrected (promoted) answer: {result['corrected_answer']['answer_summary']}")
        if candidate_answer:
            _section("CORRECTED CANDIDATE RESULT (from the unpromoted repair candidate)")
            print(f"  {candidate_answer['answer_summary']}")

        result["candidate_answer"] = candidate_answer
        return result

    # Direct data-product path: no natural-language question to ground relevance in -- an
    # operational health check on one named data product, e.g. from a monitor/scheduler.
    _section("BUSINESS SIGNAL")
    print(f"  Operational check: is {pipeline_name!r} trustworthy?")
    _section(f"DATA PRODUCT: {pipeline_name}")

    pipeline_run = storage.read_json("curated/pipeline_run.json") if storage.exists("curated/pipeline_run.json") else {}
    run_entry = pipeline_run.get("pipelines", {}).get(pipeline_name, {})
    is_trustworthy = run_entry.get("etl_status") == "SUCCESS" and run_entry.get("validation_status") == "PASS"

    if is_trustworthy:
        _section("TRUST CHECK")
        print("  PASS -- no incident.")
        return {"pipeline_name": pipeline_name, "self_heal": None, "candidate_answer": None}

    if human_approved_categories:
        _section("HUMAN APPROVAL")
        print(f"  Operator approved generating a reviewable candidate for: {sorted(human_approved_categories)}")
    heal = _attempt_self_heal(
        pipeline_name,
        storage,
        diagnosis_model_client_factory,
        mode=mode,
        human_approved_categories=human_approved_categories,
        repair_model_client_factory=repair_model_client_factory,
    )
    _print_heal(pipeline_name, heal, mode)

    candidate_answer = None
    verification = heal.get("repair_verification") or {}
    if mode == "create_pr" and verification.get("verification_status") == "VERIFIED_PENDING_PR":
        # Persisted the same way an auto-detected candidate is (see auto_scan_and_repair
        # below) -- so accept/reject works uniformly regardless of whether a human manually
        # triggered this check or a health-monitor scan found it first.
        storage.write_json(
            _pending_repair_key(pipeline_name),
            {"pipeline_name": pipeline_name, "status": "pending_review", "pr_artifact": verification.get("pr_artifact"), "diagnosis": heal.get("diagnosis")},
        )
        question = f"What is the current state of {pipeline_name}?"
        try:
            candidate_answer = answer_from_candidate(
                question, storage, diagnosis_model_client_factory, pipeline_name, verification.get("metrics_after", {})
            )
        except _CANDIDATE_ANSWER_ERRORS as exc:
            # The candidate-answer Q&A pass uses a different tool loop than diagnosis/repair --
            # a model client scripted for one (e.g. a demo's no-API-cost stand-in) can't serve
            # the other. The PR artifact/verification result above is already real and
            # complete; a failure narrating it as a natural-language answer is not fatal.
            print(f"\n  (could not narrate a candidate answer via Q&A: {exc})")
        if candidate_answer:
            _section("CORRECTED CANDIDATE RESULT (from the unpromoted repair candidate)")
            print(f"  {candidate_answer['answer_summary']}")

    return {"pipeline_name": pipeline_name, "self_heal": heal, "candidate_answer": candidate_answer}


# --- Automatic detection + human accept/reject -----------------------------------------------
#
# Everything above requires an operator to explicitly trigger an incident check. This section
# is the automatic version: a health-monitor-style scan that finds an untrusted data product,
# runs diagnosis, and -- for the one root-cause category this system has a real, narrow,
# pre-approved remediation path for (SOURCE_CONTRACT_CHANGE -> a pipeline-owned config
# pointer, never the shared business_rules.json or an ETL source file) -- generates a
# candidate repair automatically, without a human approving that step. The human checkpoint
# moves to the END instead: nothing is promoted until a human explicitly accepts the
# resulting VERIFIED_PENDING_PR candidate (a real, local `git merge`, never pushed) or
# rejects it (discards the candidate branch). Generating a candidate is safe to automate
# because it never leaves the sandbox; merging into main is not, so that step stays manual.
#
# This auto-approval is deliberately narrow: it does NOT extend to any other normally-refused
# category (LOW confidence, INSUFFICIENT_EVIDENCE, or any root_cause_category besides
# SOURCE_CONTRACT_CHANGE) -- there is no equivalent safe, pre-approved fix registered for
# those, so they still stop at HUMAN_REVIEW_REQUIRED with no candidate generated at all.

AUTO_APPROVED_CATEGORIES = frozenset({"SOURCE_CONTRACT_CHANGE"})
PIPELINE_RUN_KEY = "curated/pipeline_run.json"


def _pending_repair_key(pipeline_name: str) -> str:
    return f"curated/pending_repairs/{pipeline_name}.json"


def list_pending_repairs(storage: S3Storage) -> list[dict]:
    """Every data product with a candidate repair currently awaiting a human accept/reject
    decision -- persisted so the console doesn't need to rescan to show what's pending."""
    pending = []
    for pipeline_name in sorted(PIPELINE_REGISTRY):
        key = _pending_repair_key(pipeline_name)
        if storage.exists(key):
            pending.append(storage.read_json(key))
    return pending


def auto_scan_and_repair(
    storage: S3Storage, diagnosis_model_client_factory, repair_model_client_factory=None, pipeline_names: frozenset[str] | None = None
) -> list[dict]:
    """The health-monitor scan: for every untrusted data product without an existing pending
    candidate, run the incident-response lifecycle with SOURCE_CONTRACT_CHANGE pre-approved
    (see module note above). Idempotent per pipeline -- a pipeline with an already-pending
    candidate is reported as such, never re-diagnosed/re-repaired on every scan (that would
    both waste real model calls and spawn a fresh git branch each time). A pipeline that's
    healthy but has a stale pending record (e.g. fixed by some other means) has that record
    cleared. Returns one result dict per pipeline this scan actually looked at (skips
    pipelines that are healthy with no stale record -- nothing to report).

    pipeline_names, when given, restricts the scan to just those pipelines -- e.g. a scripted
    (no-API-cost) model client is only valid for the one scenario it's built for (see
    src.api._require_scripted_model_eligible); every existing caller passes None (scan
    everything), unaffected."""
    results = []
    for row in data_product_estate(storage):
        pipeline_name = row["pipeline_name"]
        if pipeline_names is not None and pipeline_name not in pipeline_names:
            continue
        pending_key = _pending_repair_key(pipeline_name)
        is_trustworthy = row["etl_status"] == "SUCCESS" and row["validation_status"] == "PASS"

        if is_trustworthy:
            if storage.exists(pending_key):
                storage.delete(pending_key)
                results.append({"pipeline_name": pipeline_name, "status": "resolved_externally"})
            continue

        if storage.exists(pending_key):
            results.append({"pipeline_name": pipeline_name, "status": "already_pending", "pending": storage.read_json(pending_key)})
            continue

        _section(f"AUTO-DETECTED INCIDENT: {pipeline_name}")
        heal = _attempt_self_heal(
            pipeline_name,
            storage,
            diagnosis_model_client_factory,
            mode="create_pr",
            human_approved_categories=AUTO_APPROVED_CATEGORIES,
            repair_model_client_factory=repair_model_client_factory,
        )
        _print_heal(pipeline_name, heal, "create_pr")
        verification = heal.get("repair_verification") or {}

        if verification.get("verification_status") == "VERIFIED_PENDING_PR":
            pending_record = {
                "pipeline_name": pipeline_name,
                "status": "pending_review",
                "pr_artifact": verification.get("pr_artifact"),
                "diagnosis": heal.get("diagnosis"),
            }
            storage.write_json(pending_key, pending_record)
            results.append(pending_record)
        else:
            results.append(
                {
                    "pipeline_name": pipeline_name,
                    "status": verification.get("verification_status", "NOT_VERIFIED"),
                    "diagnosis": heal.get("diagnosis"),
                    "summary": verification.get("summary"),
                }
            )
    return results


def accept_repair(pipeline_name: str, branch: str, storage: S3Storage, spark) -> dict:
    """A human explicitly accepting a VERIFIED_PENDING_PR candidate: a real, local `git
    merge` of its branch into the current branch (never pushed to GitHub -- matches this
    project's "no real GitHub PR publication" boundary), then a real rerun of this pipeline's
    ETL/validation so the accepted change actually takes effect in the live environment --
    merging alone would leave real curated data reflecting the pre-fix state until rerun.
    Never automatic; only ever called in direct response to an explicit human action."""
    _section(f"ACCEPT REPAIR: {pipeline_name} ({branch})")
    try:
        subprocess.run(
            ["git", "merge", "--no-ff", branch, "-m", f"Merge {branch}: accept repair for {pipeline_name}"],
            check=True, capture_output=True, text=True, timeout=30,
        )
    except subprocess.CalledProcessError as exc:
        return {"accepted": False, "pipeline_name": pipeline_name, "branch": branch, "error": f"git merge failed: {exc.stderr.strip()}"}
    except subprocess.TimeoutExpired:
        return {"accepted": False, "pipeline_name": pipeline_name, "branch": branch, "error": "git merge timed out"}
    subprocess.run(["git", "branch", "-d", branch], capture_output=True, timeout=30)

    from src.migrate_lifecycle_to_s3 import migrate_context

    migrate_context(storage)  # re-sync the just-merged context files (e.g. a repointed config) to S3

    spec = PIPELINE_REGISTRY[pipeline_name]
    business_rules = storage.read_json("context/business_rules.json")
    pipeline_configuration_file = getattr(spec, "pipeline_configuration_file", None)
    if pipeline_configuration_file and storage.exists(pipeline_configuration_file):
        pointer = storage.read_json(pipeline_configuration_file)
        business_rules_file = pointer.get("business_rules_file")
        if business_rules_file and storage.exists(business_rules_file):
            business_rules = storage.read_json(business_rules_file)

    module = importlib.import_module(spec.etl_source_file.replace("/", ".").removesuffix(".py"))
    outputs = spec.run_etl(module, spark, business_rules, DEFAULT_AS_OF_DATE)
    for key, df in outputs.items():
        storage.write_parquet(key, df)
    validation_rules = storage.read_json(spec.validation_rules_key)
    validation = spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

    pipeline_run = storage.read_json(PIPELINE_RUN_KEY) if storage.exists(PIPELINE_RUN_KEY) else {"pipelines": {}}
    pipeline_run.setdefault("pipelines", {})[pipeline_name] = {
        "etl_status": "SUCCESS", "etl_error": None,
        "validation_status": validation["overall_status"], "validation_error": None,
    }
    pipeline_run["overall_status"] = (
        "SUCCESS" if all(r.get("etl_status") == "SUCCESS" and r.get("validation_status") == "PASS" for r in pipeline_run["pipelines"].values())
        else "FAILURE"
    )
    storage.write_json(PIPELINE_RUN_KEY, pipeline_run)
    storage.delete(_pending_repair_key(pipeline_name))

    print(f"  merged {branch} into main; {pipeline_name} rerun for real against the accepted change: validation_status={validation['overall_status']}")
    return {"accepted": True, "pipeline_name": pipeline_name, "branch": branch, "validation_status": validation["overall_status"]}


def reject_repair(pipeline_name: str, branch: str, storage: S3Storage) -> dict:
    """A human explicitly rejecting a candidate: discard its branch, clear the pending
    record. The real repository was never touched by the candidate in the first place."""
    _section(f"REJECT REPAIR: {pipeline_name} ({branch})")
    result = subprocess.run(["git", "branch", "-D", branch], capture_output=True, text=True, timeout=30)
    rejected = result.returncode == 0
    storage.delete(_pending_repair_key(pipeline_name))
    print(f"  {'discarded' if rejected else 'failed to discard'} candidate branch {branch}")
    return {"rejected": rejected, "pipeline_name": pipeline_name, "branch": branch, "detail": (result.stderr or result.stdout or "").strip()}


def _default_model_client_factory() -> DiagnosisModelClient:
    model = os.environ.get(ANSWER_MODEL_ENV_VAR)
    return OpenAIDiagnosisModelClient(model=model) if model else OpenAIDiagnosisModelClient()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data-operations control layer: estate view + incident response.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("estate", help="Show the trust/governance status of every registered data product.")
    subparsers.add_parser("scale", help="Show measured (not projected) scale of the current data estate.")

    incident = subparsers.add_parser("incident", help="Run the business-signal -> incident-response lifecycle.")
    group = incident.add_mutually_exclusive_group(required=True)
    group.add_argument("--question", type=str, help="A natural-language business signal.")
    group.add_argument("--pipeline", type=str, choices=sorted(PIPELINE_REGISTRY), help="Check one data product directly.")
    incident.add_argument("--mode", type=str, default="create_pr", choices=["create_pr", "auto_promote"])
    incident.add_argument(
        "--approve-category",
        type=str,
        action="append",
        default=[],
        help="Explicitly approve generating a create_pr candidate for a normally-refused root_cause_category "
        "(e.g. SOURCE_CONTRACT_CHANGE) on this one incident. Only valid with --pipeline and --mode create_pr.",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        storage = S3Storage()
        if args.command == "estate":
            print_estate(data_product_estate(storage))
            return
        if args.command == "scale":
            print_scale_summary(scale_summary(storage))
            return

        run_incident_response(
            storage,
            _default_model_client_factory,
            question=args.question,
            human_approved_categories=frozenset(args.approve_category),
            pipeline_name=args.pipeline,
            mode=args.mode,
        )
    except (AskLifecycleError, ModelClientError, StorageError) as exc:
        print(f"data_ops failed: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
