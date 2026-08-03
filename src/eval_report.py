"""Buckets this project's eval/test results into four categories the original spec requires
never be merged into one number: deterministic (no model, no real infrastructure), real
infrastructure (real S3/Spark, no model), scripted-model (the full diagnose -> repair ->
verify agent loop, exercised against real infrastructure with a canned model client -- zero
API cost, zero flakiness), and live-model (the same loops against a real model).

Every number here comes from something that was actually run:

- deterministic: src.eval_harness's own refusal_accuracy/context_extraction sections
  (curated/eval_report_latest.json) -- pure Python/regex logic, no Spark, no S3 writes, no
  model call.
- real_infrastructure: a real pytest run against real S3/Spark (no model calls), run fresh
  by this module unless explicitly skipped.
- scripted_model / live_model: real python3 -m demo.enterprise_incident --run-repair
  runs (see that module's docstring), discovered from curated/demo_runs/*.json -- each run's
  own `live_model` flag says which bucket it belongs to. src.eval_harness's scenario results
  (which always use a real model today) are folded into live_model too, clearly labeled by
  source.

A bucket with no real data behind it reports {"available": False} -- this module never
estimates, backfills, or fabricates a number for a category nothing has actually measured.

    python3 -m src.eval_report                          # deterministic + real-infra (fresh) + any persisted demo runs
    python3 -m src.eval_report --skip-real-infrastructure
    python3 -m src.eval_report --demo-manifest demo_output/run_manifest.json
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from src.storage import S3Storage, StorageError

REAL_INFRASTRUCTURE_TEST_PATHS = [
    "tests/test_etl_spark_loan_portfolio.py",
    "tests/test_etl_spark_campaign_funnel.py",
    "tests/test_etl_spark_underwriting_performance.py",
    "tests/test_etl_spark_payment_performance.py",
    "tests/test_etl_spark_delinquency_default.py",
    "tests/test_etl_spark_coupon_performance.py",
    "tests/test_migrate_lifecycle_to_s3.py",
    "tests/test_lifecycle_verify_repair.py",
    "tests/test_run_lifecycle_etl_pipelines.py",
]


class _ResultCollector:
    """A minimal pytest plugin: counts pass/fail/skip from real test outcomes, nothing else."""

    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def pytest_runtest_logreport(self, report) -> None:
        if report.when == "call":
            if report.passed:
                self.passed += 1
            elif report.failed:
                self.failed += 1
        elif report.when == "setup" and report.skipped:
            self.skipped += 1
        elif report.when == "setup" and report.failed:
            self.failed += 1


def run_real_infrastructure_tests(test_paths: list[str] | None = None) -> dict:
    """Actually runs a real-S3/real-Spark, no-model test subset in-process and reports what
    happened -- never a cached or assumed number. Skips cleanly (reports available=False)
    if pytest itself can't be imported for any reason."""
    try:
        import pytest
    except ImportError:
        return {"available": False, "reason": "pytest not importable"}

    paths = list(test_paths or REAL_INFRASTRUCTURE_TEST_PATHS)
    collector = _ResultCollector()
    start = time.monotonic()
    pytest.main(["-q", *paths], plugins=[collector])
    duration = time.monotonic() - start
    total = collector.passed + collector.failed + collector.skipped
    return {
        "available": True,
        "test_paths": paths,
        "passed": collector.passed,
        "failed": collector.failed,
        "skipped": collector.skipped,
        "total": total,
        "pass_rate": (collector.passed / total) if total else None,
        "duration_seconds": duration,
        "source": "measured just now via a live pytest run against real S3/Spark -- no model calls",
    }


def _deterministic_bucket(eval_harness_report: dict | None) -> dict:
    if eval_harness_report is None:
        return {"available": False}
    refusal = eval_harness_report.get("refusal_accuracy")
    context = eval_harness_report.get("context_extraction")
    if refusal is None and context is None:
        return {"available": False}
    bucket = {"available": True, "source": "curated/eval_report_latest.json (src.eval_harness) -- no model, no Spark/S3"}
    if refusal is not None:
        bucket["refusal_accuracy"] = refusal.get("accuracy")
        bucket["refusal_case_count"] = len(refusal.get("cases", []))
    if context is not None:
        bucket["context_extraction_overall_f1"] = context.get("overall_f1")
    return bucket


def _repair_verification_outcomes(manifest: dict) -> list[str | None]:
    """Every verification_status a demo run's investigate_and_repair stage produced (the
    refused-by-default check and, if reached, the human-approved one)."""
    outcomes = []
    for stage in manifest.get("stages", []):
        if stage.get("stage") != "investigate_and_repair":
            continue
        result = stage.get("result") or {}
        for key in ("refused", "approved"):
            sub = result.get(key) or {}
            self_heal = sub.get("self_heal") or {}
            verification = self_heal.get("repair_verification") or {}
            if verification:
                outcomes.append(verification.get("verification_status"))
    return outcomes


def _bucket_from_demo_manifests(manifests: list[dict], *, live_model: bool) -> dict:
    relevant = [m for m in manifests if bool(m.get("live_model")) == live_model]
    if not relevant:
        return {"available": False}
    outcomes: list[str | None] = []
    for manifest in relevant:
        outcomes.extend(_repair_verification_outcomes(manifest))
    label = "--live-model" if live_model else "--scripted-model"
    bucket = {
        "available": True,
        "runs_measured": len(relevant),
        "run_ids": [m.get("run_id") for m in relevant],
        "source": f"{len(relevant)} real `python3 -m demo.enterprise_incident {label}` run(s)",
    }
    if outcomes:
        bucket["repair_verification_outcomes"] = outcomes
        bucket["verified_pending_pr_or_verified_rate"] = sum(
            o in ("VERIFIED", "VERIFIED_PENDING_PR") for o in outcomes
        ) / len(outcomes)
    return bucket


def _merge_eval_harness_into_live_model(live_model_bucket: dict, eval_harness_report: dict | None) -> dict:
    """src.eval_harness.run_evals() always uses a real OpenAI model today (its diagnosis/
    repair client factories are hardcoded, not swappable to a scripted stand-in) -- so its
    scenario results are live-model evidence too, folded in here, clearly labeled by source
    rather than silently merged into a single unlabeled number."""
    if eval_harness_report is None or "summary" not in eval_harness_report:
        return live_model_bucket
    summary = eval_harness_report["summary"]
    merged = dict(live_model_bucket)
    merged["available"] = True
    merged["eval_harness_scenarios"] = {
        "scenario_count": summary.get("scenario_count"),
        "diagnosis_success_rate": summary.get("diagnosis_success_rate"),
        "repair_success_rate": summary.get("repair_success_rate"),
        "end_to_end_success_rate": summary.get("end_to_end_success_rate"),
        "avg_latency_seconds": summary.get("avg_latency_seconds"),
        "source": "curated/eval_report_latest.json (src.eval_harness) -- real OpenAI model calls",
    }
    return merged


def load_demo_manifests_from_s3(storage: S3Storage) -> list[dict]:
    try:
        keys = storage.list_paths("curated/demo_runs/")
    except StorageError:
        return []
    manifests = []
    for key in keys:
        if key.endswith(".json"):
            manifests.append(storage.read_json(key))
    return manifests


def build_eval_report(
    *,
    eval_harness_report: dict | None = None,
    demo_manifests: list[dict] | None = None,
    run_real_infrastructure: bool = True,
) -> dict:
    """Pure assembly of the four buckets from whatever real data is supplied. Does not read
    S3 or run pytest itself except for run_real_infrastructure -- callers (main(), a future
    API endpoint) decide how to source eval_harness_report/demo_manifests."""
    demo_manifests = demo_manifests or []
    live_model_bucket = _merge_eval_harness_into_live_model(
        _bucket_from_demo_manifests(demo_manifests, live_model=True), eval_harness_report
    )
    return {
        "deterministic": _deterministic_bucket(eval_harness_report),
        "real_infrastructure": run_real_infrastructure_tests() if run_real_infrastructure else {"available": False},
        "scripted_model": _bucket_from_demo_manifests(demo_manifests, live_model=False),
        "live_model": live_model_bucket,
    }


def print_eval_report(report: dict) -> None:
    print("Eval report (four categories, never merged)")
    for bucket_name in ("deterministic", "real_infrastructure", "scripted_model", "live_model"):
        bucket = report[bucket_name]
        print(f"\n  {bucket_name}:")
        if not bucket.get("available"):
            print("    not available -- nothing real has been measured for this category yet")
            continue
        for key, value in bucket.items():
            if key == "available":
                continue
            print(f"    {key}: {value}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bucket this project's eval/test results into four, never-merged categories.")
    parser.add_argument("--demo-manifest", action="append", default=[], help="Path to a demo run_manifest.json to fold in (repeatable).")
    parser.add_argument("--skip-real-infrastructure", action="store_true", help="Skip the live pytest run (faster; that bucket reports unavailable).")
    parser.add_argument("--skip-eval-harness", action="store_true", help="Don't read curated/eval_report_latest.json.")
    parser.add_argument("--skip-s3-demo-runs", action="store_true", help="Don't auto-discover curated/demo_runs/*.json.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()

    eval_harness_report = None
    if not args.skip_eval_harness and storage.exists("curated/eval_report_latest.json"):
        eval_harness_report = storage.read_json("curated/eval_report_latest.json")

    demo_manifests = [] if args.skip_s3_demo_runs else load_demo_manifests_from_s3(storage)
    for path in args.demo_manifest:
        demo_manifests.append(json.loads(Path(path).read_text()))

    report = build_eval_report(
        eval_harness_report=eval_harness_report,
        demo_manifests=demo_manifests,
        run_real_infrastructure=not args.skip_real_infrastructure,
    )
    storage.write_json("curated/eval_report_bucketed_latest.json", report)
    print_eval_report(report)


if __name__ == "__main__":
    main()
