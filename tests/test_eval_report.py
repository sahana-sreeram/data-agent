"""Tests for src/eval_report.py's bucketing logic. Pure functions -- no real S3/Spark/model
calls; run_real_infrastructure=False everywhere here so tests never trigger a nested pytest
run. run_real_infrastructure_tests itself (the one function that does run pytest for real)
is exercised live in this session's manual verification, not here -- a test suite
recursively invoking itself is exactly the kind of thing to avoid.
"""

from __future__ import annotations

from src.eval_report import build_eval_report, load_demo_manifests_from_s3

EVAL_HARNESS_REPORT = {
    "refusal_accuracy": {"accuracy": 1.0, "cases": [{"name": "a"}, {"name": "b"}]},
    "context_extraction": {"overall_f1": 0.92},
    "summary": {
        "scenario_count": 4,
        "diagnosis_success_rate": 1.0,
        "repair_success_rate": 1.0,
        "end_to_end_success_rate": 1.0,
        "avg_latency_seconds": {"diagnosis": 10.0},
    },
}


def _demo_manifest(*, live_model: bool, run_id: str, verification_statuses: list) -> dict:
    stages = []
    if verification_statuses:
        stages.append(
            {
                "stage": "investigate_and_repair",
                "result": {
                    "refused": {"self_heal": {"repair_verification": {"verification_status": verification_statuses[0]}}},
                    "approved": (
                        {"self_heal": {"repair_verification": {"verification_status": verification_statuses[1]}}}
                        if len(verification_statuses) > 1
                        else {}
                    ),
                },
            }
        )
    return {"run_id": run_id, "live_model": live_model, "stages": stages}


def test_all_buckets_report_unavailable_with_no_real_data():
    report = build_eval_report(run_real_infrastructure=False)
    for name in ("deterministic", "real_infrastructure", "scripted_model", "live_model"):
        assert report[name] == {"available": False}


def test_deterministic_bucket_reads_refusal_and_context_extraction():
    report = build_eval_report(eval_harness_report=EVAL_HARNESS_REPORT, run_real_infrastructure=False)
    deterministic = report["deterministic"]
    assert deterministic["available"] is True
    assert deterministic["refusal_accuracy"] == 1.0
    assert deterministic["refusal_case_count"] == 2
    assert deterministic["context_extraction_overall_f1"] == 0.92


def test_scripted_and_live_model_manifests_are_bucketed_separately():
    manifests = [
        _demo_manifest(live_model=False, run_id="scripted-1", verification_statuses=["BLOCKED", "VERIFIED_PENDING_PR"]),
        _demo_manifest(live_model=True, run_id="live-1", verification_statuses=["BLOCKED", "NOT_VERIFIED"]),
    ]
    report = build_eval_report(demo_manifests=manifests, run_real_infrastructure=False)

    scripted = report["scripted_model"]
    assert scripted["available"] is True
    assert scripted["runs_measured"] == 1
    assert scripted["run_ids"] == ["scripted-1"]
    assert scripted["repair_verification_outcomes"] == ["BLOCKED", "VERIFIED_PENDING_PR"]
    assert scripted["verified_pending_pr_or_verified_rate"] == 0.5

    live = report["live_model"]
    assert live["available"] is True
    assert live["runs_measured"] == 1
    assert live["run_ids"] == ["live-1"]
    assert live["repair_verification_outcomes"] == ["BLOCKED", "NOT_VERIFIED"]
    assert live["verified_pending_pr_or_verified_rate"] == 0.0

    # never merged -- a scripted run must never contribute to the live-model bucket or vice versa
    assert "eval_harness_scenarios" not in scripted


def test_eval_harness_scenarios_fold_into_live_model_not_scripted_model():
    report = build_eval_report(eval_harness_report=EVAL_HARNESS_REPORT, run_real_infrastructure=False)
    assert report["scripted_model"] == {"available": False}
    live = report["live_model"]
    assert live["available"] is True
    assert live["eval_harness_scenarios"]["scenario_count"] == 4
    assert live["eval_harness_scenarios"]["diagnosis_success_rate"] == 1.0


def test_load_demo_manifests_from_s3_skips_cleanly_on_storage_error():
    class _RaisingStorage:
        def list_paths(self, prefix):
            from src.storage import StorageError

            raise StorageError("unreachable")

    assert load_demo_manifests_from_s3(_RaisingStorage()) == []
