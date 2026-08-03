"""Tests for src/data_ops.py -- the data-operations console's presentation/orchestration
layer. Against real S3 (skips cleanly if unreachable) since PIPELINE_REGISTRY and the real
FileContextStore's context/ directory are both real, global state this module reads by
design -- mocking them would test a fake registry, not this one. No live model calls:
run_incident_response's model-touching paths are exercised with ScriptedDiagnosisModelClient.
"""

from __future__ import annotations

import subprocess

import pytest

import src.data_ops as data_ops_module
from src.data_ops import (
    accept_repair,
    auto_scan_and_repair,
    data_product_estate,
    list_pending_repairs,
    print_estate,
    reject_repair,
    run_incident_response,
)
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.model_client import ModelResponse, ScriptedDiagnosisModelClient, ToolCall


def test_data_product_estate_has_one_row_per_registered_pipeline(s3_storage):
    rows = data_product_estate(s3_storage)
    assert {row["pipeline_name"] for row in rows} == set(PIPELINE_REGISTRY)
    for row in rows:
        assert row["context_provenance"] in ("human", "human+generated", "generated", "legacy_file")
        assert isinstance(row["open_conflicts"], int)


def test_print_estate_does_not_raise(s3_storage, capsys):
    print_estate(data_product_estate(s3_storage))
    out = capsys.readouterr().out
    assert "DATA PRODUCT ESTATE" in out
    assert "loan_portfolio" in out


def test_run_incident_response_requires_exactly_one_of_question_or_pipeline(s3_storage):
    def factory():
        raise AssertionError("no model call expected")

    with pytest.raises(ValueError):
        run_incident_response(s3_storage, factory)
    with pytest.raises(ValueError):
        run_incident_response(s3_storage, factory, question="q", pipeline_name="loan_portfolio")


def test_run_incident_response_reports_no_incident_for_a_healthy_pipeline(s3_storage, capsys):
    if not s3_storage.exists("curated/pipeline_run.json"):
        pytest.skip("curated lifecycle data not present in this environment")
    pipeline_run = s3_storage.read_json("curated/pipeline_run.json")
    entry = pipeline_run.get("pipelines", {}).get("loan_portfolio", {})
    if entry.get("etl_status") != "SUCCESS" or entry.get("validation_status") != "PASS":
        pytest.skip("loan_portfolio is not currently healthy in this environment")

    def factory():
        raise AssertionError("no model call needed for a trustworthy data product")

    result = run_incident_response(s3_storage, factory, pipeline_name="loan_portfolio", mode="create_pr")
    assert result == {"pipeline_name": "loan_portfolio", "self_heal": None, "candidate_answer": None}
    assert "PASS -- no incident" in capsys.readouterr().out


def test_run_incident_response_narrates_a_grounded_healthy_question(s3_storage, capsys):
    if not s3_storage.exists("curated/loan_portfolio.parquet"):
        pytest.skip("curated lifecycle data not present in this environment")
    real_value = s3_storage.read_parquet("curated/loan_portfolio.parquet").iloc[0]["total_outstanding_principal"]

    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_loan_portfolio_summary", arguments={})]),
        ModelResponse(
            tool_calls=[
                ToolCall(
                    id="2",
                    name="submit_answer",
                    arguments={
                        "answer_status": "ANSWERED",
                        "question": "What is our total outstanding principal?",
                        "answer_summary": f"Total outstanding principal is {real_value}.",
                        "as_of_date": "2026-07-20",
                        "cited_metrics": [
                            {
                                "metric_name": "total_outstanding_principal",
                                "value": real_value,
                                "source_reference": "get_loan_portfolio_summary",
                                "row_identifier": None,
                            }
                        ],
                        "caveats": [],
                    },
                )
            ]
        ),
    ]
    client = ScriptedDiagnosisModelClient(responses)
    result = run_incident_response(
        s3_storage, lambda: client, question="What is our total outstanding principal?", mode="create_pr"
    )
    assert result["answer"]["answer_status"] == "ANSWERED"
    assert result["self_heal"] is None
    out = capsys.readouterr().out
    assert "PROVENANCE: loan_portfolio.total_outstanding_principal" in out
    assert "Metric definition:" in out


def test_run_incident_response_question_path_persists_a_pending_repair(s3_storage, monkeypatch):
    """A business question that triggers a self-heal reaching VERIFIED_PENDING_PR must show
    up in the Repairs tab the same way a direct pipeline_name check does -- not just get
    narrated and forgotten."""
    key = "curated/pending_repairs/loan_portfolio.json"
    if s3_storage.exists(key):
        s3_storage.delete(key)

    fake_result = {
        "question": "What is our total outstanding principal?",
        "relevant_pipelines": ["loan_portfolio"],
        "validation_failures": {"loan_portfolio": ["total_outstanding_principal_status_vocabulary_drift"]},
        "answer": {"answer_status": "UNRELIABLE_DATA", "question": "x", "answer_summary": "x", "as_of_date": None, "cited_metrics": [], "caveats": []},
        "self_heal": {
            "loan_portfolio": {
                "diagnosis": {"diagnosis_status": "DIAGNOSED", "root_cause_category": "SOURCE_CONTRACT_CHANGE", "confidence": "HIGH", "root_cause": "x", "evidence": []},
                "repair_result": {"repair_status": "APPLIED", "target_file": "context/pipeline_rules/loan_portfolio.json"},
                "repair_verification": {"verification_status": "VERIFIED_PENDING_PR", "summary": "ok", "pr_artifact": {"branch": "repair/xyz"}, "metrics_after": {}},
            }
        },
        "corrected_answer": None,
    }
    monkeypatch.setattr(data_ops_module, "answer_lifecycle_question", lambda question, storage, factory, mode="create_pr": fake_result)
    monkeypatch.setattr(
        data_ops_module,
        "answer_from_candidate",
        lambda question, storage, factory, pipeline_name, metrics_after: {"answer_status": "ANSWERED", "answer_summary": "candidate answer"},
    )

    try:
        result = run_incident_response(s3_storage, lambda: None, question="What is our total outstanding principal?", mode="create_pr")
        assert result["candidate_answer"]["answer_summary"] == "candidate answer"
        assert s3_storage.exists(key)
        assert s3_storage.read_json(key)["pr_artifact"]["branch"] == "repair/xyz"
    finally:
        if s3_storage.exists(key):
            s3_storage.delete(key)


# --- Automatic detection (auto_scan_and_repair) + pending-repair tracking ------------------


@pytest.fixture
def clean_pending_loan_portfolio(s3_storage):
    """loan_portfolio is the only pipeline these tests write a pending record for -- always
    clean before and after so a failed test run can't leave a stale record behind."""
    key = "curated/pending_repairs/loan_portfolio.json"
    if s3_storage.exists(key):
        s3_storage.delete(key)
    yield key
    if s3_storage.exists(key):
        s3_storage.delete(key)


def _all_healthy_rows():
    return [
        {"pipeline_name": name, "etl_status": "SUCCESS", "validation_status": "PASS", "context_provenance": "human", "review_status": None, "open_conflicts": 0}
        for name in PIPELINE_REGISTRY
    ]


def test_list_pending_repairs_reflects_persisted_records(s3_storage, clean_pending_loan_portfolio):
    assert list_pending_repairs(s3_storage) == []
    record = {"pipeline_name": "loan_portfolio", "status": "pending_review"}
    s3_storage.write_json(clean_pending_loan_portfolio, record)
    assert list_pending_repairs(s3_storage) == [record]


def test_auto_scan_skips_healthy_pipelines_with_no_pending_record(s3_storage, monkeypatch):
    monkeypatch.setattr(data_ops_module, "data_product_estate", lambda storage: _all_healthy_rows())

    def boom(*a, **k):
        raise AssertionError("must not attempt self-heal for a healthy pipeline")

    monkeypatch.setattr(data_ops_module, "_attempt_self_heal", boom)
    assert auto_scan_and_repair(s3_storage, lambda: None) == []


def test_auto_scan_clears_a_stale_pending_record_once_healthy(s3_storage, clean_pending_loan_portfolio, monkeypatch):
    s3_storage.write_json(clean_pending_loan_portfolio, {"pipeline_name": "loan_portfolio", "status": "pending_review"})
    monkeypatch.setattr(data_ops_module, "data_product_estate", lambda storage: _all_healthy_rows())

    def boom(*a, **k):
        raise AssertionError("no model call expected for a healthy pipeline")

    monkeypatch.setattr(data_ops_module, "_attempt_self_heal", boom)

    results = auto_scan_and_repair(s3_storage, lambda: None)

    assert results == [{"pipeline_name": "loan_portfolio", "status": "resolved_externally"}]
    assert not s3_storage.exists(clean_pending_loan_portfolio)


def test_auto_scan_reports_already_pending_without_rerunning(s3_storage, clean_pending_loan_portfolio, monkeypatch):
    pending_record = {"pipeline_name": "loan_portfolio", "status": "pending_review", "pr_artifact": {"branch": "repair/abc"}}
    s3_storage.write_json(clean_pending_loan_portfolio, pending_record)

    untrusted_rows = [
        {"pipeline_name": "loan_portfolio", "etl_status": "SUCCESS", "validation_status": "FAIL", "context_provenance": "human", "review_status": None, "open_conflicts": 0}
    ]
    monkeypatch.setattr(data_ops_module, "data_product_estate", lambda storage: untrusted_rows)

    def boom(*a, **k):
        raise AssertionError("must not re-diagnose/re-repair a pipeline with an existing pending candidate")

    monkeypatch.setattr(data_ops_module, "_attempt_self_heal", boom)

    results = auto_scan_and_repair(s3_storage, lambda: None)
    assert results == [{"pipeline_name": "loan_portfolio", "status": "already_pending", "pending": pending_record}]


def test_auto_scan_generates_and_persists_a_new_candidate(s3_storage, clean_pending_loan_portfolio, monkeypatch):
    untrusted_rows = [
        {"pipeline_name": "loan_portfolio", "etl_status": "SUCCESS", "validation_status": "FAIL", "context_provenance": "human", "review_status": None, "open_conflicts": 0}
    ]
    monkeypatch.setattr(data_ops_module, "data_product_estate", lambda storage: untrusted_rows)

    fake_heal = {
        "diagnosis": {"diagnosis_status": "DIAGNOSED", "root_cause_category": "SOURCE_CONTRACT_CHANGE", "confidence": "HIGH", "root_cause": "x", "evidence": []},
        "repair_result": {"repair_status": "APPLIED", "target_file": "context/pipeline_rules/loan_portfolio.json"},
        "repair_verification": {"verification_status": "VERIFIED_PENDING_PR", "summary": "ok", "pr_artifact": {"branch": "repair/xyz", "risk_classification": "HIGH"}},
    }
    seen_kwargs = {}

    def fake_attempt_self_heal(pipeline_name, storage, factory, **kwargs):
        seen_kwargs.update(kwargs)
        return fake_heal

    monkeypatch.setattr(data_ops_module, "_attempt_self_heal", fake_attempt_self_heal)

    results = auto_scan_and_repair(s3_storage, lambda: None)

    assert seen_kwargs["mode"] == "create_pr"
    assert seen_kwargs["human_approved_categories"] == frozenset({"SOURCE_CONTRACT_CHANGE"})
    assert len(results) == 1
    assert results[0]["status"] == "pending_review"
    assert results[0]["pr_artifact"]["branch"] == "repair/xyz"
    assert s3_storage.exists(clean_pending_loan_portfolio)
    assert s3_storage.read_json(clean_pending_loan_portfolio)["pr_artifact"]["branch"] == "repair/xyz"


# --- accept_repair / reject_repair (subprocess mocked -- never touches real git) -----------


class _FakeAcceptStorage:
    def __init__(self, files: dict) -> None:
        self.files = dict(files)
        self.written_parquet: dict = {}

    def read_json(self, path: str) -> dict:
        return self.files[path]

    def write_json(self, path: str, value) -> None:
        self.files[path] = value

    def exists(self, path: str) -> bool:
        return path in self.files

    def delete(self, path: str) -> None:
        self.files.pop(path, None)

    def write_parquet(self, path: str, dataframe) -> None:
        self.written_parquet[path] = dataframe


def _fake_spec(pipeline_configuration_file=None):
    return type(
        "Spec",
        (),
        {
            "etl_source_file": "src.etl_spark_loan_portfolio",  # module-path-shaped; run_etl is stubbed so it's never imported
            "validation_rules_key": "context/validations/loan_portfolio.json",
            "pipeline_configuration_file": pipeline_configuration_file,
            "run_etl": staticmethod(lambda module, spark, business_rules, as_of_date: {"curated/loan_portfolio.parquet": f"df-for-{business_rules.get('marker')}"}),
            "run_validate": staticmethod(lambda storage, business_rules, validation_rules, as_of_date: {"overall_status": "PASS", "checks": []}),
        },
    )()


def test_accept_repair_reports_git_merge_failure_without_touching_anything_else(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "git", stderr="conflict"))
    )
    storage = _FakeAcceptStorage({})
    result = accept_repair("loan_portfolio", "repair/abc", storage, spark="fake-spark")
    assert result["accepted"] is False
    assert "conflict" in result["error"]


def test_accept_repair_merges_resolves_pointer_and_reruns(monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: calls.append(a) or subprocess.CompletedProcess(a, 0))
    monkeypatch.setattr(data_ops_module, "importlib", type("M", (), {"import_module": staticmethod(lambda name: "fake-module")}))

    fake_migrate_context_calls = []
    monkeypatch.setattr("src.migrate_lifecycle_to_s3.migrate_context", lambda storage: fake_migrate_context_calls.append(1))

    spec = _fake_spec(pipeline_configuration_file="context/pipeline_rules/loan_portfolio.json")
    monkeypatch.setattr(data_ops_module, "PIPELINE_REGISTRY", {"loan_portfolio": spec})

    storage = _FakeAcceptStorage(
        {
            "context/business_rules.json": {"marker": "stale"},
            "context/pipeline_rules/loan_portfolio.json": {"business_rules_file": "context/business_rules_demo.json"},
            "context/business_rules_demo.json": {"marker": "adopted"},
            "context/validations/loan_portfolio.json": {"rules": []},
            "curated/pending_repairs/loan_portfolio.json": {"pipeline_name": "loan_portfolio"},
        }
    )

    result = accept_repair("loan_portfolio", "repair/abc", storage, spark="fake-spark")

    assert result["accepted"] is True
    assert result["validation_status"] == "PASS"
    assert storage.written_parquet["curated/loan_portfolio.parquet"] == "df-for-adopted"  # resolved via the pointer, not the stale file
    assert storage.read_json("curated/pipeline_run.json")["pipelines"]["loan_portfolio"]["validation_status"] == "PASS"
    assert not storage.exists("curated/pending_repairs/loan_portfolio.json")
    assert fake_migrate_context_calls == [1]
    assert any(c[0][:2] == ["git", "merge"] for c in calls)
    assert any(c[0][:2] == ["git", "branch"] for c in calls)


def test_accept_repair_falls_back_to_applying_stored_diff_when_branch_merge_fails(monkeypatch):
    """The branch that created a candidate may live in a different pod's ephemeral git repo
    than whichever pod is serving this accept call (confirmed live, ROSA, 2026-07-29) -- this
    is the common case once create_candidate_repair/accept_repair can run in different
    processes, not an edge case."""
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        if args[:2] == ["git", "merge"]:
            raise subprocess.CalledProcessError(1, "git", stderr="repair/abc - not something we can merge")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(data_ops_module, "importlib", type("M", (), {"import_module": staticmethod(lambda name: "fake-module")}))
    monkeypatch.setattr("src.migrate_lifecycle_to_s3.migrate_context", lambda storage: None)

    spec = _fake_spec(pipeline_configuration_file=None)
    monkeypatch.setattr(data_ops_module, "PIPELINE_REGISTRY", {"loan_portfolio": spec})

    storage = _FakeAcceptStorage(
        {
            "context/business_rules.json": {"marker": "stale"},
            "context/validations/loan_portfolio.json": {"rules": []},
            "curated/pending_repairs/loan_portfolio.json": {
                "pipeline_name": "loan_portfolio",
                "pr_artifact": {
                    "diff": "--- a/context/business_rules.json\n+++ b/context/business_rules.json\n@@ -1 +1 @@\n-old\n+new\n",
                    "target_file": "context/business_rules.json",
                },
            },
        }
    )

    result = accept_repair("loan_portfolio", "repair/abc", storage, spark="fake-spark")

    assert result["accepted"] is True
    assert result["validation_status"] == "PASS"
    assert not storage.exists("curated/pending_repairs/loan_portfolio.json")
    assert any(c[:2] == ["git", "merge"] for c in calls)
    assert any(c[:2] == ["git", "apply"] for c in calls)
    assert any(c[:2] == ["git", "add"] for c in calls)
    assert any(c[:2] == ["git", "commit"] for c in calls)
    assert not any(c[:2] == ["git", "branch"] for c in calls)  # nothing to delete locally -- the branch never existed here


def test_accept_repair_reports_original_merge_error_when_no_stored_diff_available(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "git", stderr="not something we can merge"))
    )
    storage = _FakeAcceptStorage({"curated/pending_repairs/loan_portfolio.json": {"pipeline_name": "loan_portfolio"}})  # no pr_artifact
    result = accept_repair("loan_portfolio", "repair/abc", storage, spark="fake-spark")
    assert result["accepted"] is False
    assert "not something we can merge" in result["error"]


def test_reject_repair_deletes_branch_and_clears_pending_record(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout="", stderr=""))
    storage = _FakeAcceptStorage({"curated/pending_repairs/loan_portfolio.json": {"pipeline_name": "loan_portfolio"}})

    result = reject_repair("loan_portfolio", "repair/abc", storage)

    assert result["rejected"] is True
    assert not storage.exists("curated/pending_repairs/loan_portfolio.json")
