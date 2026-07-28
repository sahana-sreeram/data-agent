"""Tests for src/api.py -- the FastAPI layer over src/ask_lifecycle.py. No live model/S3/Spark
calls: answer_lifecycle_question and S3Storage are monkeypatched, mirroring how
tests/test_ask_lifecycle.py already stubs storage and self-heal.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

import src.api as api_module
from src.ask_lifecycle import AskLifecycleError
from src.model_client import ModelClientError

client = TestClient(api_module.app)

FAKE_ANSWER_RESULT = {
    "question": "What is the total outstanding principal?",
    "relevant_pipelines": ["loan_portfolio"],
    "validation_failures": {},
    "answer": {
        "answer_status": "ANSWERED",
        "question": "What is the total outstanding principal?",
        "answer_summary": "1234.0",
        "as_of_date": "2026-07-20",
        "cited_metrics": [{"metric_name": "total_outstanding_principal", "value": 1234.0, "source_reference": "get_loan_portfolio_summary"}],
        "caveats": [],
    },
    "self_heal": None,
    "corrected_answer": None,
}


def test_ask_returns_the_full_answer_payload(monkeypatch):
    monkeypatch.setattr(api_module, "answer_lifecycle_question", lambda question, storage, factory: FAKE_ANSWER_RESULT)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post("/api/ask", json={"question": "What is the total outstanding principal?"})

    assert response.status_code == 200
    body = response.json()
    assert body["answer"]["answer_status"] == "ANSWERED"
    assert body["relevant_pipelines"] == ["loan_portfolio"]
    assert body["self_heal"] is None


def test_ask_rejects_an_empty_question():
    response = client.post("/api/ask", json={"question": "   "})
    assert response.status_code == 400


def test_ask_lifecycle_error_becomes_503(monkeypatch):
    def _raise(question, storage, factory):
        raise AskLifecycleError("curated/pipeline_run.json not found")

    monkeypatch.setattr(api_module, "answer_lifecycle_question", _raise)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post("/api/ask", json={"question": "anything"})

    assert response.status_code == 503
    assert "pipeline_run.json" in response.json()["detail"]


def test_model_client_error_becomes_502(monkeypatch):
    def _raise(question, storage, factory):
        raise ModelClientError("OPENAI_API_KEY is not set")

    monkeypatch.setattr(api_module, "answer_lifecycle_question", _raise)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post("/api/ask", json={"question": "anything"})

    assert response.status_code == 502


def test_health_returns_pipeline_run_contents(monkeypatch):
    class _FakeStorage:
        def exists(self, path: str) -> bool:
            return True

        def read_json(self, path: str) -> dict:
            return {"overall_status": "SUCCESS", "pipelines": {"loan_portfolio": {"etl_status": "SUCCESS", "validation_status": "PASS"}}}

    monkeypatch.setattr(api_module, "S3Storage", _FakeStorage)

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json()["overall_status"] == "SUCCESS"


def test_health_missing_pipeline_run_is_503(monkeypatch):
    class _FakeStorage:
        def exists(self, path: str) -> bool:
            return False

    monkeypatch.setattr(api_module, "S3Storage", _FakeStorage)

    response = client.get("/api/health")

    assert response.status_code == 503


def test_estate_returns_data_product_estate(monkeypatch):
    fake_rows = [{"pipeline_name": "loan_portfolio", "etl_status": "SUCCESS", "validation_status": "PASS", "context_provenance": "human+generated", "review_status": "UNREVIEWED", "open_conflicts": 0}]
    monkeypatch.setattr(api_module, "data_product_estate", lambda storage: fake_rows)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.get("/api/estate")

    assert response.status_code == 200
    assert response.json() == {"pipelines": fake_rows}


def test_scale_returns_scale_summary(monkeypatch):
    fake_summary = {"customers": 100, "raw_table_row_counts": {}, "raw_total_rows": 0, "storage": {}, "registered_pipelines": 6, "upstream_services": 6}
    monkeypatch.setattr(api_module, "scale_summary", lambda storage: fake_summary)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.get("/api/scale")

    assert response.status_code == 200
    assert response.json() == fake_summary


def test_context_detail_rejects_an_unknown_pipeline():
    response = client.get("/api/context/not_a_real_pipeline")
    assert response.status_code == 404


def test_incident_requires_exactly_one_of_question_or_pipeline_name():
    assert client.post("/api/incident", json={}).status_code == 400
    assert client.post("/api/incident", json={"question": "x", "pipeline_name": "loan_portfolio"}).status_code == 400


def test_incident_rejects_an_unknown_pipeline_name():
    response = client.post("/api/incident", json={"pipeline_name": "not_a_real_pipeline"})
    assert response.status_code == 404


def test_incident_delegates_to_run_incident_response(monkeypatch):
    fake_result = {"pipeline_name": "loan_portfolio", "self_heal": None, "candidate_answer": None}
    seen = {}

    def _fake_run_incident_response(storage, factory, *, question=None, pipeline_name=None, mode="create_pr", human_approved_categories=frozenset(), repair_model_client_factory=None):
        seen.update(pipeline_name=pipeline_name, mode=mode, human_approved_categories=human_approved_categories)
        return fake_result

    monkeypatch.setattr(api_module, "run_incident_response", _fake_run_incident_response)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post(
        "/api/incident",
        json={"pipeline_name": "loan_portfolio", "mode": "create_pr", "approve_categories": ["SOURCE_CONTRACT_CHANGE"]},
    )

    assert response.status_code == 200
    assert response.json() == fake_result
    assert seen["pipeline_name"] == "loan_portfolio"
    assert seen["human_approved_categories"] == frozenset({"SOURCE_CONTRACT_CHANGE"})


def test_evaluations_reports_all_four_buckets_unavailable_when_nothing_has_run(monkeypatch):
    class _FakeStorage:
        def exists(self, path: str) -> bool:
            return False

        def list_paths(self, prefix: str) -> list:
            return []

    monkeypatch.setattr(api_module, "S3Storage", _FakeStorage)

    response = client.get("/api/evaluations")

    assert response.status_code == 200
    body = response.json()
    for bucket_name in ("deterministic", "real_infrastructure", "scripted_model", "live_model"):
        assert body[bucket_name] == {"available": False}


def test_evaluations_folds_eval_harness_report_and_persisted_real_infra_result(monkeypatch):
    class _FakeStorage:
        def exists(self, path: str) -> bool:
            return path in ("curated/eval_report_latest.json", "curated/eval_report_bucketed_latest.json")

        def list_paths(self, prefix: str) -> list:
            return []

        def read_json(self, path: str) -> dict:
            if path == "curated/eval_report_latest.json":
                return {"refusal_accuracy": {"accuracy": 1.0, "cases": []}, "summary": {"scenario_count": 4}}
            assert path == "curated/eval_report_bucketed_latest.json"
            return {"real_infrastructure": {"available": True, "passed": 44, "failed": 0}}

    monkeypatch.setattr(api_module, "S3Storage", _FakeStorage)

    response = client.get("/api/evaluations")

    assert response.status_code == 200
    body = response.json()
    assert body["deterministic"]["refusal_accuracy"] == 1.0
    assert body["real_infrastructure"] == {"available": True, "passed": 44, "failed": 0}
    assert body["live_model"]["eval_harness_scenarios"]["scenario_count"] == 4


def test_incidents_scan_delegates_to_auto_scan_and_repair(monkeypatch):
    fake_results = [{"pipeline_name": "loan_portfolio", "status": "pending_review"}]
    monkeypatch.setattr(api_module, "auto_scan_and_repair", lambda storage, factory: fake_results)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post("/api/incidents/scan")

    assert response.status_code == 200
    assert response.json() == {"results": fake_results}


def test_incidents_scan_with_scripted_model_restricts_to_loan_portfolio(monkeypatch):
    seen_kwargs = {}

    def fake_auto_scan(storage, diagnosis_factory, repair_factory=None, pipeline_names=None):
        seen_kwargs["diagnosis_factory"] = diagnosis_factory
        seen_kwargs["repair_factory"] = repair_factory
        seen_kwargs["pipeline_names"] = pipeline_names
        return []

    monkeypatch.setattr(api_module, "auto_scan_and_repair", fake_auto_scan)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post("/api/incidents/scan", json={"use_scripted_model": True})

    assert response.status_code == 200
    assert seen_kwargs["pipeline_names"] == frozenset({"loan_portfolio"})
    assert seen_kwargs["diagnosis_factory"] is api_module._scripted_diagnosis_client_factory
    assert seen_kwargs["repair_factory"] is api_module._scripted_repair_client_factory


def test_incident_use_scripted_model_rejects_a_non_loan_portfolio_pipeline():
    response = client.post(
        "/api/incident",
        json={"pipeline_name": "payment_performance", "mode": "create_pr", "use_scripted_model": True},
    )
    assert response.status_code == 400
    assert "loan_portfolio" in response.json()["detail"]


def test_incident_use_scripted_model_passes_scripted_factories(monkeypatch):
    seen_kwargs = {}

    def fake_run_incident_response(storage, diagnosis_factory, *, question=None, pipeline_name=None, mode="create_pr", human_approved_categories=frozenset(), repair_model_client_factory=None):
        seen_kwargs["diagnosis_factory"] = diagnosis_factory
        seen_kwargs["repair_model_client_factory"] = repair_model_client_factory
        return {"pipeline_name": pipeline_name, "self_heal": None, "candidate_answer": None}

    monkeypatch.setattr(api_module, "run_incident_response", fake_run_incident_response)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post(
        "/api/incident",
        json={"pipeline_name": "loan_portfolio", "mode": "create_pr", "use_scripted_model": True},
    )

    assert response.status_code == 200
    assert seen_kwargs["diagnosis_factory"] is api_module._scripted_diagnosis_client_factory
    assert seen_kwargs["repair_model_client_factory"] is api_module._scripted_repair_client_factory


def test_repairs_pending_returns_list_pending_repairs(monkeypatch):
    fake_pending = [{"pipeline_name": "loan_portfolio", "status": "pending_review"}]
    monkeypatch.setattr(api_module, "list_pending_repairs", lambda storage: fake_pending)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.get("/api/repairs/pending")

    assert response.status_code == 200
    assert response.json() == {"pending": fake_pending}


def test_repairs_accept_rejects_an_unknown_pipeline():
    response = client.post("/api/repairs/accept", json={"pipeline_name": "not_a_real_pipeline", "branch": "repair/abc"})
    assert response.status_code == 404


def test_repairs_accept_returns_409_when_accept_repair_fails(monkeypatch):
    monkeypatch.setattr(api_module, "accept_repair", lambda pipeline_name, branch, storage, spark: {"accepted": False, "error": "git merge failed: conflict"})
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    class _FakeSpark:
        def stop(self):
            pass

        class sparkContext:
            @staticmethod
            def setLogLevel(level):
                pass

    monkeypatch.setattr("src.spark_session.get_spark_session", lambda name: _FakeSpark())

    response = client.post("/api/repairs/accept", json={"pipeline_name": "loan_portfolio", "branch": "repair/abc"})

    assert response.status_code == 409
    assert "conflict" in response.json()["detail"]


def test_repairs_accept_returns_the_result_on_success(monkeypatch):
    fake_result = {"accepted": True, "pipeline_name": "loan_portfolio", "branch": "repair/abc", "validation_status": "PASS"}
    monkeypatch.setattr(api_module, "accept_repair", lambda pipeline_name, branch, storage, spark: fake_result)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    class _FakeSpark:
        def stop(self):
            pass

        class sparkContext:
            @staticmethod
            def setLogLevel(level):
                pass

    monkeypatch.setattr("src.spark_session.get_spark_session", lambda name: _FakeSpark())

    response = client.post("/api/repairs/accept", json={"pipeline_name": "loan_portfolio", "branch": "repair/abc"})

    assert response.status_code == 200
    assert response.json() == fake_result


def test_repairs_reject_rejects_an_unknown_pipeline():
    response = client.post("/api/repairs/reject", json={"pipeline_name": "not_a_real_pipeline", "branch": "repair/abc"})
    assert response.status_code == 404


def test_repairs_reject_delegates_to_reject_repair(monkeypatch):
    fake_result = {"rejected": True, "pipeline_name": "loan_portfolio", "branch": "repair/abc", "detail": ""}
    monkeypatch.setattr(api_module, "reject_repair", lambda pipeline_name, branch, storage: fake_result)
    monkeypatch.setattr(api_module, "S3Storage", lambda: object())

    response = client.post("/api/repairs/reject", json={"pipeline_name": "loan_portfolio", "branch": "repair/abc"})

    assert response.status_code == 200
    assert response.json() == fake_result


def test_index_html_is_served_at_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "Data Operations Console" in response.text


def test_static_assets_are_served():
    assert client.get("/app.js").status_code == 200
    assert client.get("/style.css").status_code == 200
