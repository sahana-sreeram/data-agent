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


def test_index_html_is_served_at_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "Lifecycle Data Agent" in response.text


def test_static_assets_are_served():
    assert client.get("/app.js").status_code == 200
    assert client.get("/style.css").status_code == 200
