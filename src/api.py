"""FastAPI web app over the existing data-operations system -- lets a browser walk the same
business-signal -> trust-check -> incident -> diagnosis -> repair -> verify -> PR-artifact
lifecycle src/data_ops.py already narrates on a terminal, plus a whole-estate overview and
per-pipeline context/provenance view. Purely a presentation layer: every fact it serves
already exists in an existing function's return value (src.ask_lifecycle.
answer_lifecycle_question, src.data_ops.data_product_estate/run_incident_response,
src.context_retriever.ContextRetriever) -- this adds no new business logic.

Run with `python3 -m src.api`, then open http://127.0.0.1:8000/. /api/ask is unchanged and
still answers a single natural-language question directly (used by the Incidents tab's
business-signal form); /api/incident is the richer, data_ops-backed entry point that also
supports checking one data product directly and an operator-approved repair override.
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.ask_lifecycle import ANSWER_MODEL_ENV_VAR, AskLifecycleError, answer_lifecycle_question
from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.data_ops import data_product_estate, run_incident_response, scale_summary
from src.eval_report import build_eval_report, load_demo_manifests_from_s3
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.storage import S3Storage, StorageError

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")

app = FastAPI(title="Data Operations Console")


class AskRequest(BaseModel):
    question: str


class IncidentRequest(BaseModel):
    question: str | None = None
    pipeline_name: str | None = None
    mode: str = "create_pr"
    approve_categories: list[str] = []


def _model_client_factory() -> DiagnosisModelClient:
    answer_model = os.environ.get(ANSWER_MODEL_ENV_VAR)
    return OpenAIDiagnosisModelClient(model=answer_model) if answer_model else OpenAIDiagnosisModelClient()


@app.post("/api/ask")
def ask(request: AskRequest) -> dict:
    """Answer a business question. Can take anywhere from a few seconds (no self-heal
    needed) to a few minutes (a relevant pipeline is broken and gets diagnosed, repaired, and
    verified live) -- this is a sync endpoint (FastAPI runs it in its threadpool), and
    intentionally has no artificial timeout; the frontend shows a spinner and waits."""
    if not request.question or not request.question.strip():
        raise HTTPException(status_code=400, detail="question must not be empty")
    try:
        storage = S3Storage()
        return answer_lifecycle_question(request.question, storage, _model_client_factory)
    except AskLifecycleError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except (ModelClientError, StorageError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/health")
def health() -> dict:
    """curated/pipeline_run.json as-is, for a lightweight status banner."""
    try:
        storage = S3Storage()
        if not storage.exists("curated/pipeline_run.json"):
            raise HTTPException(status_code=503, detail="curated/pipeline_run.json not found -- run python3 -m src.run_lifecycle_etl_pipelines first")
        return storage.read_json("curated/pipeline_run.json")
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/estate")
def estate() -> dict:
    """One row per registered data product: trust status + context-governance status --
    src.data_ops.data_product_estate, unchanged."""
    try:
        storage = S3Storage()
        return {"pipelines": data_product_estate(storage)}
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/scale")
def scale() -> dict:
    """Measured (never projected) scale of the current data estate -- src.data_ops.scale_summary."""
    try:
        storage = S3Storage()
        return scale_summary(storage)
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/context/{pipeline_name}")
def context_detail(pipeline_name: str) -> dict:
    """Per-metric provenance (definition/lineage/pipeline implementation/current health),
    the same ContextRetriever facts src.data_ops._print_provenance narrates on a terminal."""
    if pipeline_name not in PIPELINE_REGISTRY:
        raise HTTPException(status_code=404, detail=f"unknown pipeline {pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")
    try:
        storage = S3Storage()
        spec = PIPELINE_REGISTRY[pipeline_name]
        retriever = ContextRetriever(store=FileContextStore())
        metrics_doc = storage.read_json(spec.metrics_key)

        metrics = []
        for metric_name in sorted(metrics_doc.get("metrics", {})):
            fact = retriever.get_metric(pipeline_name, metric_name, storage)
            metrics.append(
                {
                    "metric_name": metric_name,
                    "provenance": fact.provenance,
                    "review_status": fact.review_status.value if fact.review_status else None,
                    "confidence": fact.confidence,
                    "conflicts": [c.model_dump() for c in fact.conflicts],
                }
            )

        lineage_fact = retriever.get_lineage(pipeline_name, storage)
        pipeline_fact = retriever.get_pipeline_metadata(pipeline_name, storage)
        health_fact = retriever.get_runtime_health(pipeline_name, storage)
        return {
            "pipeline_name": pipeline_name,
            "metrics": metrics,
            "lineage": {"provenance": lineage_fact.provenance, "value": lineage_fact.value},
            "pipeline_metadata": {"provenance": pipeline_fact.provenance, "value": pipeline_fact.value},
            "runtime_health": {"provenance": health_fact.provenance, "value": health_fact.value},
        }
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/incident")
def incident(request: IncidentRequest) -> dict:
    """The narrated business-signal -> incident-response lifecycle (src.data_ops.
    run_incident_response), exposed to the Incidents/Repairs tabs. Exactly one of question or
    pipeline_name must be given -- a business question (relevance inferred, same as /api/ask)
    or a direct operational check on one data product. approve_categories is the explicit
    operator action that unlocks a normally-refused root_cause_category (e.g.
    SOURCE_CONTRACT_CHANGE) for a create_pr candidate on THIS pipeline_name check only -- see
    src.lifecycle_run_self_healing's docstring for the policy this overrides."""
    if bool(request.question) == bool(request.pipeline_name):
        raise HTTPException(status_code=400, detail="exactly one of question or pipeline_name must be given")
    if request.pipeline_name and request.pipeline_name not in PIPELINE_REGISTRY:
        raise HTTPException(status_code=404, detail=f"unknown pipeline {request.pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")
    try:
        storage = S3Storage()
        return run_incident_response(
            storage,
            _model_client_factory,
            question=request.question,
            pipeline_name=request.pipeline_name,
            mode=request.mode,
            human_approved_categories=frozenset(request.approve_categories),
        )
    except (AskLifecycleError, ModelClientError, StorageError, ValueError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/evaluations")
def evaluations() -> dict:
    """The four never-merged eval categories (src.eval_report.build_eval_report):
    deterministic, real_infrastructure, scripted_model, live_model. Does NOT run the live
    pytest subset inline (that takes real time -- a synchronous HTTP request is the wrong
    place for it); real_infrastructure reports whatever was last measured via
    `python3 -m src.eval_report`, or unavailable if that's never been run. Each bucket
    reports {"available": false} on its own if nothing real backs it yet -- never a
    fabricated or merged number."""
    try:
        storage = S3Storage()
        eval_harness_report = storage.read_json("curated/eval_report_latest.json") if storage.exists("curated/eval_report_latest.json") else None
        demo_manifests = load_demo_manifests_from_s3(storage)
        report = build_eval_report(eval_harness_report=eval_harness_report, demo_manifests=demo_manifests, run_real_infrastructure=False)
        if storage.exists("curated/eval_report_bucketed_latest.json"):
            report["real_infrastructure"] = storage.read_json("curated/eval_report_bucketed_latest.json").get("real_infrastructure", {"available": False})
        return report
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# Mounted last: FastAPI matches routes in registration order, and StaticFiles(html=True)
# would otherwise shadow the /api/* routes above at "/".
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
