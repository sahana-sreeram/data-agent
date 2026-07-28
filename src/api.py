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
from src.data_ops import (
    accept_repair,
    auto_scan_and_repair,
    data_product_estate,
    list_pending_repairs,
    reject_repair,
    run_incident_response,
    scale_summary,
)
from src.demo.enterprise_incident import _scripted_diagnosis_client_factory, _scripted_repair_client_factory
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
    use_scripted_model: bool = False


class ScanRequest(BaseModel):
    use_scripted_model: bool = False


class RepairDecisionRequest(BaseModel):
    pipeline_name: str
    branch: str


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


def _require_scripted_model_eligible(pipeline_name: str | None) -> None:
    """The scripted (no-API-cost, instant) model client replays a fixed tool-call sequence
    and diagnosis built specifically for the flagship payment_service v2 / loan_portfolio
    scenario (see src.demo.enterprise_incident) -- it is not a generic stand-in for any
    pipeline. Using it against a different pipeline would fail loudly during diagnosis
    grounding (its recommended_fix.target_file wouldn't be a known file for that pipeline),
    never silently produce a wrong result -- but this check gives a clearer error up front."""
    if pipeline_name != "loan_portfolio":
        raise HTTPException(
            status_code=400,
            detail="use_scripted_model is only meaningful for pipeline_name='loan_portfolio' (the flagship payment_service contract-change scenario)",
        )


@app.post("/api/incident")
def incident(request: IncidentRequest) -> dict:
    """The narrated business-signal -> incident-response lifecycle (src.data_ops.
    run_incident_response), exposed to the Incidents/Repairs tabs. Exactly one of question or
    pipeline_name must be given -- a business question (relevance inferred, same as /api/ask)
    or a direct operational check on one data product. approve_categories is the explicit
    operator action that unlocks a normally-refused root_cause_category (e.g.
    SOURCE_CONTRACT_CHANGE) for a create_pr candidate on THIS pipeline_name check only -- see
    src.lifecycle_run_self_healing's docstring for the policy this overrides.

    use_scripted_model swaps in the same no-API-cost, instant model client
    src.demo.enterprise_incident uses by default -- real Spark/S3/git the whole time, only
    the model's responses are canned -- for the one scenario it's built for (see
    _require_scripted_model_eligible). Default False preserves this endpoint's original
    behavior (a real OpenAI call) for every existing caller."""
    if bool(request.question) == bool(request.pipeline_name):
        raise HTTPException(status_code=400, detail="exactly one of question or pipeline_name must be given")
    if request.pipeline_name and request.pipeline_name not in PIPELINE_REGISTRY:
        raise HTTPException(status_code=404, detail=f"unknown pipeline {request.pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")
    if request.use_scripted_model:
        _require_scripted_model_eligible(request.pipeline_name)
    diagnosis_factory = _scripted_diagnosis_client_factory if request.use_scripted_model else _model_client_factory
    repair_factory = _scripted_repair_client_factory if request.use_scripted_model else None
    try:
        storage = S3Storage()
        return run_incident_response(
            storage,
            diagnosis_factory,
            question=request.question,
            pipeline_name=request.pipeline_name,
            mode=request.mode,
            human_approved_categories=frozenset(request.approve_categories),
            repair_model_client_factory=repair_factory,
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


@app.post("/api/incidents/scan")
def incidents_scan(request: ScanRequest = ScanRequest()) -> dict:
    """The automatic health-monitor scan (src.data_ops.auto_scan_and_repair): finds every
    untrusted data product, diagnoses it, and -- only for SOURCE_CONTRACT_CHANGE, the one
    category this system has a real pre-approved remediation path for -- generates a
    candidate repair automatically. Idempotent: a pipeline with an already-pending candidate
    is reported, not re-diagnosed/re-repaired. Nothing here ever promotes anything; see
    /api/repairs/accept for the explicit human action that does.

    use_scripted_model restricts the scan to loan_portfolio only (see
    _require_scripted_model_eligible) and uses the same no-API-cost, instant model client
    src.demo.enterprise_incident uses by default -- real Spark/S3/git the whole time."""
    try:
        storage = S3Storage()
        if request.use_scripted_model:
            results = auto_scan_and_repair(
                storage, _scripted_diagnosis_client_factory, _scripted_repair_client_factory, pipeline_names=frozenset({"loan_portfolio"})
            )
        else:
            results = auto_scan_and_repair(storage, _model_client_factory)
        return {"results": results}
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/repairs/pending")
def repairs_pending() -> dict:
    """Every data product currently awaiting a human accept/reject decision."""
    try:
        storage = S3Storage()
        return {"pending": list_pending_repairs(storage)}
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/repairs/accept")
def repairs_accept(request: RepairDecisionRequest) -> dict:
    """A human explicitly accepting a VERIFIED_PENDING_PR candidate -- a real, local `git
    merge` into the current branch (never pushed), then a real rerun of this pipeline so the
    accepted change actually takes effect. This is the one endpoint in this API that mutates
    the real repository; it only ever runs in direct response to this explicit call."""
    if request.pipeline_name not in PIPELINE_REGISTRY:
        raise HTTPException(status_code=404, detail=f"unknown pipeline {request.pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")
    from src.spark_session import get_spark_session

    storage = S3Storage()
    spark = get_spark_session("data-ops-accept-repair")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = accept_repair(request.pipeline_name, request.branch, storage, spark)
    except StorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    finally:
        spark.stop()
    if not result.get("accepted"):
        raise HTTPException(status_code=409, detail=result.get("error", "accept failed"))
    return result


@app.post("/api/repairs/reject")
def repairs_reject(request: RepairDecisionRequest) -> dict:
    """A human explicitly rejecting a candidate: discard its branch. The real repository was
    never touched by the candidate in the first place -- this only cleans up the branch and
    the pending-review record."""
    if request.pipeline_name not in PIPELINE_REGISTRY:
        raise HTTPException(status_code=404, detail=f"unknown pipeline {request.pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")
    try:
        storage = S3Storage()
        return reject_repair(request.pipeline_name, request.branch, storage)
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
