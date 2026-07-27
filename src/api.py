"""Lite FastAPI web app over src/ask_lifecycle.py -- lets a browser ask a business question
and see the full self-healing story (relevant pipelines, validation failures, diagnosis
evidence, repair status, verification, and the corrected answer) instead of reading it off a
terminal. Purely a presentation layer: every fact it serves already exists in
answer_lifecycle_question's return value (see that module's docstring for the underlying
diagnose -> repair -> verify -> re-answer flow) -- this adds no new business logic.

Run with `python3 -m src.api`, then open http://127.0.0.1:8000/.
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.ask_lifecycle import ANSWER_MODEL_ENV_VAR, AskLifecycleError, answer_lifecycle_question
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.storage import S3Storage, StorageError

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")

app = FastAPI(title="Lifecycle Data Agent")


class AskRequest(BaseModel):
    question: str


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


# Mounted last: FastAPI matches routes in registration order, and StaticFiles(html=True)
# would otherwise shadow the /api/* routes above at "/".
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
