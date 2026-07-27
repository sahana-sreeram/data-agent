"""Derives RuntimeHealth from the same curated/pipeline_run.json and
curated/<pipeline>_validation_results.json every pipeline already writes -- no new
instrumentation, just a structured read of facts that already exist."""

from __future__ import annotations

from src.context_store.models import RuntimeHealth
from src.storage import S3Storage


def enrich_runtime_health(storage: S3Storage, pipeline_name: str) -> RuntimeHealth:
    pipeline_run = storage.read_json("curated/pipeline_run.json") if storage.exists("curated/pipeline_run.json") else {}
    result = pipeline_run.get("pipelines", {}).get(pipeline_name, {})

    validation_key = f"curated/{pipeline_name}_validation_results.json"
    failed_check_ids: list[str] = []
    if storage.exists(validation_key):
        validation_results = storage.read_json(validation_key)
        failed_check_ids = [c["id"] for c in validation_results.get("checks", []) if c.get("status") == "FAIL"]

    return RuntimeHealth(
        pipeline_name=pipeline_name,
        etl_status=result.get("etl_status", "UNKNOWN"),
        validation_status=result.get("validation_status", "UNKNOWN"),
        failed_check_ids=failed_check_ids,
    )
