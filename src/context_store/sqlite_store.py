"""SQLite-backed ContextStore: the tabular alternative to FileContextStore, proving the
ContextStore Protocol isn't tied to JSON files. Chosen over Postgres to avoid a second
docker-compose service for local dev; swapping in a Postgres-backed implementation later is a
new class behind the same Protocol, not a redesign.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from src.context_store.models import (
    DatasetMetadata,
    GeneratedContext,
    HumanAnnotation,
    LineageChain,
    PipelineMetadata,
    RuntimeHealth,
)

DEFAULT_DB_PATH = Path("context/context.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS generated_context (
    asset_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS human_annotation (
    data_product TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_health (
    pipeline_name TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
"""


class SQLiteContextStore:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def get_generated_context(self, asset_id: str) -> GeneratedContext | None:
        row = self._conn.execute(
            "SELECT payload FROM generated_context WHERE asset_id = ?", (asset_id,)
        ).fetchone()
        return GeneratedContext.model_validate_json(row[0]) if row else None

    def get_data_product(self, asset_id: str) -> DatasetMetadata | None:
        context = self.get_generated_context(asset_id)
        return context.dataset_metadata if context else None

    def get_pipeline_context(self, pipeline_name: str) -> PipelineMetadata | None:
        context = self.get_generated_context(pipeline_name)
        return context.pipeline_metadata if context else None

    def get_lineage(self, asset_id: str) -> LineageChain | None:
        context = self.get_generated_context(asset_id)
        return context.lineage if context else None

    def get_metric(self, asset_id: str, metric_name: str) -> dict | None:
        annotation = self.get_human_annotation(asset_id)
        if annotation and metric_name in annotation.metrics:
            return annotation.metrics[metric_name].model_dump()
        context = self.get_generated_context(asset_id)
        if context:
            for metric in context.derived_metrics:
                if metric.name == metric_name:
                    return metric.model_dump()
        return None

    def get_runtime_health(self, pipeline_name: str) -> RuntimeHealth | None:
        row = self._conn.execute(
            "SELECT payload FROM runtime_health WHERE pipeline_name = ?", (pipeline_name,)
        ).fetchone()
        return RuntimeHealth.model_validate_json(row[0]) if row else None

    def get_human_annotation(self, data_product: str) -> HumanAnnotation | None:
        row = self._conn.execute(
            "SELECT payload FROM human_annotation WHERE data_product = ?", (data_product,)
        ).fetchone()
        return HumanAnnotation.model_validate_json(row[0]) if row else None

    def save_generated_context(self, context: GeneratedContext) -> None:
        self._conn.execute(
            "INSERT INTO generated_context (asset_id, payload) VALUES (?, ?) "
            "ON CONFLICT(asset_id) DO UPDATE SET payload = excluded.payload",
            (context.asset_id, context.model_dump_json()),
        )
        self._conn.commit()

    def save_human_annotation(self, annotation: HumanAnnotation) -> None:
        self._conn.execute(
            "INSERT INTO human_annotation (data_product, payload) VALUES (?, ?) "
            "ON CONFLICT(data_product) DO UPDATE SET payload = excluded.payload",
            (annotation.data_product, annotation.model_dump_json()),
        )
        self._conn.commit()

    def save_runtime_health(self, health: RuntimeHealth) -> None:
        self._conn.execute(
            "INSERT INTO runtime_health (pipeline_name, payload) VALUES (?, ?) "
            "ON CONFLICT(pipeline_name) DO UPDATE SET payload = excluded.payload",
            (health.pipeline_name, health.model_dump_json()),
        )
        self._conn.commit()
