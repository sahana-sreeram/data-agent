"""The ContextStore abstraction: everything that reads or writes context (generated,
human-approved, or runtime) goes through this Protocol instead of a hardcoded file path, so a
JSON-file backend and a tabular (SQLite/Postgres) backend are interchangeable.
"""

from __future__ import annotations

from typing import Protocol

from src.context_store.models import (
    DatasetMetadata,
    GeneratedContext,
    HumanAnnotation,
    LineageChain,
    PipelineMetadata,
    RuntimeHealth,
)


class ContextStore(Protocol):
    def get_data_product(self, asset_id: str) -> DatasetMetadata | None: ...

    def get_metric(self, asset_id: str, metric_name: str) -> dict | None: ...

    def get_lineage(self, asset_id: str) -> LineageChain | None: ...

    def get_pipeline_context(self, pipeline_name: str) -> PipelineMetadata | None: ...

    def get_runtime_health(self, pipeline_name: str) -> RuntimeHealth | None: ...

    def get_generated_context(self, asset_id: str) -> GeneratedContext | None: ...

    def get_human_annotation(self, data_product: str) -> HumanAnnotation | None: ...

    def save_generated_context(self, context: GeneratedContext) -> None: ...

    def save_human_annotation(self, annotation: HumanAnnotation) -> None: ...

    def save_runtime_health(self, health: RuntimeHealth) -> None: ...
