"""File-based ContextStore: the local-dev backend. Reads/writes two new, additive directories
-- context/generated/ (auto-derived, machine-written) and context/human/ (minimal,
hand-authored YAML) -- and never touches the existing context/business_rules.json,
context/metrics/*.json, context/lineage.json, etc., which remain authoritative for the live
system (see the project plan's Phase 2 notes on incremental migration).
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from src.context_store.models import (
    DatasetMetadata,
    GeneratedContext,
    HumanAnnotation,
    LineageChain,
    PipelineMetadata,
    RuntimeHealth,
)

DEFAULT_ROOT = Path("context")


class FileContextStore:
    def __init__(self, root: Path | str = DEFAULT_ROOT) -> None:
        self.root = Path(root)
        self.generated_dir = self.root / "generated"
        self.runtime_dir = self.generated_dir / "runtime"
        self.human_dir = self.root / "human"

    def _generated_path(self, asset_id: str) -> Path:
        return self.generated_dir / f"{asset_id}.json"

    def _runtime_path(self, pipeline_name: str) -> Path:
        return self.runtime_dir / f"{pipeline_name}.json"

    def _human_path(self, data_product: str) -> Path:
        return self.human_dir / f"{data_product}.yaml"

    def get_generated_context(self, asset_id: str) -> GeneratedContext | None:
        path = self._generated_path(asset_id)
        if not path.exists():
            return None
        return GeneratedContext.model_validate_json(path.read_text())

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
        path = self._runtime_path(pipeline_name)
        if not path.exists():
            return None
        return RuntimeHealth.model_validate_json(path.read_text())

    def get_human_annotation(self, data_product: str) -> HumanAnnotation | None:
        path = self._human_path(data_product)
        if not path.exists():
            return None
        return HumanAnnotation.model_validate(yaml.safe_load(path.read_text()))

    def save_generated_context(self, context: GeneratedContext) -> None:
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self._generated_path(context.asset_id).write_text(
            json.dumps(json.loads(context.model_dump_json()), indent=2)
        )

    def save_human_annotation(self, annotation: HumanAnnotation) -> None:
        self.human_dir.mkdir(parents=True, exist_ok=True)
        self._human_path(annotation.data_product).write_text(
            yaml.safe_dump(json.loads(annotation.model_dump_json()), sort_keys=False)
        )

    def save_runtime_health(self, health: RuntimeHealth) -> None:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._runtime_path(health.pipeline_name).write_text(
            json.dumps(json.loads(health.model_dump_json()), indent=2)
        )
