"""ContextRetriever: the one interface live agent code (Q&A, diagnosis, repair, and
verification reporting) reads context through, instead of hardcoded context/*.json paths.

For a pipeline with no GeneratedContext yet (every pipeline except loan_portfolio, as of this
vertical slice), every method here degrades to reading the exact same file(s) today's tools
already read directly -- so wiring this in changes nothing observable for those pipelines,
and "migrate pipeline N" later is a data change (run the enrichment CLI once, author its
context/human/<name>.yaml), never an agent-code change. For a pipeline that DOES have
generated context (loan_portfolio), facts are merged with any human annotation
(src.context_store.merge.merge_context: human-approved wins, mismatches are surfaced as
ContextConflicts, never silently dropped) and returned as a ContextFact carrying full
provenance -- where the value came from, its review status/confidence, a schema fingerprint,
the upstream contract version(s) it was generated against, and any unresolved conflict.

ensure_fresh() is the selective-invalidation half of this. It ONLY ever refreshes a pipeline
that already has a stored GeneratedContext -- it never bootstraps a brand new one for a
pipeline that hasn't been migrated yet, so an unmigrated pipeline's first ContextRetriever
call costs nothing extra and writes nothing. For an already-migrated pipeline, it recomputes
the ETL source file's hash and the observed upstream contract version(s) it depends on
(currently just payment_service's PAID/SETTLED rename -- see
src.context_enrichment.contract_detector) and, if either has drifted from what the stored
GeneratedContext recorded, regenerates it (a structural pass only -- no live model call) before
serving anything. This is what makes a payment_service contract change or an ETL edit visible
without a manual `python3 -m src.context_enrichment.cli` run.
"""

from __future__ import annotations

import datetime
import hashlib
import importlib
import inspect
from dataclasses import dataclass
from pathlib import Path

from src.context_enrichment.contract_detector import detect_payment_service_contract_version
from src.context_store.merge import merge_context
from src.context_store.models import ContextFact, DatasetMetadata, GeneratedContext
from src.context_store.store import ContextStore
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.manifest_loader import ManifestError, load_manifest
from src.storage import S3Storage

DEFAULT_PIPELINES_DIR = Path("pipelines")

# Which service(s) a pipeline manifest's inputs name, mapped to a detector that reads raw
# data and reports the contract version actually observed. Only payment_service has more
# than one contract version today; adding a new one is one line here, not a new code path.
_CONTRACT_DETECTORS = {"payment_service": detect_payment_service_contract_version}


def _sha256_of_file(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _schema_fingerprint(dataset_metadata: DatasetMetadata | None) -> str | None:
    if dataset_metadata is None or not dataset_metadata.columns:
        return None
    canonical = ",".join(f"{k}:{v}" for k, v in sorted(dataset_metadata.columns.items()))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _source_services(pipeline_name: str) -> set[str]:
    manifest_path = DEFAULT_PIPELINES_DIR / f"{pipeline_name}.yaml"
    if not manifest_path.exists():
        return set()
    try:
        manifest = load_manifest(manifest_path)
    except ManifestError:
        return set()
    return {i["source_service"] for i in manifest.get("inputs", []) if "source_service" in i}


@dataclass
class ContextRetriever:
    store: ContextStore

    def ensure_fresh(self, pipeline_name: str, storage: S3Storage) -> GeneratedContext | None:
        """Return this pipeline's current GeneratedContext, regenerating it first (structural
        pass, no model call) if its ETL source or an upstream contract it depends on has
        drifted. Returns None -- WITHOUT attempting to create anything -- for a pipeline that
        has no GeneratedContext at all yet; that first-time generation is a deliberate,
        separate migration action (`python3 -m src.context_enrichment.cli --pipeline <name>`),
        not something a read path should trigger implicitly."""
        existing = self.store.get_generated_context(pipeline_name)
        if existing is None:
            return None

        spec = PIPELINE_REGISTRY.get(pipeline_name)
        if spec is None:
            return existing

        current_hash = _sha256_of_file(Path(spec.etl_source_file))
        current_contract_versions = {
            service: _CONTRACT_DETECTORS[service](storage)
            for service in _source_services(pipeline_name)
            if service in _CONTRACT_DETECTORS
        }
        stored_hash = existing.pipeline_metadata.etl_source_hash if existing.pipeline_metadata else None
        if stored_hash == current_hash and existing.service_contract_versions == current_contract_versions:
            return existing  # nothing has drifted -- keep serving what's already stored

        from src.context_enrichment.cli import enrich_one_pipeline

        try:
            context = enrich_one_pipeline(
                pipeline_name, storage, datetime.datetime.now(datetime.timezone.utc).isoformat(), use_codex=False
            )
        except Exception:  # noqa: BLE001 -- a regeneration failure should never block serving a fact
            return existing

        if context.pipeline_metadata is not None:
            context.pipeline_metadata.etl_source_hash = current_hash
        context.service_contract_versions = current_contract_versions
        self.store.save_generated_context(context)
        return context

    def get_metric(self, pipeline_name: str, metric_name: str, storage: S3Storage) -> ContextFact:
        generated = self.ensure_fresh(pipeline_name, storage)
        human = self.store.get_human_annotation(pipeline_name)

        if generated is None and human is None:
            return self._legacy_metric(pipeline_name, metric_name, storage)

        resolved = merge_context(pipeline_name, generated, human)
        metric_annotation = human.metrics.get(metric_name) if human else None
        derived = next(
            (m for m in (generated.derived_metrics if generated else []) if m.name == metric_name), None
        )

        if metric_annotation is not None:
            value = {
                "canonical_definition": metric_annotation.canonical_definition,
                "business_rule": metric_annotation.business_rule,
                "approved_by": metric_annotation.approved_by,
                "approval_status": metric_annotation.approval_status.value,
            }
            if derived is not None:
                value["formula"] = derived.formula
                value["source_fields"] = derived.source_fields
            provenance = "merged" if derived is not None else "human"
        elif derived is not None:
            value = {"formula": derived.formula, "source_fields": derived.source_fields}
            provenance = "generated"
        else:
            return self._legacy_metric(pipeline_name, metric_name, storage)

        conflicts = [c for c in resolved.conflicts if c.field.startswith(f"{metric_name}.")]
        return ContextFact(
            asset_id=pipeline_name,
            field=metric_name,
            value=value,
            provenance=provenance,
            source_commit=generated.source_commit if generated else None,
            review_status=generated.review_status if generated else None,
            confidence=(generated.confidence.get(metric_name) if generated else None),
            schema_fingerprint=_schema_fingerprint(generated.dataset_metadata if generated else None),
            service_contract_version=generated.service_contract_versions if generated else {},
            conflicts=conflicts,
        )

    def _legacy_metric(self, pipeline_name: str, metric_name: str, storage: S3Storage) -> ContextFact:
        spec = PIPELINE_REGISTRY[pipeline_name]
        metrics_doc = storage.read_json(spec.metrics_key)
        value = metrics_doc.get("metrics", {}).get(metric_name)
        return ContextFact(asset_id=pipeline_name, field=metric_name, value=value, provenance="legacy_file")

    def get_lineage(self, pipeline_name: str, storage: S3Storage) -> ContextFact:
        generated = self.ensure_fresh(pipeline_name, storage)
        if generated is None or generated.lineage is None:
            spec = PIPELINE_REGISTRY[pipeline_name]
            lineage_doc = storage.read_json("context/lineage.json")
            entry = lineage_doc.get("datasets", {}).get(spec.lineage_key)
            return ContextFact(asset_id=pipeline_name, field="lineage", value=entry, provenance="legacy_file")
        return ContextFact(
            asset_id=pipeline_name,
            field="lineage",
            value=generated.lineage.model_dump(),
            provenance="generated",
            source_commit=generated.source_commit,
            review_status=generated.review_status,
            schema_fingerprint=_schema_fingerprint(generated.dataset_metadata),
            service_contract_version=generated.service_contract_versions,
        )

    def get_pipeline_metadata(self, pipeline_name: str, storage: S3Storage) -> ContextFact:
        generated = self.ensure_fresh(pipeline_name, storage)
        if generated is None or generated.pipeline_metadata is None:
            spec = PIPELINE_REGISTRY[pipeline_name]
            value = {"etl_source_file": spec.etl_source_file, "functions": list(spec.etl_function_names)}
            return ContextFact(asset_id=pipeline_name, field="pipeline_metadata", value=value, provenance="legacy_file")
        return ContextFact(
            asset_id=pipeline_name,
            field="pipeline_metadata",
            value=generated.pipeline_metadata.model_dump(),
            provenance="generated",
            source_commit=generated.source_commit,
            review_status=generated.review_status,
            schema_fingerprint=_schema_fingerprint(generated.dataset_metadata),
            service_contract_version=generated.service_contract_versions,
        )

    def get_relevant_code(self, pipeline_name: str, storage: S3Storage) -> ContextFact:
        spec = PIPELINE_REGISTRY[pipeline_name]
        module_name = spec.etl_source_file.replace("/", ".").removesuffix(".py")
        module = importlib.import_module(module_name)
        functions_source = {name: inspect.getsource(getattr(module, name)) for name in spec.etl_function_names}
        generated = self.ensure_fresh(pipeline_name, storage)
        return ContextFact(
            asset_id=pipeline_name,
            field="relevant_code",
            value={"file": spec.etl_source_file, "functions": functions_source},
            provenance="generated" if generated is not None else "legacy_file",
            source_commit=generated.source_commit if generated else None,
            review_status=generated.review_status if generated else None,
            schema_fingerprint=_schema_fingerprint(generated.dataset_metadata if generated else None),
            service_contract_version=generated.service_contract_versions if generated else {},
        )

    def get_business_rules(self, pipeline_name: str, storage: S3Storage) -> ContextFact:
        """context/business_rules.json is cross-cutting and has no generated/human
        counterpart as a WHOLE document (only per-metric business_rule sub-dicts inside a
        HumanAnnotation) -- always legacy_file. Still routed through here so every tool has
        one interface; a mismatch between this file and an approved per-metric business_rule
        is caught by get_metric()'s conflicts instead."""
        value = storage.read_json("context/business_rules.json")
        return ContextFact(asset_id=pipeline_name, field="business_rules", value=value, provenance="legacy_file")

    def get_runtime_health(self, pipeline_name: str, storage: S3Storage) -> ContextFact:
        runtime = self.store.get_runtime_health(pipeline_name)
        if runtime is None:
            pipeline_run = storage.read_json("curated/pipeline_run.json") if storage.exists("curated/pipeline_run.json") else {}
            entry = pipeline_run.get("pipelines", {}).get(pipeline_name, {})
            value = {
                "pipeline_name": pipeline_name,
                "etl_status": entry.get("etl_status"),
                "validation_status": entry.get("validation_status"),
            }
            return ContextFact(asset_id=pipeline_name, field="runtime_health", value=value, provenance="legacy_file")
        return ContextFact(
            asset_id=pipeline_name, field="runtime_health", value=runtime.model_dump(), provenance="runtime"
        )
