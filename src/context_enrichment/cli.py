"""CLI entrypoint for the enrichment pipeline.

    python3 -m src.context_enrichment.cli --pipeline loan_portfolio
    python3 -m src.context_enrichment.cli --all
    python3 -m src.context_enrichment.cli --all --use-codex

Runs schema introspection, structural code parsing, lineage construction, and runtime health
for one or all registered pipelines, merges them into one GeneratedContext per pipeline, and
writes them through a ContextStore. The --use-codex flag additionally calls a model for
grain/caveats/derived-metric formulas the structural pass can't reliably produce; without it,
enrichment is fully deterministic and needs no API key.
"""

from __future__ import annotations

import argparse
import sys

from src.context_enrichment.code_enricher import enrich_pipeline_structurally, enrich_pipeline_with_codex
from src.context_enrichment.lineage_builder import build_lineage
from src.context_enrichment.runtime_enricher import enrich_runtime_health
from src.context_enrichment.schema_introspector import introspect_dataset
from src.context_store.file_store import FileContextStore
from src.context_store.models import GeneratedContext
from src.context_store.store import ContextStore
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.model_client import ModelClientError, OpenAIResponsesModelClient
from src.storage import S3Storage


def enrich_one_pipeline(
    pipeline_name: str,
    storage: S3Storage,
    generated_at: str,
    use_codex: bool = False,
) -> GeneratedContext:
    spec = PIPELINE_REGISTRY[pipeline_name]

    if use_codex:
        context = enrich_pipeline_with_codex(pipeline_name, OpenAIResponsesModelClient(), generated_at)
    else:
        pipeline_metadata = enrich_pipeline_structurally(pipeline_name)
        context = GeneratedContext(
            asset_id=pipeline_name,
            generated_by="code_enricher",
            generated_at=generated_at,
            sources=list(spec.raw_tables),
            joins=pipeline_metadata.joins,
            filters=pipeline_metadata.filters,
            business_rule_references=pipeline_metadata.business_rule_lookups,
            pipeline_metadata=pipeline_metadata,
        )

    if storage.exists(spec.curated_keys[0]):
        context.dataset_metadata = introspect_dataset(storage, pipeline_name, spec.curated_keys[0])
    # else: curated output not present yet (e.g. never ETL'd) -- enrichment degrades gracefully

    metrics_doc = storage.read_json(spec.metrics_key) if storage.exists(spec.metrics_key) else {}
    metric_names = list(metrics_doc.get("metrics", {}))
    if metric_names:
        context.lineage = build_lineage(pipeline_name, metric_names[0])

    return context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run automatic context enrichment for one or all lifecycle pipelines.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--pipeline", type=str, choices=sorted(PIPELINE_REGISTRY))
    group.add_argument("--all", action="store_true")
    parser.add_argument("--use-codex", action="store_true", help="Also call a model for grain/caveats/derived metrics.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    import datetime

    args = parse_args(argv)
    pipelines = sorted(PIPELINE_REGISTRY) if args.all else [args.pipeline]

    storage = S3Storage()
    store: ContextStore = FileContextStore()
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for pipeline_name in pipelines:
        try:
            context = enrich_one_pipeline(pipeline_name, storage, generated_at, use_codex=args.use_codex)
        except ModelClientError as exc:
            print(f"{pipeline_name}: enrichment failed ({exc})", file=sys.stderr)
            continue

        health = enrich_runtime_health(storage, pipeline_name)
        store.save_runtime_health(health)
        store.save_generated_context(context)
        print(f"{pipeline_name}: wrote generated context (review_status={context.review_status.value})")


if __name__ == "__main__":
    main()
