"""Builds structured lineage chains: business metric -> curated field -> curated dataset ->
Spark function -> source raw table(s) -> upstream service (once one produces that table).

The upstream-service step is optional and empty until Phase 4's service->raw-table mapping
exists -- this function's shape doesn't change when that lands, only whether the last step is
populated, so diagnosis code written against it today keeps working unmodified.
"""

from __future__ import annotations

from src.context_store.models import LineageChain, LineageStep
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY

# Populated once src/events_to_lifecycle_tables.py (Phase 4) exists; empty for now, meaning
# "no known upstream service" rather than "wrong" -- build_lineage degrades gracefully.
RAW_TABLE_TO_SERVICE: dict[str, str] = {}


def build_lineage(pipeline_name: str, metric_name: str) -> LineageChain:
    spec = PIPELINE_REGISTRY[pipeline_name]
    steps = [
        LineageStep(kind="business_metric", name=metric_name),
        LineageStep(kind="curated_field", name=metric_name),
        LineageStep(kind="curated_dataset", name=pipeline_name),
    ]
    for function_name in spec.etl_function_names:
        steps.append(LineageStep(kind="spark_function", name=function_name))
    for table in spec.raw_tables:
        steps.append(LineageStep(kind="source_table", name=table))
        service = RAW_TABLE_TO_SERVICE.get(table)
        if service:
            steps.append(LineageStep(kind="upstream_service", name=service))

    return LineageChain(asset_id=f"{pipeline_name}.{metric_name}", steps=steps)
