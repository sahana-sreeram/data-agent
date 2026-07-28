"""Builds structured lineage chains: business metric -> curated field -> curated dataset ->
Spark function -> source raw table(s) -> upstream service (once one produces that table).

The upstream-service step is optional and empty for a raw table no service produces -- this
function's shape doesn't change either way, only whether the last step is populated, so
diagnosis code written against it keeps working unmodified regardless.
"""

from __future__ import annotations

from src.context_store.models import LineageChain, LineageStep
from src.events_to_lifecycle_tables import EVENT_TYPE_TO_TABLE, SERVICE_BY_EVENT_TYPE
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY

# Derived from src.events_to_lifecycle_tables's own event-type -> table and event-type ->
# service mappings (the real, tested source of truth for which service produces which raw
# table) rather than duplicated here by hand. A raw table with no producing event type (none
# today) simply has no upstream_service step -- degrades gracefully, same as before.
RAW_TABLE_TO_SERVICE: dict[str, str] = {
    table: SERVICE_BY_EVENT_TYPE[event_type] for event_type, table in EVENT_TYPE_TO_TABLE.items()
}


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
