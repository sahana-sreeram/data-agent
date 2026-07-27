"""Pydantic schemas for the context layer.

Three kinds of context, kept as distinct model families so precedence (see
src/context_store/merge.py) is always about comparing like-for-like fields, never guessing
which layer a value came from:

- GeneratedContext: technical facts a machine derived (schema introspection, code parsing, or
  a Codex/LLM pass) about one asset (a pipeline or a dataset). Always UNREVIEWED until a human
  (or a proven-reliable eval score) promotes it -- see ReviewStatus.
- HumanAnnotation: the small set of facts only a human can authoritatively decide (canonical
  metric definitions, approval, repair policy). Deliberately minimal -- anything derivable from
  code/schema/runtime belongs in GeneratedContext instead.
- ResolvedContext: the merge output for one asset, plus any unresolved conflicts between the
  two -- never a silently-picked winner (see ContextConflict).
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ReviewStatus(str, Enum):
    UNREVIEWED = "UNREVIEWED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ApprovalStatus(str, Enum):
    APPROVED = "APPROVED"
    PENDING = "PENDING"
    REJECTED = "REJECTED"


class ConflictStatus(str, Enum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"


class JoinInfo(BaseModel):
    left: str
    right: str
    on: list[str] = Field(default_factory=list)
    how: str = "inner"


class FilterInfo(BaseModel):
    expression: str
    description: str = ""


class DerivedMetric(BaseModel):
    name: str
    formula: str
    source_fields: list[str] = Field(default_factory=list)


class DatasetMetadata(BaseModel):
    """Auto-derivable facts about one physical dataset (raw or curated)."""

    dataset_name: str
    physical_location: str
    columns: dict[str, str] = Field(default_factory=dict)  # column_name -> dtype
    nullable_columns: list[str] = Field(default_factory=list)
    row_count_estimate: int | None = None
    partition_columns: list[str] = Field(default_factory=list)
    sample_value_summaries: dict[str, Any] = Field(default_factory=dict)
    freshness: str | None = None
    producer: str | None = None
    owner: str | None = None
    grain_hypothesis: str | None = None
    candidate_keys: list[str] = Field(default_factory=list)
    uniqueness_observations: dict[str, Any] = Field(default_factory=dict)


class PipelineMetadata(BaseModel):
    """Auto-derivable facts about one ETL pipeline's implementation."""

    pipeline_name: str
    etl_source_file: str
    etl_source_hash: str | None = None  # sha256 of etl_source_file at generation time -- see
    # src.context_retriever.ContextRetriever.ensure_fresh, which recomputes this and
    # regenerates context when it no longer matches the file on disk.
    functions: list[str] = Field(default_factory=list)
    source_datasets: list[str] = Field(default_factory=list)
    output_datasets: list[str] = Field(default_factory=list)
    joins: list[JoinInfo] = Field(default_factory=list)
    filters: list[FilterInfo] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)
    calculated_fields: list[DerivedMetric] = Field(default_factory=list)
    group_by_grain: list[str] = Field(default_factory=list)
    business_rule_lookups: list[str] = Field(default_factory=list)
    test_files: list[str] = Field(default_factory=list)
    update_mode: str = "full_refresh"
    downstream_consumers: list[str] = Field(default_factory=list)


class LineageStep(BaseModel):
    kind: str  # "business_metric" | "curated_field" | "curated_dataset" | "spark_function" | "source_table" | "upstream_service"
    name: str


class LineageChain(BaseModel):
    asset_id: str
    steps: list[LineageStep] = Field(default_factory=list)


class GeneratedContext(BaseModel):
    """Structured, schema-validated output of the enrichment pipeline. Never free text."""

    asset_id: str
    generated_by: str  # "schema_introspector" | "code_enricher" | "codex" | ...
    source_commit: str | None = None
    generated_at: str
    review_status: ReviewStatus = ReviewStatus.UNREVIEWED
    grain: str | None = None
    sources: list[str] = Field(default_factory=list)
    joins: list[JoinInfo] = Field(default_factory=list)
    filters: list[FilterInfo] = Field(default_factory=list)
    derived_metrics: list[DerivedMetric] = Field(default_factory=list)
    business_rule_references: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    confidence: dict[str, float] = Field(default_factory=dict)
    dataset_metadata: DatasetMetadata | None = None
    pipeline_metadata: PipelineMetadata | None = None
    lineage: LineageChain | None = None
    # {service_name: contract_version} as observed in raw data at generation time -- e.g.
    # {"payment_service": "v1"}. Empty until a detector (see
    # src.context_enrichment.contract_detector) populates it. Used by ContextRetriever's
    # selective invalidation to notice an upstream contract change without a code diff.
    service_contract_versions: dict[str, str] = Field(default_factory=dict)


class MetricAnnotation(BaseModel):
    canonical_definition: str
    business_rule: dict[str, Any] = Field(default_factory=dict)
    approved_by: str | None = None
    approval_status: ApprovalStatus = ApprovalStatus.PENDING


class RepairPolicy(BaseModel):
    auto_repair: list[str] = Field(default_factory=list)
    human_review: list[str] = Field(default_factory=list)


class HumanAnnotation(BaseModel):
    """The minimal set of facts only a human can authoritatively decide."""

    data_product: str
    authoritative: bool = False
    owner: str | None = None
    metrics: dict[str, MetricAnnotation] = Field(default_factory=dict)
    repair_policy: RepairPolicy | None = None
    sensitive_data_classification: str | None = None
    escalation_rules: list[str] = Field(default_factory=list)


class RuntimeHealth(BaseModel):
    pipeline_name: str
    etl_status: str
    validation_status: str
    last_run_at: str | None = None
    failed_check_ids: list[str] = Field(default_factory=list)


class ContextConflict(BaseModel):
    """A field where the human-approved and code-derived layers disagree. Never silently
    resolved -- both values and the disagreement are preserved for the diagnosis agent."""

    field: str
    human_approved: Any
    code_observed: Any
    conflict_status: ConflictStatus = ConflictStatus.MISMATCH


class ResolvedContext(BaseModel):
    """The merge output for one asset: generated + human + runtime, with conflicts intact."""

    asset_id: str
    generated: GeneratedContext | None = None
    human: HumanAnnotation | None = None
    runtime: RuntimeHealth | None = None
    conflicts: list[ContextConflict] = Field(default_factory=list)


class ContextFact(BaseModel):
    """One fact returned by src.context_retriever.ContextRetriever -- a metric definition,
    a lineage chain, a schema, etc. -- with full provenance attached, so a caller (or a
    diagnosis/repair model) never has to re-derive where a value came from or how much to
    trust it. `provenance="legacy_file"` marks a pipeline that has no generated/human context
    yet: the value is read straight from today's context/*.json files, and review_status/
    confidence/source_commit are all None -- see ContextRetriever's module docstring."""

    asset_id: str
    field: str
    value: Any
    provenance: str  # "human" | "generated" | "merged" | "runtime" | "legacy_file"
    source_commit: str | None = None
    review_status: ReviewStatus | None = None
    confidence: float | None = None
    schema_fingerprint: str | None = None
    service_contract_version: dict[str, str] = Field(default_factory=dict)
    conflicts: list[ContextConflict] = Field(default_factory=list)
