"""The PipelineSpec dataclass, standalone so both src.lifecycle_pipeline_registry (which
builds instances of it, hardcoded or manifest-discovered) and src.manifest_loader (which
builds instances of it FROM a manifest) can import it without a circular dependency between
those two modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import SparkSession

# run_etl(etl_module, spark, business_rules, as_of_date) -> {curated_key: pandas.DataFrame}
RunEtl = Callable[[object, SparkSession, dict, str], dict]
# run_validate(storage, business_rules, validation_rules, as_of_date) -> validation result dict
RunValidate = Callable[[object, dict, dict, str], dict]


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    raw_tables: tuple
    curated_keys: tuple
    validation_rules_key: str
    metrics_key: str
    lineage_key: str
    etl_source_file: str
    etl_function_names: tuple
    test_file: str
    run_etl: RunEtl
    run_validate: RunValidate
    # None for every pipeline except loan_portfolio: the repo-relative path to a small,
    # pipeline-owned pointer file (e.g. context/pipeline_rules/loan_portfolio.json) naming
    # which already-approved business-rules file this ONE pipeline reads. A registered
    # CONFIGURATION_CHANGE repair target may repoint it -- see context/repair_targets.json --
    # without ever writing to the shared, cross-pipeline context/business_rules.json. Purely
    # additive: every pipeline without one behaves exactly as before this field existed.
    pipeline_configuration_file: str | None = None
