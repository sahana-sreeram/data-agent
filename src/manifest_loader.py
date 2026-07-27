"""Loads pipeline manifests (pipelines/*.yaml) -- the generalized-onboarding surface.

Scoping decision (see the project plan's Phase 8 notes): the 5 existing pipelines' real
`PIPELINE_REGISTRY` entries keep their hand-written run_etl/run_validate adapters, because
their underlying compute_*/validate_* function signatures are genuinely heterogeneous
(campaign_funnel's ETL takes just `spark`; underwriting_performance has two compute functions
producing two curated outputs; several validators drop business_rules or as_of_date) --
retrofitting them onto one signature would be a real, risky behavior change for zero benefit.
Their manifests exist as a fidelity check (test_manifest_loader.py proves every field matches
the real registry exactly) and as the metadata layer a future context/onboarding tool would
read, NOT as their actual execution wiring.

build_generic_pipeline_spec is the real onboarding path: it dynamically imports a manifest's
runtime.source_file/functions[0] and validation.module/function and wraps them in a
PipelineSpec assuming the STANDARDIZED signature (spark, business_rules, as_of_date) for ETL
and (storage, business_rules, validation_rules, as_of_date) for validation. Of the 5 existing
pipelines, loan_portfolio and payment_performance already happen to match this signature
exactly (proving the generic path genuinely works against real code, not just a fixture) --
a 6th, newly-onboarded pipeline that follows the same signature convention needs nothing more
than a manifest and its own ETL/validator files to appear in the registry.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import yaml

from src.pipeline_spec import PipelineSpec

DEFAULT_PIPELINES_DIR = Path("pipelines")


class ManifestError(Exception):
    """Raised for a malformed or incomplete pipeline manifest."""


def load_manifest(path: Path | str) -> dict:
    path = Path(path)
    try:
        manifest = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise ManifestError(f"{path}: invalid YAML: {exc}") from exc

    required_top_level = {"name", "inputs", "outputs", "runtime", "validation"}
    missing = required_top_level - set(manifest)
    if missing:
        raise ManifestError(f"{path}: missing required field(s): {sorted(missing)}")
    return manifest


def load_all_manifests(pipelines_dir: Path | str = DEFAULT_PIPELINES_DIR) -> dict[str, dict]:
    pipelines_dir = Path(pipelines_dir)
    manifests = {}
    for path in sorted(pipelines_dir.glob("*.yaml")):
        manifest = load_manifest(path)
        manifests[manifest["name"]] = manifest
    return manifests


def manifest_metadata_matches_registry(manifest: dict, spec: PipelineSpec) -> list[str]:
    """Returns a list of human-readable mismatches (empty = fully consistent). Used to prove
    a manifest hasn't drifted out of sync with the real, hardcoded PIPELINE_REGISTRY entry it
    describes -- see tests/test_manifest_loader.py."""
    mismatches = []

    manifest_raw_tables = tuple(i["dataset"] for i in manifest["inputs"])
    if manifest_raw_tables != spec.raw_tables:
        mismatches.append(f"inputs {manifest_raw_tables} != raw_tables {spec.raw_tables}")

    manifest_outputs = tuple(manifest["outputs"])
    if manifest_outputs != spec.curated_keys:
        mismatches.append(f"outputs {manifest_outputs} != curated_keys {spec.curated_keys}")

    if manifest["runtime"]["source_file"] != spec.etl_source_file:
        mismatches.append(f"runtime.source_file {manifest['runtime']['source_file']!r} != etl_source_file {spec.etl_source_file!r}")

    manifest_functions = tuple(manifest["runtime"]["functions"])
    if manifest_functions != spec.etl_function_names:
        mismatches.append(f"runtime.functions {manifest_functions} != etl_function_names {spec.etl_function_names}")

    if manifest["validation"].get("rules_file") != spec.validation_rules_key:
        mismatches.append(f"validation.rules_file {manifest['validation'].get('rules_file')!r} != validation_rules_key {spec.validation_rules_key!r}")

    if manifest.get("metrics_file") != spec.metrics_key:
        mismatches.append(f"metrics_file {manifest.get('metrics_file')!r} != metrics_key {spec.metrics_key!r}")

    if manifest.get("lineage_key") != spec.lineage_key:
        mismatches.append(f"lineage_key {manifest.get('lineage_key')!r} != {spec.lineage_key!r}")

    if manifest.get("test_file") != spec.test_file:
        mismatches.append(f"test_file {manifest.get('test_file')!r} != {spec.test_file!r}")

    return mismatches


def _import_attr(module_name: str, attr_name: str):
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def build_generic_pipeline_spec(manifest: dict) -> PipelineSpec:
    """Builds a real, working PipelineSpec purely from a manifest, for a pipeline whose ETL
    function has signature (spark, business_rules, as_of_date) -> Spark DataFrame and whose
    validate function has signature (storage, business_rules, validation_rules, as_of_date)
    -> dict. Raises ManifestError if the manifest doesn't declare exactly one runtime
    function (the generic path doesn't support underwriting_performance's two-function/
    two-output shape -- that pipeline keeps its hand-written registry entry)."""
    name = manifest["name"]
    functions = manifest["runtime"]["functions"]
    if len(functions) != 1:
        raise ManifestError(f"{name}: build_generic_pipeline_spec requires exactly one runtime function, got {functions}")

    source_file = manifest["runtime"]["source_file"]
    etl_module_name = source_file.replace("/", ".").removesuffix(".py")
    etl_function = _import_attr(etl_module_name, functions[0])

    validation = manifest["validation"]
    validate_function = _import_attr(validation["module"], validation["function"])

    output_key = manifest["outputs"][0]

    def run_etl(etl_module, spark, business_rules, as_of_date) -> dict:
        fn = getattr(etl_module, functions[0])
        df = fn(spark, business_rules, as_of_date)
        return {output_key: df.toPandas()}

    def run_validate(storage, business_rules, validation_rules, as_of_date) -> dict:
        return validate_function(storage, business_rules, validation_rules, as_of_date)

    return PipelineSpec(
        name=name,
        raw_tables=tuple(i["dataset"] for i in manifest["inputs"]),
        curated_keys=tuple(manifest["outputs"]),
        validation_rules_key=validation["rules_file"],
        metrics_key=manifest["metrics_file"],
        lineage_key=manifest["lineage_key"],
        etl_source_file=source_file,
        etl_function_names=tuple(functions),
        test_file=manifest["test_file"],
        run_etl=run_etl,
        run_validate=run_validate,
    )
