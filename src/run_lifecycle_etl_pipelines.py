"""Orchestrates every registered lifecycle PySpark ETL pipeline + its independent pandas
validator against ONE shared Spark session (avoids paying the JVM startup cost once per
pipeline), and writes a combined s3://<bucket>/curated/pipeline_run.json summarizing
etl_status/validation_status per pipeline -- the lifecycle-model analog of today's
local data/processed/pipeline_run.json.

PIPELINES is built generically from PIPELINE_REGISTRY (src.lifecycle_pipeline_registry),
which already includes any manifest-discovered pipeline (see that module's
_discover_generic_pipelines) alongside the hand-registered ones -- onboarding a new pipeline
via pipelines/<name>.yaml makes it show up here automatically, with no change to this file.

Each pipeline's failure is isolated: one pipeline's ETL or validation error doesn't
prevent the others from running. The ETL (compute + write curated output) and
validation (independent pandas recomputation) stages are tried SEPARATELY, so a
failure is attributed to the stage that actually failed -- an exception raised by
a validator is never misreported as an ETL failure, and vice versa. The full
traceback is captured (not just str(exc)) so a genuine coding bug (as opposed to
a legitimate data/environment issue) can actually be diagnosed from
pipeline_run.json rather than losing the stack trace on the way in.
"""

from __future__ import annotations

import importlib
import traceback
from typing import Callable

from src.lifecycle_pipeline_registry import DEFAULT_AS_OF_DATE, PIPELINE_REGISTRY
from src.pipeline_spec import PipelineSpec
from src.spark_session import get_spark_session
from src.storage import S3Storage

CURATED_RUN_KEY = "curated/pipeline_run.json"


def _make_etl_fn(spec: PipelineSpec) -> Callable:
    def etl_fn(spark, storage, business_rules) -> None:
        module_name = spec.etl_source_file.replace("/", ".").removesuffix(".py")
        etl_module = importlib.import_module(module_name)
        outputs = spec.run_etl(etl_module, spark, business_rules, DEFAULT_AS_OF_DATE)
        for key, df in outputs.items():
            storage.write_parquet(key, df)

    return etl_fn


def _make_validate_fn(spec: PipelineSpec) -> Callable:
    def validate_fn(storage, business_rules) -> dict:
        validation_rules = storage.read_json(spec.validation_rules_key)
        return spec.run_validate(storage, business_rules, validation_rules, DEFAULT_AS_OF_DATE)

    return validate_fn


# {pipeline_name: (etl_fn(spark, storage, business_rules) -> None, validate_fn(storage, business_rules) -> dict)}
PIPELINES = {name: (_make_etl_fn(spec), _make_validate_fn(spec)) for name, spec in PIPELINE_REGISTRY.items()}


def run_all_pipelines(spark, storage: S3Storage) -> dict:
    business_rules = storage.read_json("context/business_rules.json")
    results: dict[str, dict] = {}

    for name, (etl_fn, validate_fn) in PIPELINES.items():
        try:
            etl_fn(spark, storage, business_rules)
        except Exception:  # noqa: BLE001 -- isolate one pipeline's failure from the rest
            results[name] = {
                "etl_status": "FAILURE",
                "etl_error": traceback.format_exc(),
                "validation_status": "NOT_RUN",
                "validation_error": None,
            }
            continue

        try:
            validation_results = validate_fn(storage, business_rules)
            results[name] = {
                "etl_status": "SUCCESS",
                "etl_error": None,
                "validation_status": validation_results["overall_status"],
                "validation_error": None,
            }
        except Exception:  # noqa: BLE001 -- the ETL succeeded; only validation failed
            results[name] = {
                "etl_status": "SUCCESS",
                "etl_error": None,
                "validation_status": "ERROR",
                "validation_error": traceback.format_exc(),
            }

    overall_status = (
        "SUCCESS"
        if all(r["etl_status"] == "SUCCESS" and r["validation_status"] == "PASS" for r in results.values())
        else "FAILURE"
    )
    return {"pipelines": results, "overall_status": overall_status}


def main(argv: list[str] | None = None) -> None:
    storage = S3Storage()
    spark = get_spark_session("lifecycle-etl-pipelines")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_record = run_all_pipelines(spark, storage)
    finally:
        spark.stop()

    storage.write_json(CURATED_RUN_KEY, run_record)

    print(f"overall_status: {run_record['overall_status']}")
    for name, result in run_record["pipelines"].items():
        print(f"  {name:<26} etl={result['etl_status']:<8} validation={result['validation_status']}")
        if result["etl_error"]:
            print(f"    etl_error:\n{result['etl_error']}")
        if result["validation_error"]:
            print(f"    validation_error:\n{result['validation_error']}")

    if run_record["overall_status"] != "SUCCESS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
