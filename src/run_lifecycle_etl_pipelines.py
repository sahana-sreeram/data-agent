"""Orchestrates all 5 lifecycle PySpark ETL pipelines + their independent pandas
validators against ONE shared Spark session (avoids paying the JVM startup cost 5
times), and writes a combined s3://<bucket>/curated/pipeline_run.json summarizing
etl_status/validation_status per pipeline -- the lifecycle-model analog of today's
local data/processed/pipeline_run.json.

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

import traceback

from src.spark_session import get_spark_session
from src.storage import S3Storage

import src.etl_spark_campaign_funnel as campaign_funnel
import src.etl_spark_delinquency_default as delinquency_default
import src.etl_spark_loan_portfolio as loan_portfolio
import src.etl_spark_payment_performance as payment_performance
import src.etl_spark_underwriting_performance as underwriting_performance
import src.validate_campaign_funnel as validate_campaign_funnel
import src.validate_delinquency_default as validate_delinquency_default
import src.validate_loan_portfolio as validate_loan_portfolio
import src.validate_payment_performance as validate_payment_performance
import src.validate_underwriting_performance as validate_underwriting_performance

CURATED_RUN_KEY = "curated/pipeline_run.json"


def _etl_loan_portfolio(spark, storage, business_rules) -> None:
    df = loan_portfolio.compute_loan_portfolio(spark, business_rules)
    loan_portfolio.write_curated(df, storage)


def _validate_loan_portfolio(storage, business_rules) -> dict:
    validation_rules = storage.read_json("context/validations/loan_portfolio.json")
    return validate_loan_portfolio.validate_loan_portfolio(storage, business_rules, validation_rules)


def _etl_campaign_funnel(spark, storage, business_rules) -> None:
    df = campaign_funnel.compute_campaign_funnel(spark)
    campaign_funnel.write_curated(df, storage)


def _validate_campaign_funnel(storage, business_rules) -> dict:
    validation_rules = storage.read_json("context/validations/campaign_funnel.json")
    return validate_campaign_funnel.validate_campaign_funnel(storage, validation_rules)


def _etl_underwriting_performance(spark, storage, business_rules) -> None:
    performance_df = underwriting_performance.compute_underwriting_performance(spark)
    rejections_df = underwriting_performance.compute_rejection_distribution(spark)
    underwriting_performance.write_curated(performance_df, rejections_df, storage)


def _validate_underwriting_performance(storage, business_rules) -> dict:
    validation_rules = storage.read_json("context/validations/underwriting_performance.json")
    return validate_underwriting_performance.validate_underwriting_performance(storage, validation_rules)


def _etl_payment_performance(spark, storage, business_rules) -> None:
    df = payment_performance.compute_payment_performance(spark, business_rules)
    payment_performance.write_curated(df, storage)


def _validate_payment_performance(storage, business_rules) -> dict:
    validation_rules = storage.read_json("context/validations/payment_performance.json")
    return validate_payment_performance.validate_payment_performance(storage, business_rules, validation_rules)


def _etl_delinquency_default(spark, storage, business_rules) -> None:
    df = delinquency_default.compute_delinquency_default(spark, business_rules)
    delinquency_default.write_curated(df, storage)


def _validate_delinquency_default(storage, business_rules) -> dict:
    validation_rules = storage.read_json("context/validations/delinquency_default.json")
    return validate_delinquency_default.validate_delinquency_default(storage, business_rules, validation_rules)


# {pipeline_name: (etl_fn(spark, storage, business_rules), validate_fn(storage, business_rules) -> dict)}
PIPELINES = {
    "loan_portfolio": (_etl_loan_portfolio, _validate_loan_portfolio),
    "campaign_funnel": (_etl_campaign_funnel, _validate_campaign_funnel),
    "underwriting_performance": (_etl_underwriting_performance, _validate_underwriting_performance),
    "payment_performance": (_etl_payment_performance, _validate_payment_performance),
    "delinquency_default": (_etl_delinquency_default, _validate_delinquency_default),
}


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
