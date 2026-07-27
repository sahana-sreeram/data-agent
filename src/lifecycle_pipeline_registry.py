"""The single source of truth for what each of the 5 lifecycle curated pipelines needs to
be diagnosed, repaired, and verified generically.

The 5 ETL/validate module pairs are genuinely heterogeneous (some take business_rules, some
don't; some take as_of_date, some don't; underwriting_performance has two compute functions
and two curated outputs) -- rather than let every consumer (diagnosis, repair, verify) grow
its own per-pipeline if/elif dispatch, each PipelineSpec normalizes its pipeline's quirks
behind one uniform `run_etl`/`run_validate` signature. Every generalized self-healing module
reads from PIPELINE_REGISTRY instead of hardcoding pipeline facts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import SparkSession

from src.storage import S3Storage
from src.validate_campaign_funnel import validate_campaign_funnel
from src.validate_delinquency_default import validate_delinquency_default
from src.validate_loan_portfolio import validate_loan_portfolio
from src.validate_payment_performance import validate_payment_performance
from src.validate_underwriting_performance import validate_underwriting_performance

DEFAULT_AS_OF_DATE = "2026-07-20"

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


def _run_etl_loan_portfolio(etl_module, spark: SparkSession, business_rules: dict, as_of_date: str) -> dict:
    df = etl_module.compute_loan_portfolio(spark, business_rules, as_of_date)
    return {"curated/loan_portfolio.parquet": df.toPandas()}


def _run_validate_loan_portfolio(storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str) -> dict:
    return validate_loan_portfolio(storage, business_rules, validation_rules, as_of_date)


def _run_etl_campaign_funnel(etl_module, spark: SparkSession, business_rules: dict, as_of_date: str) -> dict:
    df = etl_module.compute_campaign_funnel(spark)
    return {"curated/campaign_funnel.parquet": df.toPandas()}


def _run_validate_campaign_funnel(storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str) -> dict:
    return validate_campaign_funnel(storage, validation_rules)


def _run_etl_underwriting_performance(etl_module, spark: SparkSession, business_rules: dict, as_of_date: str) -> dict:
    performance_df = etl_module.compute_underwriting_performance(spark)
    rejections_df = etl_module.compute_rejection_distribution(spark)
    return {
        "curated/underwriting_performance.parquet": performance_df.toPandas(),
        "curated/underwriting_performance_rejections.parquet": rejections_df.toPandas(),
    }


def _run_validate_underwriting_performance(storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str) -> dict:
    return validate_underwriting_performance(storage, validation_rules)


def _run_etl_payment_performance(etl_module, spark: SparkSession, business_rules: dict, as_of_date: str) -> dict:
    df = etl_module.compute_payment_performance(spark, business_rules, as_of_date)
    return {"curated/payment_performance.parquet": df.toPandas()}


def _run_validate_payment_performance(storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str) -> dict:
    return validate_payment_performance(storage, business_rules, validation_rules, as_of_date)


def _run_etl_delinquency_default(etl_module, spark: SparkSession, business_rules: dict, as_of_date: str) -> dict:
    df = etl_module.compute_delinquency_default(spark, business_rules)
    return {"curated/delinquency_default.parquet": df.toPandas()}


def _run_validate_delinquency_default(storage: S3Storage, business_rules: dict, validation_rules: dict, as_of_date: str) -> dict:
    return validate_delinquency_default(storage, business_rules, validation_rules)


PIPELINE_REGISTRY: dict = {
    "loan_portfolio": PipelineSpec(
        name="loan_portfolio",
        raw_tables=("loans", "payment_events"),
        curated_keys=("curated/loan_portfolio.parquet",),
        validation_rules_key="context/validations/loan_portfolio.json",
        metrics_key="context/metrics/loan_portfolio.json",
        lineage_key="curated.loan_portfolio",
        etl_source_file="src/etl_spark_loan_portfolio.py",
        etl_function_names=("compute_loan_portfolio",),
        test_file="tests/test_etl_spark_loan_portfolio.py",
        run_etl=_run_etl_loan_portfolio,
        run_validate=_run_validate_loan_portfolio,
    ),
    "campaign_funnel": PipelineSpec(
        name="campaign_funnel",
        raw_tables=("campaigns", "email_events", "prequal_offers", "applications", "underwriting_decisions", "loans"),
        curated_keys=("curated/campaign_funnel.parquet",),
        validation_rules_key="context/validations/campaign_funnel.json",
        metrics_key="context/metrics/campaign_funnel.json",
        lineage_key="curated.campaign_funnel",
        etl_source_file="src/etl_spark_campaign_funnel.py",
        # _attribute_applications_to_campaigns is a private helper compute_campaign_funnel
        # calls -- included so get_relevant_etl_source/compare_metric_definition_to_etl
        # actually see it; a bug can live in a helper just as easily as the public function.
        etl_function_names=("compute_campaign_funnel", "_attribute_applications_to_campaigns"),
        test_file="tests/test_etl_spark_campaign_funnel.py",
        run_etl=_run_etl_campaign_funnel,
        run_validate=_run_validate_campaign_funnel,
    ),
    "underwriting_performance": PipelineSpec(
        name="underwriting_performance",
        raw_tables=("underwriting_decisions", "applications", "customers"),
        curated_keys=("curated/underwriting_performance.parquet", "curated/underwriting_performance_rejections.parquet"),
        validation_rules_key="context/validations/underwriting_performance.json",
        metrics_key="context/metrics/underwriting_performance.json",
        lineage_key="curated.underwriting_performance",
        etl_source_file="src/etl_spark_underwriting_performance.py",
        # _decisions_with_risk_segment/_breakdown are private helpers compute_underwriting_performance
        # calls -- included for the same reason as campaign_funnel's helper above.
        etl_function_names=(
            "compute_underwriting_performance", "compute_rejection_distribution",
            "_decisions_with_risk_segment", "_breakdown",
        ),
        test_file="tests/test_etl_spark_underwriting_performance.py",
        run_etl=_run_etl_underwriting_performance,
        run_validate=_run_validate_underwriting_performance,
    ),
    "payment_performance": PipelineSpec(
        name="payment_performance",
        raw_tables=("payment_schedule", "payment_events"),
        curated_keys=("curated/payment_performance.parquet",),
        validation_rules_key="context/validations/payment_performance.json",
        metrics_key="context/metrics/payment_performance.json",
        lineage_key="curated.payment_performance",
        etl_source_file="src/etl_spark_payment_performance.py",
        etl_function_names=("compute_payment_performance",),
        test_file="tests/test_etl_spark_payment_performance.py",
        run_etl=_run_etl_payment_performance,
        run_validate=_run_validate_payment_performance,
    ),
    "delinquency_default": PipelineSpec(
        name="delinquency_default",
        raw_tables=("loans", "customers", "delinquency_events", "defaults"),
        curated_keys=("curated/delinquency_default.parquet",),
        validation_rules_key="context/validations/delinquency_default.json",
        metrics_key="context/metrics/delinquency_default.json",
        lineage_key="curated.delinquency_default",
        etl_source_file="src/etl_spark_delinquency_default.py",
        # _metrics_for_group is a private helper compute_delinquency_default calls twice
        # (overall + per-segment) -- it's where loss_rate's business-rule-driven
        # denominator is actually computed, so it must be included for the same reason as
        # campaign_funnel's/underwriting_performance's helpers above.
        etl_function_names=("compute_delinquency_default", "_metrics_for_group"),
        test_file="tests/test_etl_spark_delinquency_default.py",
        run_etl=_run_etl_delinquency_default,
        run_validate=_run_validate_delinquency_default,
    ),
}
