"""PySpark ETL: underwriting_performance -- two curated tables.

1. curated/underwriting_performance.parquet -- one row per (breakdown_type, breakdown_value)
   where breakdown_type is "risk_segment" or "model_version": decision_count, approved_count,
   rejected_count, manual_review_count, approval_rate, avg_approved_amount, avg_approved_apr.
2. curated/underwriting_performance_rejections.parquet -- one row per rejection_reason, count.

Reads s3a://<bucket>/raw/{underwriting_decisions,applications,customers}.parquet.
See src/validate_underwriting_performance.py for the independent (pandas) recomputation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_session import get_spark_session, s3a_path
from src.storage import S3Storage

CURATED_KEY = "curated/underwriting_performance.parquet"
REJECTIONS_CURATED_KEY = "curated/underwriting_performance_rejections.parquet"


def _decisions_with_risk_segment(
    underwriting_decisions: DataFrame, applications: DataFrame, customers: DataFrame
) -> DataFrame:
    app_customer = applications.select("application_id", "customer_id")
    return (
        underwriting_decisions.join(app_customer, on="application_id", how="inner")
        .join(customers.select("customer_id", "risk_segment"), on="customer_id", how="inner")
    )


def _breakdown(decisions: DataFrame, group_column: str, breakdown_type: str) -> DataFrame:
    aggregated = decisions.groupBy(F.col(group_column).alias("breakdown_value")).agg(
        F.count(F.lit(1)).alias("decision_count"),
        F.sum(F.when(F.col("decision") == "APPROVED", 1).otherwise(0)).alias("approved_count"),
        F.sum(F.when(F.col("decision") == "REJECTED", 1).otherwise(0)).alias("rejected_count"),
        F.sum(F.when(F.col("decision") == "MANUAL_REVIEW", 1).otherwise(0)).alias("manual_review_count"),
        F.round(
            F.avg(F.when(F.col("decision") == "APPROVED", F.col("approved_amount"))), 2
        ).alias("avg_approved_amount"),
        F.round(
            F.avg(F.when(F.col("decision") == "APPROVED", F.col("approved_apr"))), 4
        ).alias("avg_approved_apr"),
    )
    aggregated = aggregated.withColumn(
        "approval_rate", F.round(F.col("approved_count") / F.col("decision_count"), 4)
    )
    return aggregated.withColumn("breakdown_type", F.lit(breakdown_type)).select(
        "breakdown_type", "breakdown_value", "decision_count", "approved_count", "rejected_count",
        "manual_review_count", "approval_rate", "avg_approved_amount", "avg_approved_apr",
    )


def compute_underwriting_performance(spark: SparkSession) -> DataFrame:
    underwriting_decisions = spark.read.parquet(s3a_path("raw", "underwriting_decisions.parquet"))
    applications = spark.read.parquet(s3a_path("raw", "applications.parquet"))
    customers = spark.read.parquet(s3a_path("raw", "customers.parquet"))

    decisions_with_segment = _decisions_with_risk_segment(underwriting_decisions, applications, customers)

    by_risk_segment = _breakdown(decisions_with_segment, "risk_segment", "risk_segment")
    by_model_version = _breakdown(underwriting_decisions, "model_version", "model_version")

    return by_risk_segment.unionByName(by_model_version)


def compute_rejection_distribution(spark: SparkSession) -> DataFrame:
    underwriting_decisions = spark.read.parquet(s3a_path("raw", "underwriting_decisions.parquet"))
    return (
        underwriting_decisions.filter(F.col("decision") == "REJECTED")
        .groupBy("rejection_reason")
        .agg(F.count(F.lit(1)).alias("count"))
    )


def write_curated(performance_df: DataFrame, rejections_df: DataFrame, storage: S3Storage) -> tuple[str, str]:
    storage.write_parquet(CURATED_KEY, performance_df.toPandas())
    storage.write_parquet(REJECTIONS_CURATED_KEY, rejections_df.toPandas())
    return f"s3://{storage.bucket}/{CURATED_KEY}", f"s3://{storage.bucket}/{REJECTIONS_CURATED_KEY}"


def main() -> None:
    storage = S3Storage()
    spark = get_spark_session("underwriting-performance-etl")
    spark.sparkContext.setLogLevel("WARN")
    try:
        performance_pandas = compute_underwriting_performance(spark).toPandas()
        rejections_pandas = compute_rejection_distribution(spark).toPandas()
        storage.write_parquet(CURATED_KEY, performance_pandas)
        storage.write_parquet(REJECTIONS_CURATED_KEY, rejections_pandas)

        print(f"Wrote s3://{storage.bucket}/{CURATED_KEY}")
        for d in performance_pandas.to_dict(orient="records"):
            print(f"  {d['breakdown_type']:<13} {d['breakdown_value']:<14} decisions={d['decision_count']:<4} approval_rate={d['approval_rate']}")
        print(f"Wrote s3://{storage.bucket}/{REJECTIONS_CURATED_KEY}")
        for d in rejections_pandas.to_dict(orient="records"):
            print(f"  {d['rejection_reason']:<24} {d['count']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
