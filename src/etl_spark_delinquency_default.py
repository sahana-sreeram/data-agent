"""PySpark ETL: delinquency_default -- one overall row (risk_segment='ALL') plus one row
per risk_segment. Reads s3a://<bucket>/raw/{loans,customers,delinquency_events,defaults}.parquet,
writes to s3a://<bucket>/curated/delinquency_default.parquet.

See src/validate_delinquency_default.py for the independent (pandas) recomputation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_session import get_spark_session, s3a_path
from src.storage import S3Storage

CURATED_KEY = "curated/delinquency_default.parquet"
OVERALL_LABEL = "ALL"


def _metrics_for_group(loans_with_segment: DataFrame, delinquency_events: DataFrame, defaults: DataFrame, business_rules: dict, group_column: str) -> DataFrame:
    loan_counts = loans_with_segment.groupBy(group_column).agg(
        F.count(F.lit(1)).alias("loan_count"),
        F.round(F.sum("principal_amount"), 2).alias("total_funded_principal"),
    )

    delinquent = (
        delinquency_events.select("loan_id").distinct()
        .join(loans_with_segment.select("loan_id", group_column), on="loan_id", how="inner")
        .groupBy(group_column)
        .agg(F.count(F.lit(1)).alias("delinquent_loan_count"))
    )

    default_metrics = (
        defaults.join(loans_with_segment.select("loan_id", group_column), on="loan_id", how="inner")
        .groupBy(group_column)
        .agg(
            F.count(F.lit(1)).alias("default_count"),
            F.round(F.sum("balance_at_default"), 2).alias("total_balance_at_default"),
            F.round(F.sum("recovery_amount"), 2).alias("total_recovery_amount"),
        )
    )

    result = (
        loan_counts.join(delinquent, on=group_column, how="left")
        .join(default_metrics, on=group_column, how="left")
        .fillna(0, subset=["delinquent_loan_count", "default_count", "total_balance_at_default", "total_recovery_amount"])
    )

    result = result.withColumn(
        "delinquency_rate", F.round(F.col("delinquent_loan_count") / F.col("loan_count"), 4)
    ).withColumn(
        "default_rate", F.round(F.col("default_count") / F.col("loan_count"), 4)
    ).withColumn(
        "recovery_rate",
        F.when(F.col("total_balance_at_default") > 0, F.round(F.col("total_recovery_amount") / F.col("total_balance_at_default"), 4)),
    )
    # Determine the denominator for loss_rate from business rules (defaults to portfolio-level rate)
    loss_denominator_column = business_rules.get("loss_rate_denominator", "total_funded_principal")
    if loss_denominator_column not in result.columns:
        raise ValueError(
            f"Configured loss rate denominator column '{loss_denominator_column}' "
            f"not found in delinquency_default metrics (available columns: {', '.join(result.columns)})"
        )
    result = result.withColumn(
        "loss_rate",
        F.when(
            F.col(loss_denominator_column) > 0,
            F.round(
                (F.col("total_balance_at_default") - F.col("total_recovery_amount"))
                / F.col(loss_denominator_column),
                4,
            ),
        ),
    )
    return result


def compute_delinquency_default(spark: SparkSession, business_rules: dict) -> DataFrame:
    loans = spark.read.parquet(s3a_path("raw", "loans.parquet"))
    customers = spark.read.parquet(s3a_path("raw", "customers.parquet"))
    delinquency_events = spark.read.parquet(s3a_path("raw", "delinquency_events.parquet"))
    defaults = spark.read.parquet(s3a_path("raw", "defaults.parquet"))

    loans_with_segment = loans.join(
        customers.select("customer_id", "risk_segment"), on="customer_id", how="inner"
    )
    loans_with_overall = loans_with_segment.withColumn("overall_key", F.lit(OVERALL_LABEL))

    by_segment = _metrics_for_group(loans_with_segment, delinquency_events, defaults, business_rules, "risk_segment")
    by_segment = by_segment.withColumnRenamed("risk_segment", "breakdown_value")

    overall = _metrics_for_group(loans_with_overall, delinquency_events, defaults, business_rules, "overall_key")
    overall = overall.withColumnRenamed("overall_key", "breakdown_value")

    columns = [
        "breakdown_value", "loan_count", "total_funded_principal", "delinquent_loan_count",
        "delinquency_rate", "default_count", "default_rate", "total_balance_at_default",
        "total_recovery_amount", "recovery_rate", "loss_rate",
    ]
    return overall.select(*columns).unionByName(by_segment.select(*columns))


def write_curated(df: DataFrame, storage: S3Storage) -> str:
    storage.write_parquet(CURATED_KEY, df.toPandas())
    return f"s3://{storage.bucket}/{CURATED_KEY}"


def main() -> None:
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")

    spark = get_spark_session("delinquency-default-etl")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result_df = compute_delinquency_default(spark, business_rules)
        pandas_df = result_df.toPandas()
        storage.write_parquet(CURATED_KEY, pandas_df)
        print(f"Wrote s3://{storage.bucket}/{CURATED_KEY}")
        for d in pandas_df.to_dict(orient="records"):
            print(f"  {d['breakdown_value']:<8} loans={d['loan_count']:<5} default_rate={d['default_rate']} loss_rate={d['loss_rate']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
