"""PySpark ETL: payment_performance -- single-row curated summary of expected vs. actual
payment collection. Reads s3a://<bucket>/raw/{payment_schedule,payment_events}.parquet,
writes to s3a://<bucket>/curated/payment_performance.parquet.

See src/validate_payment_performance.py for the independent (pandas) recomputation.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_session import get_spark_session, s3a_path
from src.storage import S3Storage

CURATED_KEY = "curated/payment_performance.parquet"
DEFAULT_AS_OF_DATE = "2026-07-20"


def compute_payment_performance(
    spark: SparkSession, business_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE
) -> DataFrame:
    payment_schedule = spark.read.parquet(s3a_path("raw", "payment_schedule.parquet"))
    payment_events = spark.read.parquet(s3a_path("raw", "payment_events.parquet"))

    expected = payment_schedule.filter(F.col("due_date") <= F.lit(as_of_date))
    expected_agg = expected.agg(
        F.count(F.lit(1)).alias("expected_payment_count"),
        F.round(F.sum("scheduled_amount"), 2).alias("expected_amount_due"),
    )

    # "Collected" nets a REVERSAL against its original PAID amount (REVERSAL's amount is
    # already negative by construction) -- same discipline as loan_portfolio's outstanding
    # principal, and what the "reversed payments counted as successful" failure category
    # this guards against would get wrong.
    net_collected_statuses = business_rules["successful_payment_statuses"] + ["REVERSED"]
    collected_agg = payment_events.filter(F.col("payment_status").isin(net_collected_statuses)).agg(
        F.round(F.sum("amount"), 2).alias("total_collected_amount")
    )

    successful_payment_count = payment_events.filter(
        F.col("payment_status").isin(business_rules["successful_payment_statuses"])
    ).count()

    # MISSED events carry amount=0 (nothing was paid) -- the missed AMOUNT is what the
    # schedule expected, found by joining back to payment_schedule via schedule_id.
    missed_events = payment_events.filter(F.col("payment_status") == "MISSED")
    missed_agg = missed_events.join(
        payment_schedule.select(F.col("schedule_id"), F.col("scheduled_amount")), on="schedule_id", how="inner"
    ).agg(
        F.count(F.lit(1)).alias("missed_payment_count"),
        F.round(F.coalesce(F.sum("scheduled_amount"), F.lit(0.0)), 2).alias("missed_amount"),
    )

    late_payment_count = payment_events.filter(F.col("payment_status") == "LATE").count()
    failed_payment_count = payment_events.filter(F.col("payment_status") == "FAILED").count()

    threshold_days = business_rules["prepayment_threshold_days"]
    paid_with_due_date = payment_events.filter(
        F.col("payment_status").isin(business_rules["successful_payment_statuses"])
    ).join(payment_schedule.select(F.col("schedule_id"), F.col("due_date")), on="schedule_id", how="inner")
    prepaid_count = paid_with_due_date.filter(
        F.datediff(F.col("due_date").cast("date"), F.col("payment_date").cast("date")) >= threshold_days
    ).count()

    # Materialize each aggregation's single row ONCE -- DataFrame.first() is a Spark
    # action, so calling it twice per aggregation (as this used to) re-ran the whole
    # read+filter+aggregate (and, for missed_agg, its join with payment_schedule) twice.
    expected_row = expected_agg.first()
    collected_row = collected_agg.first()
    missed_row = missed_agg.first()

    row = spark.createDataFrame(
        [
            (
                expected_row["expected_payment_count"],
                expected_row["expected_amount_due"],
                successful_payment_count,
                collected_row["total_collected_amount"] or 0.0,
                missed_row["missed_payment_count"],
                missed_row["missed_amount"] or 0.0,
                late_payment_count,
                failed_payment_count,
                prepaid_count,
            )
        ],
        (
            "expected_payment_count long, expected_amount_due double, successful_payment_count long, "
            "total_collected_amount double, missed_payment_count long, missed_amount double, "
            "late_payment_count long, failed_payment_count long, prepaid_count long"
        ),
    )

    row = row.withColumn(
        "collection_rate",
        F.when(F.col("expected_amount_due") > 0, F.round(F.col("total_collected_amount") / F.col("expected_amount_due"), 4)),
    ).withColumn(
        "prepayment_rate",
        F.when(F.col("successful_payment_count") > 0, F.round(F.col("prepaid_count") / F.col("successful_payment_count"), 4)),
    ).drop("prepaid_count").withColumn("as_of_date", F.lit(as_of_date))

    return row


def write_curated(df: DataFrame, storage: S3Storage) -> str:
    storage.write_parquet(CURATED_KEY, df.toPandas())
    return f"s3://{storage.bucket}/{CURATED_KEY}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the payment_performance PySpark ETL.")
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")

    spark = get_spark_session("payment-performance-etl")
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary_df = compute_payment_performance(spark, business_rules, args.as_of_date)
        pandas_df = summary_df.toPandas()
        storage.write_parquet(CURATED_KEY, pandas_df)
        path = f"s3://{storage.bucket}/{CURATED_KEY}"
        print(f"Wrote {path}")
        for key, value in pandas_df.iloc[0].to_dict().items():
            print(f"  {key:<28} {value}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
