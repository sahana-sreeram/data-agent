"""PySpark ETL: loan_portfolio -- the direct analog of the original portfolio_summary.json,
over the 12-table lifecycle model. Reads s3a://<bucket>/raw/{loans,payment_events}.parquet,
writes a single-row curated summary to s3a://<bucket>/curated/loan_portfolio.parquet.

Metric formulas are fixed in context/metrics/loan_portfolio.json -- this module implements
them, it does not redefine them. See src/validate_loan_portfolio.py for the INDEPENDENT
(pandas, not Spark) recomputation used to check this ETL's output -- never import that
module's logic from here, or vice versa; reusing one calculation to "validate" the other
would let a bug in either pass unnoticed.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_session import get_spark_session, s3a_path
from src.storage import S3Storage

DEFAULT_AS_OF_DATE = "2026-07-20"


def compute_loan_portfolio(spark: SparkSession, business_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE) -> DataFrame:
    """Compute the loan_portfolio curated summary as a single-row Spark DataFrame."""
    loans = spark.read.parquet(s3a_path("raw", "loans.parquet"))
    payment_events = spark.read.parquet(s3a_path("raw", "payment_events.parquet"))

    # "Successful" for balance purposes = an original successful payment (PAID) net of any
    # later REVERSAL -- a REVERSAL's amount is already stored negative, so summing both
    # statuses together nets them correctly. This is NOT the same as
    # business_rules.successful_payment_statuses alone (["PAID"]), which answers a
    # different question (how many payments succeeded) -- see context/metrics/loan_portfolio.json.
    net_payment_statuses = business_rules["successful_payment_statuses"] + ["REVERSED"]
    net_paid_by_loan = (
        payment_events.filter(F.col("payment_status").isin(net_payment_statuses))
        .groupBy("loan_id")
        .agg(F.sum("amount").alias("net_paid"))
    )

    joined = loans.join(net_paid_by_loan, on="loan_id", how="left").fillna({"net_paid": 0.0})
    joined = joined.withColumn(
        "outstanding_principal",
        F.greatest(F.col("principal_amount") - F.col("net_paid"), F.lit(0.0)),
    )

    accrual_statuses = business_rules["interest_accrual"]["accrues_on_statuses"]
    joined = joined.withColumn(
        "days_since_origination",
        F.datediff(F.lit(as_of_date).cast("date"), F.col("originated_at").cast("date")),
    )
    joined = joined.withColumn(
        "accrued_interest",
        F.when(
            F.col("loan_status").isin(accrual_statuses),
            F.col("principal_amount") * F.col("interest_rate") * F.col("days_since_origination") / F.lit(365.0),
        ).otherwise(F.lit(0.0)),
    )

    summary = joined.agg(
        F.count(F.lit(1)).alias("loan_count"),
        F.sum(F.when(F.col("loan_status") == "ACTIVE", 1).otherwise(0)).alias("active_loan_count"),
        F.sum(F.when(F.col("loan_status") == "CLOSED", 1).otherwise(0)).alias("closed_loan_count"),
        F.sum(F.when(F.col("loan_status") == "DEFAULTED", 1).otherwise(0)).alias("defaulted_loan_count"),
        F.round(F.sum("principal_amount"), 2).alias("total_funded_principal"),
        F.round(F.sum("outstanding_principal"), 2).alias("total_outstanding_principal"),
        F.round(F.avg("interest_rate"), 4).alias("avg_interest_rate"),
        F.round(F.sum("accrued_interest"), 2).alias("total_accrued_interest"),
    ).withColumn("as_of_date", F.lit(as_of_date))

    return summary


CURATED_KEY = "curated/loan_portfolio.parquet"


def write_curated(df: DataFrame, storage: S3Storage) -> str:
    """Write the single-row summary as one clean Parquet object.

    Spark's native df.write.parquet(path) always creates a PARTITIONED DIRECTORY
    (part-NNNNN files + a _SUCCESS marker), never a single object at that exact key --
    fine for large fact tables, but wrong for this table's grain (one row). Since the
    curated output here is tiny, the compute happens in Spark (real distributed
    transformation over the raw tables) but the small result is collected to the driver
    and written as a single object via S3Storage, so both pandas (the validator) and
    Spark can read it back at one exact key with no partition-file bookkeeping.
    """
    storage.write_parquet(CURATED_KEY, df.toPandas())
    return f"s3://{storage.bucket}/{CURATED_KEY}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the loan_portfolio PySpark ETL.")
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")

    spark = get_spark_session("loan-portfolio-etl")
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary_df = compute_loan_portfolio(spark, business_rules, args.as_of_date)
        # Materialize ONCE (this is what write_curated does internally too) and derive
        # both the write and the printed summary from that single pandas DataFrame,
        # rather than a separate .collect() action re-running the whole Spark DAG.
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
